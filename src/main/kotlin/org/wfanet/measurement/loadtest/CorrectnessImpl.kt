// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.loadtest

import com.google.protobuf.ByteString
import io.grpc.StatusException
import java.nio.file.Paths
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.transform
import org.wfanet.anysketch.AnySketch
import org.wfanet.anysketch.SketchProtos
import org.wfanet.anysketch.crypto.ElGamalPublicKeys
import org.wfanet.anysketch.crypto.EncryptSketchRequest
import org.wfanet.anysketch.crypto.EncryptSketchResponse
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter
import org.wfanet.estimation.Estimators
import org.wfanet.estimation.ValueHistogram
import org.wfanet.measurement.api.v1alpha.CombinedPublicKey
import org.wfanet.measurement.api.v1alpha.GlobalComputation
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsRequest
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt.PublisherDataCoroutineStub
import org.wfanet.measurement.api.v1alpha.Sketch
import org.wfanet.measurement.api.v1alpha.SketchConfig
import org.wfanet.measurement.api.v1alpha.UploadMetricValueRequest
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.MinimumIntervalThrottler
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.crypto.ElGamalPublicKey
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.internal.MetricDefinition
import org.wfanet.measurement.internal.SketchMetricDefinition
import org.wfanet.measurement.internal.kingdom.ReportConfig
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.TimePeriod
import org.wfanet.measurement.internal.loadtest.TestResult
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.createBlob

private const val MAX_COUNTER_VALUE = 10
private const val DECAY_RATE = 23.0
private const val INDEX_SIZE = 330_000L
private const val GLOBAL_COMPUTATION_ID = "1"

class CorrectnessImpl(
  override val dataProviderCount: Int,
  override val campaignCount: Int,
  override val generatedSetSize: Int = 1000,
  override val universeSize: Long = 10_000_000_000L,
  override val runId: String,
  override val sketchConfig: SketchConfig,
  override val encryptionPublicKey: ElGamalPublicKey,
  override val storageClient: StorageClient,
  override val combinedPublicKeyId: String,
  override val publisherDataStub: PublisherDataCoroutineStub
) : Correctness {

  suspend fun process(relationalDatabase: KingdomRelationalDatabase) {
    logger.info("Starting with RunID: $runId ...")
    val testResult = TestResult.newBuilder().setRunId(runId)

    val advertiser = relationalDatabase.createAdvertiser()
    logger.info("Created an Advertiser: $advertiser")
    val externalAdvertiserId = ExternalId(advertiser.externalAdvertiserId)

    val campaignTriples =
      (1..dataProviderCount)
        .asFlow()
        .transform {
          emitAll(
            relationalDatabase.createDataProvider(externalAdvertiserId)
          )
        }
        .toList()

    // Schedule a report before loading the metric requisitions.
    val campaignIds = campaignTriples.map { it.second }
    relationalDatabase.scheduleReport(externalAdvertiserId, campaignIds)

    val anySketches = campaignTriples.map { it.third }
    val combinedAnySketch = SketchProtos.toAnySketch(sketchConfig).apply { mergeAll(anySketches) }
    val reach = estimateCardinality(combinedAnySketch)
    val frequency = estimateFrequency(combinedAnySketch)
    val storedResultsPath = storeEstimationResults(reach, frequency)
    testResult.computationBlobKey = storedResultsPath
    logger.info("Estimation Results saved with blob key: $storedResultsPath")

    // Finally, we are sending encrypted sketches to the PublisherDataService.
    campaignTriples.forEach { (externalDataProviderId, externalCampaignId, anySketch) ->
      encryptAndSend(externalDataProviderId, externalCampaignId, anySketch, testResult)
    }

    val blobKey = storeTestResult(testResult.build())
    println("Test Result saved with blob key: $blobKey")
    logger.info("Finished with manifest: $blobKey.")
  }

  private fun KingdomRelationalDatabase.createDataProvider(
    externalAdvertiserId: ExternalId
  ): Flow<Triple<ExternalId, ExternalId, AnySketch>> = flow {
    val dataProvider = createDataProvider()
    logger.info("Created a Data Provider: $dataProvider")
    val externalDataProviderId = ExternalId(dataProvider.externalDataProviderId)

    generateReach().forEach { reach ->
      emit(createCampaign(reach, externalAdvertiserId, externalDataProviderId))
    }
  }

  private suspend fun KingdomRelationalDatabase.createCampaign(
    reach: Set<Long>,
    externalAdvertiserId: ExternalId,
    externalDataProviderId: ExternalId
  ): Triple<ExternalId, ExternalId, AnySketch> {
    val campaign = createCampaign(
      externalDataProviderId,
      externalAdvertiserId,
      "Campaign name"
    )
    logger.info("Created a Campaign $campaign")
    val externalCampaignId = ExternalId(campaign.externalCampaignId)
    val anySketch = generateSketch(reach)

    return Triple(externalDataProviderId, externalCampaignId, anySketch)
  }

  private suspend fun KingdomRelationalDatabase.scheduleReport(
    externalAdvertiserId: ExternalId,
    campaignIds: List<ExternalId>
  ) {
    val metricDefinition = MetricDefinition.newBuilder().apply {
      sketchBuilder.apply {
        sketchConfigId = 12345L
        type = SketchMetricDefinition.Type.IMPRESSION_REACH_AND_FREQUENCY
      }
    }.build()
    val reportConfig = createReportConfig(
      ReportConfig.newBuilder().apply {
        this.externalAdvertiserId = externalAdvertiserId.value
        numRequisitions = campaignIds.size.toLong()
        reportConfigDetailsBuilder.apply {
          addMetricDefinitions(metricDefinition)
          reportDurationBuilder.apply {
            unit = TimePeriod.Unit.DAY
            count = 1
          }
        }
      }.build(),
      campaignIds
    )
    logger.info("Created a ReportConfig: $reportConfig")
    val externalReportConfigId = ExternalId(reportConfig.externalReportConfigId)
    val schedule = createSchedule(
      ReportConfigSchedule.newBuilder().apply {
        this.externalAdvertiserId = externalAdvertiserId.value
        this.externalReportConfigId = externalReportConfigId.value
        repetitionSpecBuilder.apply {
          start = Instant.now().toProtoTime()
          repetitionPeriodBuilder.apply {
            unit = TimePeriod.Unit.DAY
            count = 1
          }
        }
        nextReportStartTime = repetitionSpec.start
      }.build()
    )
    logger.info("Created a ReportConfigSchedule: $schedule")
  }

  private suspend fun encryptAndSend(
    externalDataProviderId: ExternalId,
    externalCampaignId: ExternalId,
    anySketch: AnySketch,
    testResult: TestResult.Builder
  ) {
    val sketchProto = anySketch.toSketchProto(sketchConfig)
    logger.info("Created a Sketch with ${sketchProto.registersCount} registers.")

    val storedSketchPath = storeSketch(sketchProto)
    val sketchBlobKeys = TestResult.Sketch.newBuilder().setBlobKey(storedSketchPath)
    logger.info("Raw Sketch saved with blob key: $storedSketchPath")

    val encryptedSketch = encryptSketch(sketchProto)
    val storedEncryptedSketchPath = storeEncryptedSketch(encryptedSketch)
    sketchBlobKeys.encryptedBlobKey = storedEncryptedSketchPath
    testResult.addSketches(sketchBlobKeys)
    logger.info("Encrypted Sketch saved with blob key: $storedEncryptedSketchPath")

    try {
      sendToServer(
        externalDataProviderId.apiId.value,
        externalCampaignId.apiId.value,
        encryptedSketch
      )
    } catch (e: StatusException) {
      logger.warning(
        "Failed sending the sketch for Campaign: " +
          "${externalCampaignId.value} to the server due to $e."
      )
      throw e
    }
  }

  override fun generateReach(): Sequence<Set<Long>> {
    return generateIndependentSets(universeSize, generatedSetSize).take(campaignCount)
  }

  override fun generateSketch(reach: Set<Long>): AnySketch {
    val anySketch: AnySketch = SketchProtos.toAnySketch(sketchConfig)
    for (value: Long in reach) {
      anySketch.insert(value, mapOf("frequency" to 1L))
    }
    return anySketch
  }

  override fun estimateCardinality(anySketch: AnySketch): Long {
    val activeRegisterCount = anySketch.toList().size.toLong()
    return Estimators.EstimateCardinalityLiquidLegions(
      DECAY_RATE,
      INDEX_SIZE,
      activeRegisterCount
    )
  }

  override fun estimateFrequency(anySketch: AnySketch): Map<Long, Long> {
    val valueIndex = anySketch.getValueIndex("SamplingIndicator").asInt
    return ValueHistogram.calculateHistogram(anySketch, "Frequency") {
      it.values[valueIndex] != -1L
    }
  }

  override fun encryptSketch(sketch: Sketch): ByteString {
    val request: EncryptSketchRequest = EncryptSketchRequest.newBuilder()
      .setSketch(sketch)
      .setCurveId(encryptionPublicKey.ellipticCurveId.toLong())
      .setMaximumValue(MAX_COUNTER_VALUE)
      .setElGamalKeys(
        ElGamalPublicKeys.newBuilder()
          .setElGamalG(encryptionPublicKey.generator)
          .setElGamalY(encryptionPublicKey.element)
          .build()
      )
      .build()
    val response = EncryptSketchResponse.parseFrom(
      SketchEncrypterAdapter.EncryptSketch(request.toByteArray())
    )
    return response.encryptedSketch
  }

  override suspend fun storeSketch(sketch: Sketch): String {
    return storeBlob(sketch.toByteString())
  }

  override suspend fun storeEncryptedSketch(encryptedSketch: ByteString): String {
    return storeBlob(encryptedSketch)
  }

  override suspend fun storeEstimationResults(
    reach: Long,
    frequency: Map<Long, Long>
  ): String {
    val computation = GlobalComputation.newBuilder().apply {
      keyBuilder.globalComputationId = GLOBAL_COMPUTATION_ID
      state = GlobalComputation.State.SUCCEEDED
      resultBuilder.apply {
        setReach(reach)
        putAllFrequency(frequency)
      }
    }.build()
    logger.info("Reach and Frequency are computed: $computation")
    return storeBlob(computation.toByteString())
  }

  override suspend fun storeTestResult(testResult: TestResult): String {
    return storeBlob(testResult.toByteString())
  }

  override suspend fun sendToServer(
    dataProviderId: String,
    campaignId: String,
    encryptedSketch: ByteString
  ) {
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(250))
    // TODO(b/159036541): Use PublisherDataService.getCombinedPublicKey method to get this.
    val combinedPublicKey = CombinedPublicKey.newBuilder().apply {
      keyBuilder.combinedPublicKeyId = combinedPublicKeyId
      publicKey = with(encryptionPublicKey) { generator.concat(element) }
    }.build()

    var uploaded = false
    while (!uploaded) {
      throttler.onReady {
        val requisition: MetricRequisition? = loadMetricRequisitions(dataProviderId, campaignId)
        if (requisition != null) {
          logger.info("Server returned with a Requisition: $requisition")
          uploadRequisition(combinedPublicKey, requisition, encryptedSketch)
          uploaded = true
        }
      }
    }
  }

  private suspend fun loadMetricRequisitions(
    dataProviderId: String,
    campaignId: String
  ): MetricRequisition? {
    val request = ListMetricRequisitionsRequest.newBuilder().apply {
      parentBuilder.apply {
        this.dataProviderId = dataProviderId
        this.campaignId = campaignId
      }
      filterBuilder.addStates(MetricRequisition.State.UNFULFILLED)
      pageSize = 1
    }.build()
    val response = publisherDataStub.listMetricRequisitions(request)
    check(response.metricRequisitionsCount <= 1) { "Too many requisitions: $response" }
    return response.metricRequisitionsList.firstOrNull()
  }

  private suspend fun uploadRequisition(
    combinedPublicKey: CombinedPublicKey,
    metricRequisition: MetricRequisition,
    encryptedSketch: ByteString
  ) {
    publisherDataStub.uploadMetricValue(
      makeMetricValueFlow(combinedPublicKey, metricRequisition, encryptedSketch)
    )
    logger.info("Encrypted Sketch successfully sent to the Publisher Data Service.")
  }

  private fun makeMetricValueFlow(
    combinedPublicKey: CombinedPublicKey,
    metricRequisition: MetricRequisition,
    encryptedSketch: ByteString
  ): Flow<UploadMetricValueRequest> = flowOf(
    UploadMetricValueRequest.newBuilder().apply {
      headerBuilder.apply {
        key = metricRequisition.key
        this.combinedPublicKey = combinedPublicKey.key
      }
    }.build(),
    UploadMetricValueRequest.newBuilder().apply {
      chunkBuilder.data = encryptedSketch
    }.build()
  )

  private suspend fun storeBlob(blob: ByteString): String {
    val blobKey = generateBlobKey()
    storageClient.createBlob(blobKey, blob)
    return blobKey
  }

  private fun generateBlobKey(): String {
    return "correctness-output/" + UUID.randomUUID().toString()
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    init {
      loadLibrary(
        name = "estimators",
        directoryPath = Paths.get("any_sketch/src/main/java/org/wfanet/estimation")
      )
      loadLibrary(
        name = "sketch_encrypter_adapter",
        directoryPath = Paths.get("any_sketch/src/main/java/org/wfanet/anysketch/crypto")
      )
    }
  }
}
