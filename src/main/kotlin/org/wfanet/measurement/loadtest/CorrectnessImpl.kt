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
import kotlinx.coroutines.flow.map
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
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.createBlob

class CorrectnessImpl(
  override val dataProviderCount: Int,
  override val campaignCount: Int,
  override val generatedSetSize: Int = 1000,
  override val universeSize: Long = 10_000_000_000L,
  override val runId: String,
  override val outputDir: String,
  override val sketchConfig: SketchConfig,
  override val encryptionPublicKey: ElGamalPublicKey,
  override val sketchStorageClient: StorageClient,
  override val encryptedSketchStorageClient: StorageClient,
  override val reportStorageClient: StorageClient,
  override val combinedPublicKeyId: String,
  override val publisherDataStub: PublisherDataCoroutineStub
) : Correctness {

  private val MAX_COUNTER_VALUE = 10
  private val DECAY_RATE = 23.0
  private val INDEX_SIZE = 330_000L
  private val GLOBAL_COMPUTATION_ID = "1"

  suspend fun process(relationalDatabase: KingdomRelationalDatabase) {
    logger.info("Starting with RunID: $runId ...")

    val advertiser = relationalDatabase.createAdvertiser()
    logger.info("Created an Advertiser: $advertiser")
    val externalAdvertiserId = ExternalId(advertiser.externalAdvertiserId)

    val (campaignIds, anySketches) =
      (1..dataProviderCount)
        .asFlow()
        .transform { emitAll(relationalDatabase.createDataProvider(externalAdvertiserId)) }
        .toList()
        .unzip()

    val reach = estimateCardinality(anySketches)
    val frequency = estimateFrequency(anySketches)
    val storedResultsPath = storeEstimationResults(reach, frequency)
    logger.info("Estimation Results saved into: $outputDir/$runId/reports/$storedResultsPath")

    relationalDatabase.scheduleReport(externalAdvertiserId, campaignIds)

    logger.info("Finished.")
  }

  private fun KingdomRelationalDatabase.createDataProvider(
    externalAdvertiserId: ExternalId
  ): Flow<Pair<ExternalId, AnySketch>> {
    val dataProvider = this.createDataProvider()
    logger.info("Created a Data Provider: $dataProvider")
    val externalDataProviderId = ExternalId(dataProvider.externalDataProviderId)

    return generateReach().asFlow()
      .map { reach -> createCampaign(reach, externalAdvertiserId, externalDataProviderId) }
  }

  private suspend fun KingdomRelationalDatabase.createCampaign(
    reach: Set<Long>,
    externalAdvertiserId: ExternalId,
    externalDataProviderId: ExternalId
  ): Pair<ExternalId, AnySketch> {
    val campaign = this.createCampaign(
      externalDataProviderId,
      externalAdvertiserId,
      "Campaign name"
    )
    logger.info("Created a Campaign $campaign")
    val externalCampaignId = ExternalId(campaign.externalCampaignId)
    val anySketch = generateSketch(reach)
    val sketchProto = anySketch.toSketchProto(sketchConfig)
    logger.info("Created a Sketch with ${sketchProto.registersCount} registers.")

    val storedSketchPath = storeSketch(anySketch)
    logger.info("Raw Sketch saved into: $outputDir/$runId/sketches/$storedSketchPath")

    val encryptedSketch = encryptSketch(sketchProto)
    val storedEncryptedSketchPath = storeEncryptedSketch(encryptedSketch)
    logger.info(
      "Encrypted Sketch saved into: " +
        "$outputDir/$runId/encrypted_sketches/$storedEncryptedSketchPath"
    )

    try {
      sendToServer(
        externalDataProviderId.toString(),
        externalCampaignId.toString(),
        encryptedSketch
      )
    } catch (e: Exception) {
      logger.warning(
        "Failed sending the sketch for Campaign: " +
          "${externalCampaignId.value} to the server due to ${e.cause!!.message}."
      )
      throw e
    }

    return Pair(externalCampaignId, anySketch)
  }

  private fun KingdomRelationalDatabase.scheduleReport(
    externalAdvertiserId: ExternalId,
    campaignIds: List<ExternalId>
  ) {
    val metricDefinition = MetricDefinition.newBuilder().apply {
      sketchBuilder.apply {
        setSketchConfigId(12345L)
        type = SketchMetricDefinition.Type.IMPRESSION_REACH_AND_FREQUENCY
      }
    }.build()
    val reportConfig = this.createReportConfig(
      ReportConfig.newBuilder()
        .setExternalAdvertiserId(externalAdvertiserId.value)
        .apply {
          numRequisitions = campaignCount.toLong()
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
    val schedule = this.createSchedule(
      ReportConfigSchedule.newBuilder()
        .setExternalAdvertiserId(externalAdvertiserId.value)
        .setExternalReportConfigId(externalReportConfigId.value)
        .apply {
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

  override fun estimateCardinality(anySketches: List<AnySketch>): Long {
    val anySketch = anySketches.union()
    val activeRegisterCount = anySketch.toList().size.toLong()
    return Estimators.EstimateCardinalityLiquidLegions(
      DECAY_RATE,
      INDEX_SIZE,
      activeRegisterCount
    )
  }

  override fun estimateFrequency(anySketches: List<AnySketch>): Map<Long, Long> {
    val anySketch = anySketches.union()
    val valueIndex = anySketch.getValueIndex("SamplingIndicator").asInt
    return ValueHistogram.calculateHistogram(
      anySketch,
      "Frequency",
      { it.getValues().get(valueIndex) != -1L }
    )
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

  override suspend fun storeSketch(anySketch: AnySketch): String {
    val sketch: Sketch = anySketch.toSketchProto(sketchConfig)
    val blobKey = generateBlobKey()
    sketchStorageClient.createBlob(blobKey, sketch.toByteString())
    return blobKey
  }

  override suspend fun storeEncryptedSketch(encryptedSketch: ByteString): String {
    val blobKey = generateBlobKey()
    encryptedSketchStorageClient.createBlob(blobKey, encryptedSketch)
    return blobKey
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
    val blobKey = generateBlobKey()
    reportStorageClient.createBlob(blobKey, computation.toByteString())
    return blobKey
  }

  override suspend fun sendToServer(
    dataProviderId: String,
    campaignId: String,
    encryptedSketch: ByteString
  ) {
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(250))
    val combinedPublicKey = CombinedPublicKey.newBuilder().apply {
      keyBuilder.combinedPublicKeyId = combinedPublicKeyId
      publicKey = with(encryptionPublicKey) { generator.concat(element) }
    }.build()

    var uploaded = false
    while (!uploaded) {
      throttler.onReady {
        val requisition: MetricRequisition? = loadMetricRequisitions(dataProviderId, campaignId)
        if (requisition != null) {
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
  ): Flow<UploadMetricValueRequest> = flow {
    emit(
      UploadMetricValueRequest.newBuilder().apply {
        headerBuilder.apply {
          key = metricRequisition.key
          this.combinedPublicKey = combinedPublicKey.key
        }
      }.build()
    )

    emit(
      UploadMetricValueRequest.newBuilder().apply {
        chunkBuilder.data = encryptedSketch
      }.build()
    )
  }

  private fun List<AnySketch>.union(): AnySketch {
    require(!this.isNullOrEmpty()) {
      "At least one AnySketch expected."
    }
    val anySketch = this.first()
    if (this.size > 1) {
      anySketch.mergeAll(this.subList(1, this.size))
    }
    return anySketch
  }

  private fun generateBlobKey(): String {
    return UUID.randomUUID().toString()
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
