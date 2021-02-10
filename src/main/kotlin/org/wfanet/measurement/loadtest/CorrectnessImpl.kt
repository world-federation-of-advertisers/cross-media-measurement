// Copyright 2020 The Cross-Media Measurement Authors
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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import java.nio.file.Paths
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.logging.Logger
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import org.wfanet.anysketch.AnySketch
import org.wfanet.anysketch.SketchProtos
import org.wfanet.anysketch.crypto.EncryptSketchRequest
import org.wfanet.anysketch.crypto.EncryptSketchRequest.DestroyedRegisterStrategy.FLAGGED_KEY
import org.wfanet.anysketch.crypto.EncryptSketchResponse
import org.wfanet.anysketch.crypto.SketchEncrypterAdapter
import org.wfanet.estimation.Estimators
import org.wfanet.estimation.ValueHistogram
import org.wfanet.measurement.api.v1alpha.CombinedPublicKey
import org.wfanet.measurement.api.v1alpha.ElGamalPublicKey
import org.wfanet.measurement.api.v1alpha.GetCombinedPublicKeyRequest
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsRequest
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.PublisherDataGrpcKt.PublisherDataCoroutineStub
import org.wfanet.measurement.api.v1alpha.Sketch
import org.wfanet.measurement.api.v1alpha.SketchConfig
import org.wfanet.measurement.api.v1alpha.UploadMetricValueRequest
import org.wfanet.measurement.common.asBufferedFlow
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.loadLibrary
import org.wfanet.measurement.common.renewedFlow
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.MetricDefinition
import org.wfanet.measurement.internal.SketchMetricDefinition
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.ReportConfig
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportDetails
import org.wfanet.measurement.internal.kingdom.TimePeriod
import org.wfanet.measurement.internal.loadtest.TestResult
import org.wfanet.measurement.kingdom.db.KingdomRelationalDatabase
import org.wfanet.measurement.kingdom.db.streamReportsFilter
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.createBlob
import org.wfanet.measurement.system.v1alpha.GlobalComputation

private const val MAX_COUNTER_VALUE = 10
private const val DECAY_RATE = 23.0
private const val INDEX_SIZE = 330_000L
private const val GLOBAL_COMPUTATION_ID = "1"

private const val STREAM_BYTE_BUFFER_SIZE = 1024 * 32 // 32 KiB

class CorrectnessImpl(
  override val dataProviderCount: Int,
  override val campaignCount: Int,
  override val generatedSetSize: Int = 1000,
  override val universeSize: Long = 10_000_000_000L,
  override val runId: String,
  override val sketchConfig: SketchConfig,
  override val storageClient: StorageClient,
  override val publisherDataStub: PublisherDataCoroutineStub
) : Correctness {

  /** Cache of [CombinedPublicKey] resource ID to [CombinedPublicKey]. */
  private val publicKeyCache = mutableMapOf<String, CombinedPublicKey>()

  suspend fun process(relationalDatabase: KingdomRelationalDatabase) {
    logger.info("Starting with RunID: $runId ...")
    val testResult = TestResult.newBuilder().setRunId(runId)

    val advertiser = relationalDatabase.createAdvertiser()
    logger.info("Created an Advertiser: $advertiser")
    val externalAdvertiserId = ExternalId(advertiser.externalAdvertiserId)

    val generatedCampaigns = List(dataProviderCount) {
      relationalDatabase.createDataProvider(externalAdvertiserId).toList()
    }.flatten()

    // Schedule a report before loading the metric requisitions.
    val campaignIds = generatedCampaigns.map { it.campaignId }
    val reportConfigAndScheduleId =
      relationalDatabase.scheduleReport(externalAdvertiserId, campaignIds)

    val anySketches = generatedCampaigns.map { it.sketch }
    val combinedAnySketch = SketchProtos.toAnySketch(sketchConfig).apply { mergeAll(anySketches) }
    val reach = estimateCardinality(combinedAnySketch)
    val frequency = estimateFrequency(combinedAnySketch)
    val storedResultsPath = storeEstimationResults(reach, frequency)
    testResult.computationBlobKey = storedResultsPath
    logger.info("Estimation Results saved with blob key: $storedResultsPath")

    // Finally, we are sending encrypted sketches to the PublisherDataService.
    coroutineScope {
      generatedCampaigns.forEach {
        launch {
          encryptAndSend(it, testResult)
        }
      }
    }

    val expectedResult =
      ReportDetails.Result.newBuilder().setReach(reach).putAllFrequency(frequency).build()
    logger.info("Expected Result: $expectedResult")

    // Start querying Spanner after 2 min.
    logger.info("Waiting 2 min...")
    delay(Duration.ofMinutes(2).toMillis())
    val finishedReport =
      relationalDatabase.getFinishedReport(
        reportConfigAndScheduleId.reportConfig,
        reportConfigAndScheduleId.schedule
      )
    val actualResult = finishedReport.reportDetails.result
    logger.info("Actual Result: $actualResult")

    assertThat(actualResult).isEqualTo(expectedResult)

    val blobKey = storeTestResult(testResult.build())
    println("Test Result saved with blob key: $blobKey")
    logger.info("Correctness Test passes with manifest: $blobKey.")
  }

  private suspend fun KingdomRelationalDatabase.getFinishedReport(
    externalReportConfigId: ExternalId,
    externalScheduleId: ExternalId
  ): Report {
    logger.info("Getting finished report from Kingdom Spanner...")
    return renewedFlow(10_000, 1_000) {
      streamReports(
        streamReportsFilter(
          externalReportConfigIds = listOf(externalReportConfigId),
          externalScheduleIds = listOf(externalScheduleId),
          states = listOf(Report.ReportState.SUCCEEDED)
        ),
        limit = 1
      )
    }.first()
  }

  private suspend fun KingdomRelationalDatabase.createDataProvider(
    externalAdvertiserId: ExternalId
  ): Flow<GeneratedCampaign> {
    val dataProvider = createDataProvider()
    logger.info("Created a Data Provider: $dataProvider")
    val externalDataProviderId = ExternalId(dataProvider.externalDataProviderId)

    return generateReach().asFlow().map { reach ->
      createCampaign(reach, externalAdvertiserId, externalDataProviderId)
    }
  }

  private suspend fun KingdomRelationalDatabase.createCampaign(
    reach: Set<Long>,
    externalAdvertiserId: ExternalId,
    externalDataProviderId: ExternalId
  ): GeneratedCampaign {
    val campaign = createCampaign(
      externalDataProviderId,
      externalAdvertiserId,
      "Campaign name"
    )
    logger.info("Created a Campaign $campaign")
    val externalCampaignId = ExternalId(campaign.externalCampaignId)
    val anySketch = generateSketch(reach)

    return GeneratedCampaign(externalDataProviderId, externalCampaignId, anySketch)
  }

  private suspend fun KingdomRelationalDatabase.scheduleReport(
    externalAdvertiserId: ExternalId,
    campaignIds: List<ExternalId>
  ): ReportConfigAndScheduleId {
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
    return ReportConfigAndScheduleId(
      externalReportConfigId,
      ExternalId(schedule.externalScheduleId)
    )
  }

  private suspend fun encryptAndSend(
    generatedCampaign: GeneratedCampaign,
    testResult: TestResult.Builder
  ) {
    val dataProviderId = generatedCampaign.dataProviderId.apiId
    val campaignId = generatedCampaign.campaignId.apiId

    val requisition = runCatching {
      getMetricRequisition(dataProviderId, campaignId)
    }.getOrThrowWithMessage {
      "Error getting metric requisition. Data provider: $dataProviderId, campaign: $campaignId"
    }
    val resourceKey = requisition.combinedPublicKey
    val combinedPublicKey = publicKeyCache.getOrPut(resourceKey.combinedPublicKeyId) {
      runCatching {
        publisherDataStub.getCombinedPublicKey(resourceKey)
      }.getOrThrowWithMessage {
        "Error getting combined public key $resourceKey"
      }
    }
    val encryptedSketch =
      encryptAndStore(generatedCampaign.sketch, combinedPublicKey.encryptionKey, testResult)

    runCatching {
      uploadMetricValue(requisition.key, encryptedSketch)
    }.getOrThrowWithMessage {
      "Error uploading metric value for ${requisition.key}"
    }
  }

  private suspend fun encryptAndStore(
    anySketch: AnySketch,
    publicKey: ElGamalPublicKey,
    testResult: TestResult.Builder
  ): ByteString {
    val sketchProto = anySketch.toSketchProto(sketchConfig)
    logger.info("Created a Sketch with ${sketchProto.registersCount} registers.")
    val sketchBlobKey = storeSketch(sketchProto)
    logger.info("Raw Sketch saved with blob key: $sketchBlobKey")

    val encryptedSketch = encryptSketch(sketchProto, publicKey)
    val encryptedSketchBlobKey = storeEncryptedSketch(encryptedSketch)
    logger.info("Encrypted Sketch saved with blob key: $encryptedSketchBlobKey")

    testResult.addSketchesBuilder().apply {
      blobKey = sketchBlobKey
      encryptedBlobKey = encryptedSketchBlobKey
    }

    return encryptedSketch
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

  override fun estimateFrequency(anySketch: AnySketch): Map<Long, Double> {
    val valueIndex = anySketch.getValueIndex("SamplingIndicator").asInt
    return ValueHistogram.calculateHistogram(anySketch, "Frequency") {
      it.values[valueIndex] != -1L
    }
  }

  override fun encryptSketch(sketch: Sketch, combinedPublicKey: ElGamalPublicKey): ByteString {
    val request: EncryptSketchRequest = EncryptSketchRequest.newBuilder().apply {
      this.sketch = sketch
      maximumValue = MAX_COUNTER_VALUE
      curveId = combinedPublicKey.ellipticCurveId.toLong()
      elGamalKeysBuilder.elGamalG = combinedPublicKey.generator
      elGamalKeysBuilder.elGamalY = combinedPublicKey.element
      destroyedRegisterStrategy = FLAGGED_KEY // for LL_V2 protocol
    }.build()
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
    frequency: Map<Long, Double>
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

  private suspend fun getMetricRequisition(
    dataProviderId: ApiId,
    campaignId: ApiId
  ): MetricRequisition {
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(250))

    var requisition: MetricRequisition? = null
    while (requisition == null) {
      throttler.onReady {
        requisition = loadMetricRequisitions(dataProviderId, campaignId)
      }
    }
    return requisition as MetricRequisition
  }

  private suspend fun loadMetricRequisitions(
    dataProviderId: ApiId,
    campaignId: ApiId
  ): MetricRequisition? {
    val request = ListMetricRequisitionsRequest.newBuilder().apply {
      parentBuilder.apply {
        this.dataProviderId = dataProviderId.value
        this.campaignId = campaignId.value
      }
      filterBuilder.addStates(MetricRequisition.State.UNFULFILLED)
      pageSize = 1
    }.build()
    val response = publisherDataStub.listMetricRequisitions(request)
    check(response.metricRequisitionsCount <= 1) { "Too many requisitions: $response" }
    return response.metricRequisitionsList.firstOrNull()
  }

  @OptIn(ExperimentalCoroutinesApi::class) // For `merge`.
  override suspend fun uploadMetricValue(
    metricValueKey: MetricRequisition.Key,
    encryptedSketch: ByteString
  ) {
    val header = flowOf(
      UploadMetricValueRequest.newBuilder().apply {
        headerBuilder.key = metricValueKey
      }.build()
    )
    val bodyContent =
      encryptedSketch.asBufferedFlow(STREAM_BYTE_BUFFER_SIZE)
        .map {
          UploadMetricValueRequest.newBuilder().apply {
            chunkBuilder.data = it
          }.build()
        }
    val request = merge(header, bodyContent)

    publisherDataStub.uploadMetricValue(request)
    logger.info("Encrypted Sketch successfully sent to the Publisher Data Service.")
  }

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
        directoryPath = Paths.get("any_sketch_java/src/main/java/org/wfanet/estimation")
      )
      loadLibrary(
        name = "sketch_encrypter_adapter",
        directoryPath = Paths.get("any_sketch_java/src/main/java/org/wfanet/anysketch/crypto")
      )
    }
  }
}

private data class GeneratedCampaign(
  val dataProviderId: ExternalId,
  val campaignId: ExternalId,
  val sketch: AnySketch
)

private data class ReportConfigAndScheduleId(val reportConfig: ExternalId, val schedule: ExternalId)

private suspend fun PublisherDataCoroutineStub.getCombinedPublicKey(
  resourceKey: CombinedPublicKey.Key
): CombinedPublicKey {
  return getCombinedPublicKey(
    GetCombinedPublicKeyRequest.newBuilder().apply {
      key = resourceKey
    }.build()
  )
}

/**
 * Returns the value or throws a [RuntimeException] wrapping the cause with the
 * provided message.
 */
private fun <T> Result<T>.getOrThrowWithMessage(provideMessage: () -> String): T {
  return getOrElse { cause ->
    throw RuntimeException(provideMessage(), cause)
  }
}
