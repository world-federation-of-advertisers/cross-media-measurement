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

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.FieldValueList
import com.google.cloud.bigquery.Job
import com.google.cloud.bigquery.JobId
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.QueryParameterValue
import com.google.cloud.bigquery.StandardSQLTypeName
import com.google.cloud.bigquery.TableResult
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.type.Date
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
import org.wfanet.measurement.internal.loadtest.LabeledEvent
import org.wfanet.measurement.internal.loadtest.TestResult
import org.wfanet.measurement.kingdom.db.KingdomRelationalDatabase
import org.wfanet.measurement.kingdom.db.ReportDatabase
import org.wfanet.measurement.kingdom.db.streamReportsFilter
import org.wfanet.measurement.kingdom.db.testing.DatabaseTestHelper
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.createBlob
import org.wfanet.measurement.system.v1alpha.GlobalComputation

private const val MAX_COUNTER_VALUE = 5
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

  suspend fun process(
    relationalDatabase: KingdomRelationalDatabase,
    relationalDatabaseTestHelper: DatabaseTestHelper
  ) {
    logger.info("Starting with RunID: $runId ...")
    val testResult = TestResult.newBuilder().setRunId(runId)

    val externalAdvertiserId = createAdvertiser(relationalDatabaseTestHelper)

    val generatedCampaigns =
      List(dataProviderCount) {
          relationalDatabaseTestHelper.createDataProvider(externalAdvertiserId).toList()
        }
        .flatten()

    // Schedule a report before loading the metric requisitions.
    val campaignIds = generatedCampaigns.map { it.campaignId }
    val reportConfigAndScheduleId =
      relationalDatabaseTestHelper.scheduleReport(externalAdvertiserId, campaignIds)

    val anySketches = generatedCampaigns.map { it.sketch }
    val combinedAnySketch = SketchProtos.toAnySketch(sketchConfig).apply { mergeAll(anySketches) }
    val reach = estimateCardinality(combinedAnySketch)
    val frequency = estimateFrequency(combinedAnySketch)
    val storedResultsPath = storeEstimationResults(reach, frequency)
    testResult.computationBlobKey = storedResultsPath
    logger.info("Estimation Results saved with blob key: $storedResultsPath")

    // Finally, we are sending encrypted sketches to the PublisherDataService.
    coroutineScope { generatedCampaigns.forEach { launch { encryptAndSend(it, testResult) } } }

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

  suspend fun process(
    relationalDatabaseTestHelper: DatabaseTestHelper,
    bigQuery: BigQuery,
    tableName: String,
    queryParameter: QueryParameter
  ) {
    logger.info("Starting with RunID: $runId ...")
    val testResult = TestResult.newBuilder().setRunId(runId)

    // Validate all the parameters.
    validateParameters(queryParameter)
    val events: Map<Long, List<Long>> =
      bigQuery
        .getLabeledEvents(
          tableName = tableName,
          publisher = queryParameter.publisherIds.map { it.toInt() }.toTypedArray(),
          sex = queryParameter.sex,
          ageGroup = queryParameter.ageGroups,
          socialGrade = queryParameter.socialGrades,
          complete = queryParameter.complete,
          beginDate = queryParameter.beginDate,
          endDate = queryParameter.endDate
        )
        .groupBy({ it.publisherId }, { it.vid })

    val externalAdvertiserId = createAdvertiser(relationalDatabaseTestHelper)

    val campaigns = mutableListOf<GeneratedCampaign>()
    events.forEach { event ->
      val dataProvider = relationalDatabaseTestHelper.createDataProvider()
      logger.info(
        "PublisherId: ${event.key} written to Spanner as ${dataProvider.externalDataProviderId}."
      )
      val externalDataProviderId = ExternalId(dataProvider.externalDataProviderId)
      campaigns.add(
        relationalDatabaseTestHelper.createCampaign(
          event.value,
          externalAdvertiserId,
          externalDataProviderId,
          "Publisher ${event.key} Campaign"
        )
      )
    }

    // Schedule a report before loading the metric requisitions.
    val campaignIds = campaigns.map { it.campaignId }
    relationalDatabaseTestHelper.scheduleReport(externalAdvertiserId, campaignIds)

    // Finally, we are sending encrypted sketches to the PublisherDataService.
    coroutineScope { campaigns.forEach { launch { encryptAndSend(it, testResult) } } }
  }

  /**
   * Validates the given query parameters.
   *
   * @param queryParameter a [QueryParameter] consists of all parameters to query.
   * @throws IllegalArgumentException
   */
  private fun validateParameters(queryParameter: QueryParameter) {
    queryParameter.sex.forEach { it.toSex() }
    queryParameter.socialGrades.forEach { it.toSocialGrade() }
    queryParameter.ageGroups.forEach { it.toAgeGroup() }
    queryParameter.complete.forEach { it.toComplete() }
  }

  /**
   * Get demo data from BigQuery table.
   *
   * @param tableName name of the BigQuery table to query.
   * @param publisher Array of publisher ids to filter in query.
   * @param sex Array of sexes to filter in query.
   * @param ageGroup Array of age groups to filter in query.
   * @param socialGrade Array of age social grades to filter in query.
   * @param complete Array of completion to filter in query.
   * @param beginDate String of begin date to filter in query.
   * @param endDate String of end date to filter in query.
   * @return List of [LabeledEvent]s from the executed query.
   */
  private fun BigQuery.getLabeledEvents(
    tableName: String,
    publisher: Array<Int>,
    sex: Array<String>,
    ageGroup: Array<String>,
    socialGrade: Array<String>,
    complete: Array<String>,
    beginDate: String,
    endDate: String
  ): List<LabeledEvent> {
    val queryConfig =
      buildQueryConfig(
        tableName,
        publisher,
        sex,
        ageGroup,
        socialGrade,
        complete,
        beginDate,
        endDate
      )
    val jobId: JobId = JobId.of(UUID.randomUUID().toString())

    var queryJob: Job = this.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())
    logger.info("Connected to BigQuery Successfully.")
    queryJob = queryJob.waitFor()

    if (queryJob == null || queryJob.status.error != null) {
      throw RuntimeException()
    }
    logger.info("Running query on BigQuery table.")
    val result: TableResult = queryJob.getQueryResults()

    val labeledEvents = mutableListOf<LabeledEvent>()
    for (row: FieldValueList in result.iterateAll()) {
      val event = buildLabeledEvent(row)
      labeledEvents.add(event)
    }
    logger.info("Query executed successfully!")
    logger.info("Events size: ${labeledEvents.size}")

    return labeledEvents
  }

  // Builds a query based on the parameters given.
  private fun buildQueryConfig(
    tableName: String,
    publisher: Array<Int>,
    sex: Array<String>,
    ageGroup: Array<String>,
    socialGrade: Array<String>,
    complete: Array<String>,
    beginDate: String,
    endDate: String
  ): QueryJobConfiguration {
    val query =
      """
      SELECT publisher_id, event_id, sex, age_group, social_grade, complete, vid, date
      FROM `$tableName`
      WHERE publisher_id IN UNNEST(@publisher)
      AND sex IN UNNEST(@sex)
      AND age_group IN UNNEST(@age_group)
      AND social_grade IN UNNEST(@social_grade)
      AND complete IN UNNEST(@complete)
      AND date BETWEEN @begin_date AND @end_date
      """.trimIndent()
    val queryConfig: QueryJobConfiguration =
      QueryJobConfiguration.newBuilder(query)
        .addNamedParameter(
          "publisher",
          QueryParameterValue.array(publisher, StandardSQLTypeName.INT64)
        )
        .addNamedParameter("sex", QueryParameterValue.array(sex, StandardSQLTypeName.STRING))
        .addNamedParameter(
          "age_group",
          QueryParameterValue.array(ageGroup, StandardSQLTypeName.STRING)
        )
        .addNamedParameter(
          "social_grade",
          QueryParameterValue.array(socialGrade, StandardSQLTypeName.STRING)
        )
        .addNamedParameter(
          "complete",
          QueryParameterValue.array(complete, StandardSQLTypeName.STRING)
        )
        .addNamedParameter("begin_date", QueryParameterValue.date(beginDate))
        .addNamedParameter("end_date", QueryParameterValue.date(endDate))
        .build()
    return queryConfig
  }

  // Converts BigQuery table row into LabeledEvent proto.
  private fun buildLabeledEvent(row: FieldValueList): LabeledEvent {
    return LabeledEvent.newBuilder()
      .setEventId(row.get("event_id").longValue)
      .setPublisherId(row.get("publisher_id").longValue)
      .setSex(row.get("sex").stringValue.toSex())
      .setAgeGroup(row.get("age_group").stringValue.toAgeGroup())
      .setSocialGrade(row.get("social_grade").stringValue.toSocialGrade())
      .setComplete(row.get("complete").stringValue.toComplete())
      .setVid(row.get("vid").longValue)
      .setDate(row.get("date").stringValue.toDate())
      .build()
  }

  // Utility function that converts String SQL date format into Date.
  private fun String.toDate(): Date {
    val date = com.google.cloud.Date.parseDate(this)
    return Date.newBuilder().setDay(date.dayOfMonth).setMonth(date.month).setYear(date.year).build()
  }

  // Utility function that converts String Sex into enum Sex.
  private val sexMap: Map<String, LabeledEvent.Sex> =
    mapOf("M" to LabeledEvent.Sex.MALE, "F" to LabeledEvent.Sex.FEMALE)
  private fun String.toSex(): LabeledEvent.Sex {
    return sexMap[this] ?: throw IllegalArgumentException("Unsupported value for Sex parameter.")
  }

  // Utility function that converts String AgeGroup into enum AgeGroup.
  private val ageGroupMap: Map<String, LabeledEvent.AgeGroup> =
    mapOf(
      "18_34" to LabeledEvent.AgeGroup._18_34,
      "35_54" to LabeledEvent.AgeGroup._35_54,
      "55+" to LabeledEvent.AgeGroup._55
    )
  private fun String.toAgeGroup(): LabeledEvent.AgeGroup {
    return ageGroupMap[this]
      ?: throw IllegalArgumentException("Unsupported value for Age Group parameter.")
  }

  // Utility function that converts String Social grade into enum SocialGrade.
  private val socialGradeMap: Map<String, LabeledEvent.SocialGrade> =
    mapOf("ABC1" to LabeledEvent.SocialGrade.ABC1, "C2DE" to LabeledEvent.SocialGrade.C2DE)
  private fun String.toSocialGrade(): LabeledEvent.SocialGrade {
    return socialGradeMap[this]
      ?: throw IllegalArgumentException("Unsupported value for Social Grade parameter.")
  }

  // Utility function that converts String Complete into enum Complete.
  private val completeMap: Map<String, LabeledEvent.Complete> =
    mapOf("0" to LabeledEvent.Complete.INCOMPLETE, "1" to LabeledEvent.Complete.COMPLETE)
  private fun String.toComplete(): LabeledEvent.Complete {
    return completeMap[this]
      ?: throw IllegalArgumentException("Unsupported value for Complete parameter.")
  }

  private suspend fun ReportDatabase.getFinishedReport(
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
      }
      .first()
  }

  private suspend fun createAdvertiser(databaseTestHelper: DatabaseTestHelper): ExternalId {
    val advertiser = databaseTestHelper.createAdvertiser()
    logger.info("Created an Advertiser: $advertiser")
    return ExternalId(advertiser.externalAdvertiserId)
  }

  private suspend fun DatabaseTestHelper.createDataProvider(
    externalAdvertiserId: ExternalId
  ): Flow<GeneratedCampaign> {
    val dataProvider = createDataProvider()
    logger.info("Created a Data Provider: $dataProvider")
    val externalDataProviderId = ExternalId(dataProvider.externalDataProviderId)

    return generateReach().asFlow().map { reach ->
      createCampaign(reach.toList(), externalAdvertiserId, externalDataProviderId)
    }
  }

  private suspend fun DatabaseTestHelper.createCampaign(
    reach: List<Long>,
    externalAdvertiserId: ExternalId,
    externalDataProviderId: ExternalId,
    campaignName: String = "Campaign name"
  ): GeneratedCampaign {
    val campaign = createCampaign(externalDataProviderId, externalAdvertiserId, campaignName)
    logger.info("Created a Campaign $campaign")
    val externalCampaignId = ExternalId(campaign.externalCampaignId)
    val anySketch = generateSketch(reach)

    return GeneratedCampaign(externalDataProviderId, externalCampaignId, anySketch)
  }

  private suspend fun DatabaseTestHelper.scheduleReport(
    externalAdvertiserId: ExternalId,
    campaignIds: List<ExternalId>
  ): ReportConfigAndScheduleId {
    val metricDefinition =
      MetricDefinition.newBuilder()
        .apply {
          sketchBuilder.apply {
            sketchConfigId = 12345L
            type = SketchMetricDefinition.Type.IMPRESSION_REACH_AND_FREQUENCY
          }
        }
        .build()
    val reportConfig =
      createReportConfig(
        ReportConfig.newBuilder()
          .apply {
            this.externalAdvertiserId = externalAdvertiserId.value
            numRequisitions = campaignIds.size.toLong()
            reportConfigDetailsBuilder.apply {
              addMetricDefinitions(metricDefinition)
              reportDurationBuilder.apply {
                unit = TimePeriod.Unit.DAY
                count = 1
              }
            }
          }
          .build(),
        campaignIds
      )
    logger.info("Created a ReportConfig: $reportConfig")
    val externalReportConfigId = ExternalId(reportConfig.externalReportConfigId)
    val schedule =
      createSchedule(
        ReportConfigSchedule.newBuilder()
          .apply {
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
          }
          .build()
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

    val requisition =
      runCatching { getMetricRequisition(dataProviderId, campaignId) }.getOrThrowWithMessage {
        "Error getting metric requisition. Data provider: $dataProviderId, campaign: $campaignId"
      }
    val resourceKey = requisition.combinedPublicKey
    val combinedPublicKey =
      publicKeyCache.getOrPut(resourceKey.combinedPublicKeyId) {
        runCatching { publisherDataStub.getCombinedPublicKey(resourceKey) }.getOrThrowWithMessage {
          "Error getting combined public key $resourceKey"
        }
      }
    val encryptedSketch =
      encryptAndStore(generatedCampaign.sketch, combinedPublicKey.encryptionKey, testResult)

    runCatching { uploadMetricValue(requisition.key, encryptedSketch) }.getOrThrowWithMessage {
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

  override fun generateSketch(reach: List<Long>): AnySketch {
    val anySketch: AnySketch = SketchProtos.toAnySketch(sketchConfig)
    for (value: Long in reach) {
      anySketch.insert(value, mapOf("frequency" to 1L))
    }
    return anySketch
  }

  override fun estimateCardinality(anySketch: AnySketch): Long {
    val activeRegisterCount = anySketch.toList().size.toLong()
    return Estimators.EstimateCardinalityLiquidLegions(DECAY_RATE, INDEX_SIZE, activeRegisterCount)
  }

  override fun estimateFrequency(anySketch: AnySketch): Map<Long, Double> {
    val valueIndex = anySketch.getValueIndex("SamplingIndicator").asInt
    val actualHistogram =
      ValueHistogram.calculateHistogram(anySketch, "Frequency") { it.values[valueIndex] != -1L }
    val result = mutableMapOf<Long, Double>()
    actualHistogram.forEach {
      val key = minOf(it.key, MAX_COUNTER_VALUE.toLong())
      result[key] = result.getOrDefault(key, 0.0) + it.value
    }
    return result
  }

  override fun encryptSketch(sketch: Sketch, combinedPublicKey: ElGamalPublicKey): ByteString {
    val request: EncryptSketchRequest =
      EncryptSketchRequest.newBuilder()
        .apply {
          this.sketch = sketch
          maximumValue = MAX_COUNTER_VALUE
          curveId = combinedPublicKey.ellipticCurveId.toLong()
          elGamalKeysBuilder.generator = combinedPublicKey.generator
          elGamalKeysBuilder.element = combinedPublicKey.element
          destroyedRegisterStrategy = FLAGGED_KEY // for LL_V2 protocol
        }
        .build()
    val response =
      EncryptSketchResponse.parseFrom(SketchEncrypterAdapter.EncryptSketch(request.toByteArray()))
    return response.encryptedSketch
  }

  override suspend fun storeSketch(sketch: Sketch): String {
    return storeBlob(sketch.toByteString())
  }

  override suspend fun storeEncryptedSketch(encryptedSketch: ByteString): String {
    return storeBlob(encryptedSketch)
  }

  override suspend fun storeEstimationResults(reach: Long, frequency: Map<Long, Double>): String {
    val computation =
      GlobalComputation.newBuilder()
        .apply {
          keyBuilder.globalComputationId = GLOBAL_COMPUTATION_ID
          state = GlobalComputation.State.SUCCEEDED
          resultBuilder.apply {
            setReach(reach)
            putAllFrequency(frequency)
          }
        }
        .build()
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
      throttler.onReady { requisition = loadMetricRequisitions(dataProviderId, campaignId) }
    }
    return requisition as MetricRequisition
  }

  private suspend fun loadMetricRequisitions(
    dataProviderId: ApiId,
    campaignId: ApiId
  ): MetricRequisition? {
    val request =
      ListMetricRequisitionsRequest.newBuilder()
        .apply {
          parentBuilder.apply {
            this.dataProviderId = dataProviderId.value
            this.campaignId = campaignId.value
          }
          filterBuilder.addStates(MetricRequisition.State.UNFULFILLED)
          pageSize = 1
        }
        .build()
    val response = publisherDataStub.listMetricRequisitions(request)
    check(response.metricRequisitionsCount <= 1) { "Too many requisitions: $response" }
    return response.metricRequisitionsList.firstOrNull()
  }

  @OptIn(ExperimentalCoroutinesApi::class) // For `merge`.
  override suspend fun uploadMetricValue(
    metricValueKey: MetricRequisition.Key,
    encryptedSketch: ByteString
  ) {
    val header =
      flowOf(
        UploadMetricValueRequest.newBuilder().apply { headerBuilder.key = metricValueKey }.build()
      )
    val bodyContent =
      encryptedSketch.asBufferedFlow(STREAM_BYTE_BUFFER_SIZE).map {
        UploadMetricValueRequest.newBuilder().apply { chunkBuilder.data = it }.build()
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

data class QueryParameter(
  val publisherIds: Array<String>,
  val sex: Array<String>,
  val ageGroups: Array<String>,
  val socialGrades: Array<String>,
  val complete: Array<String>,
  val beginDate: String,
  val endDate: String
)

private data class GeneratedCampaign(
  val dataProviderId: ExternalId,
  val campaignId: ExternalId,
  val sketch: AnySketch
)

private data class ReportConfigAndScheduleId(
  val reportConfig: ExternalId,
  val schedule: ExternalId
)

private suspend fun PublisherDataCoroutineStub.getCombinedPublicKey(
  resourceKey: CombinedPublicKey.Key
): CombinedPublicKey {
  return getCombinedPublicKey(
    GetCombinedPublicKeyRequest.newBuilder().apply { key = resourceKey }.build()
  )
}

/**
 * Returns the value or throws a [RuntimeException] wrapping the cause with the provided message.
 */
private fun <T> Result<T>.getOrThrowWithMessage(provideMessage: () -> String): T {
  return getOrElse { cause -> throw RuntimeException(provideMessage(), cause) }
}
