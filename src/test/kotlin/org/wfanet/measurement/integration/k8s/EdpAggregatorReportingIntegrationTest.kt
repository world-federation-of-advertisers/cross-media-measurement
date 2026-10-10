/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.k8s

import com.google.cloud.storage.StorageOptions
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import com.google.crypto.tink.InsecureSecretKeyAccess
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.protobuf.TypeRegistry
import com.google.protobuf.timestamp
import com.google.protobuf.util.JsonFormat
import com.google.type.DayOfWeek
import com.google.type.interval
import io.grpc.Channel
import io.grpc.ManagedChannel
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.security.KeyPair
import java.security.cert.X509Certificate
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneId
import java.util.UUID
import java.util.logging.Logger
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import okhttp3.OkHttpClient
import okhttp3.tls.HandshakeCertificates
import okhttp3.tls.HeldCertificate
import okhttp3.tls.decodeCertificatePem
import org.junit.ClassRule
import org.junit.FixMethodOrder
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.MethodSorters
import org.junit.runners.model.Statement
import org.measurement.integration.k8s.testing.EdpaReportingIntegrationTestConfig
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.testing.OpenIdProvider
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.config.access.OpenIdProvidersConfig
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.MediaType
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.entityKey
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata as eventGroupMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup
import org.wfanet.measurement.integration.common.EventGroupConfig
import org.wfanet.measurement.integration.common.HighOverlapExpectedReach
import org.wfanet.measurement.integration.common.HighOverlapExpectedReach.ExpectedMetrics
import org.wfanet.measurement.integration.common.ImpressionTestDataConfigs
import org.wfanet.measurement.loadtest.reporting.ReportingUserSimulator
import org.wfanet.measurement.reporting.service.api.v2alpha.ImpressionQualificationFilterKey
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.DimensionSpecKt
import org.wfanet.measurement.reporting.v2alpha.EventGroup as ReportingEventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as ReportingEventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ResultGroup
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.ResultGroupSpec
import org.wfanet.measurement.reporting.v2alpha.dimensionSpec
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.reportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.reportingUnit
import org.wfanet.measurement.reporting.v2alpha.resultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.resultGroupSpec
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * Tests BasicReport reporting over the EDP Aggregator, against a deployed environment.
 *
 * The rules below provision the high overlap synthetic dataset the reports are computed over. It is
 * pre-labeled and carries its own Population, ModelLine and EventGroups, so none of the VID
 * labeling pipeline or low overlap data set that `EdpAggregatorCorrectnessTest` sets up is needed
 * here. Every rule is a no-op unless `model_line` is set, so an environment opts in only once its
 * ModelLine has been provisioned.
 *
 * Tests run in name order so that the cheaper media type report reports its result before the
 * weekly report, which computes far more metrics, risks exhausting the test timeout.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class EdpAggregatorReportingIntegrationTest {

  /** Writes the high overlap EventGroup blob and waits for `EventGroupSync` to register them. */
  private class UploadEventGroups : TestRule {

    private val bucket = TEST_CONFIG.storageBucket
    private val googleProjectId: String =
      System.getenv("GOOGLE_CLOUD_PROJECT") ?: error("GOOGLE_CLOUD_PROJECT must be set")
    private val storageClient = StorageOptions.getDefaultInstance().service

    /**
     * Per-EDP blobs for the high overlap EventGroups.
     *
     * [objectKey] is a distinct object under the EDP's `event-groups/` prefix, which is what the
     * DataWatcher matches on; `EventGroupSync` reads whichever blob triggered it. [objectMapKey] is
     * not derived from it: the sync always writes its output to the `event_group_map_blob_uri`
     * fixed per EDP in `EventGroupSyncConfig`, so it is shared with every other test seeding this
     * EDP.
     */
    private data class EdpStorage(
      val objectMapKey: String,
      val objectKey: String,
      val blobUri: String,
      val eventGroupReferenceIds: Set<String>,
    )

    private val edpStorageList: List<EdpStorage> =
      highOverlapEventGroupRefIdsByEdp.map { (edpName, referenceIds) ->
        EdpStorage(
          objectMapKey = "$edpName/event-groups-map/$edpName-event-group.binpb",
          objectKey = "$edpName/event-groups/$edpName-high-overlap-event-group.binpb",
          blobUri = "gs://$bucket/$edpName/event-groups/$edpName-high-overlap-event-group.binpb",
          eventGroupReferenceIds = referenceIds,
        )
      }

    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          if (MODEL_LINE.isEmpty()) {
            logger.info("No high overlap model line configured; skipping EventGroup upload.")
          } else {
            runBlocking {
              edpStorageList.forEach { storageClient.delete(bucket, it.objectMapKey) }
              val allEventGroups = buildEventGroups(highOverlapEventGroupMap)
              for (edpStorage in edpStorageList) {
                val groups =
                  allEventGroups.filter {
                    it.eventGroupReferenceId in edpStorage.eventGroupReferenceIds
                  }
                uploadEventGroups(edpStorage, groups)
                waitForEventGroupSyncToComplete(edpStorage)
              }
              logger.info("High overlap EventGroup sync completed.")
            }
          }
          base.evaluate()
        }
      }
    }

    private suspend fun waitForEventGroupSyncToComplete(storage: EdpStorage) {
      withTimeout(EVENT_GROUP_SYNC_TIMEOUT) {
        while (storageClient.get(bucket, storage.objectMapKey) == null) {
          logger.info("Waiting on high overlap Event Group Sync to complete...")
          delay(EVENT_GROUP_SYNC_POLLING_INTERVAL)
        }
      }
    }

    private suspend fun uploadEventGroups(storage: EdpStorage, eventGroups: List<EventGroup>) {
      val eventGroupsBlobUri = SelectedStorageClient.parseBlobUri(storage.blobUri)
      MesosRecordIoStorageClient(
          SelectedStorageClient(
            blobUri = eventGroupsBlobUri,
            rootDirectory = null,
            projectId = googleProjectId,
          )
        )
        .writeBlob(storage.objectKey, eventGroups.asFlow().map { it.toByteString() })
    }

    private fun buildEventGroups(eventGroupMap: Map<String, EventGroupConfig>): List<EventGroup> {
      return eventGroupMap.flatMap { (referenceId, config) ->
        when (config) {
          // Every high overlap event group carries an entity key: EventGroupSync filters its
          // Kingdom listing by entity type, so one without a key would be re-created on every sync.
          is EventGroupConfig.LegacySpec ->
            error("high overlap event group $referenceId has no entity key")
          // One EventGroup per entity key, spanning every date spec. The Kingdom enforces
          // uniqueness on both reference ID and entity key, so a row per date spec would
          // collide.
          is EventGroupConfig.MultiEntityKey ->
            config.entityKeySpecs.map { entityKeySpec ->
              val dateRanges = entityKeySpec.spec.dateSpecsList.map { it.dateRange }
              val startTime =
                dateRanges
                  .minOf { LocalDate.of(it.start.year, it.start.month, it.start.day) }
                  .atStartOfDay(ZONE_ID)
                  .toInstant()
              // Both bounds are exclusive: the date spec's and the interval's.
              val endTime =
                dateRanges.maxOf { it.endExclusive.toLocalDate() }.atStartOfDay(ZONE_ID).toInstant()
              eventGroup {
                eventGroupReferenceId =
                  "${entityKeySpec.entityKey.entityType}-${entityKeySpec.entityKey.entityId}"
                measurementConsumer = TEST_CONFIG.measurementConsumer
                dataAvailabilityInterval = interval {
                  this.startTime = timestamp { seconds = startTime.epochSecond }
                  this.endTime = timestamp { seconds = endTime.epochSecond }
                }
                eventGroupMetadata = eventGroupMetadata {
                  adMetadata = adMetadata {
                    campaignMetadata = campaignMetadata {
                      brand = "some-brand"
                      campaign = "some-campaign"
                    }
                  }
                  entityKeySpec.entityMetadata?.let { entityMetadata = it }
                }
                entityKey = entityKey {
                  entityType = entityKeySpec.entityKey.entityType
                  entityId = entityKeySpec.entityKey.entityId
                }
                // Every segment alternates media per stripe.
                mediaTypes += listOf(MediaType.VIDEO, MediaType.DISPLAY)
              }
            }
        }
      }
    }

    companion object {
      // Inferred as kotlin.time.Duration, which withTimeout and delay take; the java.time.Duration
      // this file imports for the HTTP client is a different type.
      private val EVENT_GROUP_SYNC_TIMEOUT = 5.minutes
      private val EVENT_GROUP_SYNC_POLLING_INTERVAL = 5.seconds
    }
  }

  /**
   * Writes the `done` markers that trigger `DataAvailabilitySync` for the high overlap impressions.
   */
  private class CreateDoneBlobs : TestRule {

    private val bucket = TEST_CONFIG.storageBucket
    private val googleProjectId: String =
      System.getenv("GOOGLE_CLOUD_PROJECT") ?: error("GOOGLE_CLOUD_PROJECT must be set")

    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          if (MODEL_LINE.isEmpty()) {
            logger.info("No high overlap model line configured; skipping DONE blobs.")
          } else {
            runBlocking {
              val modelLineId =
                requireNotNull(ModelLineKey.fromName(MODEL_LINE)) {
                    "model_line must be a full ModelLine resource name: $MODEL_LINE"
                  }
                  .modelLineId
              val paths =
                highOverlapDatesByImpressionPath.flatMap { (impressionPath, dates) ->
                  dates.map { date ->
                    "gs://$bucket/$impressionPath/model-line/$modelLineId/$date/done"
                  }
                }
              logger.info("Creating ${paths.size} high overlap DONE blob(s)...")
              writeDoneBlobs(paths)
            }
          }
          base.evaluate()
        }
      }
    }

    private suspend fun writeDoneBlobs(paths: List<String>) {
      paths.forEach { path ->
        val doneBlobUri = SelectedStorageClient.parseBlobUri(path)
        val selectedStorageClient =
          SelectedStorageClient(
            blobUri = doneBlobUri,
            rootDirectory = null,
            projectId = googleProjectId,
          )
        selectedStorageClient.getBlob(doneBlobUri.key)?.delete()
        selectedStorageClient.writeBlob(doneBlobUri.key, emptyFlow())
      }
    }
  }

  /** Builds the Reporting harness once the dataset is in place. */
  private class ReportingSystem : TestRule {

    private lateinit var _harness: ReportingUserSimulator

    val harness: ReportingUserSimulator
      get() = _harness

    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          try {
            if (MODEL_LINE.isNotEmpty()) {
              _harness = createReportingTestHarness()
            }
            base.evaluate()
          } finally {
            channels.forEach { it.shutdown() }
          }
        }
      }
    }

    private fun createReportingTestHarness(): ReportingUserSimulator {
      val reportingServiceUrl: HttpUrl =
        TEST_CONFIG.reportingServiceEndpoint.toHttpUrlOrNull()
          ?: throw IllegalArgumentException(
            "Invalid reporting service endpoint '${TEST_CONFIG.reportingServiceEndpoint}'"
          )

      val secretFiles = runtimePath(SECRET_FILES_PATH)
      val clientCertificate: X509Certificate =
        secretFiles.resolve(MC_TLS_CERT_NAME).toFile().readText().decodeCertificatePem()
      val privateKey =
        readPrivateKey(
          secretFiles.resolve(MC_TLS_KEY_NAME).toFile(),
          clientCertificate.publicKey.algorithm,
        )
      val handshakeCertificates =
        HandshakeCertificates.Builder()
          .addTrustedCertificate(
            secretFiles.resolve(REPORTING_ROOT_CERT_NAME).toFile().readText().decodeCertificatePem()
          )
          .heldCertificate(
            HeldCertificate(KeyPair(clientCertificate.publicKey, privateKey), clientCertificate)
          )
          .build()
      val okHttpReportingClient =
        OkHttpClient.Builder()
          .sslSocketFactory(
            handshakeCertificates.sslSocketFactory(),
            handshakeCertificates.trustManager,
          )
          .connectTimeout(REPORTING_HTTP_TIMEOUT)
          .readTimeout(REPORTING_HTTP_TIMEOUT)
          .writeTimeout(REPORTING_HTTP_TIMEOUT)
          .build()

      val reportingApiChannel = buildChannel(TEST_CONFIG.reportingPublicApiTarget, REPORTING_CERTS)
      val accessApiChannel = buildChannel(TEST_CONFIG.accessPublicApiTarget, ACCESS_CERTS)

      val openIdProvidersConfig =
        OpenIdProvidersConfig.newBuilder()
          .also {
            JsonFormat.parser()
              .ignoringUnknownFields()
              .merge(OPEN_ID_PROVIDERS_CONFIG_JSON_FILE.readText(), it)
          }
          .build()
      val principal =
        AbstractCorrectnessTest.createAccessPrincipal(
          TEST_CONFIG.measurementConsumer,
          accessApiChannel,
          openIdProvidersConfig.providerConfigByIssuerMap.keys.first(),
        )
      val getAccessToken = {
        OpenIdProvider(
            principal.user.issuer,
            TinkProtoKeysetFormat.parseKeyset(
              OPEN_ID_PROVIDERS_TINK_FILE.readBytes(),
              InsecureSecretKeyAccess.get(),
            ),
          )
          .generateCredentials(
            audience = TEST_CONFIG.reportingTokenAudience,
            subject = principal.user.subject,
            scopes = REPORTING_TOKEN_SCOPES,
            ttl = REPORTING_TOKEN_TTL,
          )
          .token
      }

      return ReportingUserSimulator(
        measurementConsumerName = TEST_CONFIG.measurementConsumer,
        dataProvidersClient =
          org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub(
            buildChannel(TEST_CONFIG.kingdomPublicApiTarget, MEASUREMENT_CONSUMER_CERTS)
          ),
        eventGroupsClient = ReportingEventGroupsCoroutineStub(reportingApiChannel),
        reportingSetsClient = ReportingSetsCoroutineStub(reportingApiChannel),
        okHttpReportingClient = okHttpReportingClient,
        reportingGatewayScheme = reportingServiceUrl.scheme,
        reportingGatewayHost = reportingServiceUrl.host,
        reportingGatewayPort = reportingServiceUrl.port,
        getReportingAccessToken = getAccessToken,
        modelLineName = MODEL_LINE,
      )
    }

    private fun buildChannel(target: String, certs: SigningCerts): Channel =
      buildMutualTlsChannel(target, certs, null).also { channels.add(it) }

    companion object {
      private val channels = mutableListOf<ManagedChannel>()
      private val REPORTING_HTTP_TIMEOUT: Duration = Duration.ofSeconds(30)
    }
  }

  @Test
  fun `media type and impression qualification filter report succeeds`() = runBlocking {
    check(MODEL_LINE.isNotEmpty()) { "model_line must be set to run this test" }

    val report =
      reportingSystem.harness.createBasicReportOverEventGroups(
        runId = UUID.randomUUID().toString(),
        reportTitle = "Media type and impression qualification filter breakdown",
        eventGroupReferenceIds = REPORT_EVENT_GROUP_REF_IDS,
        eventGroupEntityTypes = reportEventGroupEntityTypes,
        reportStart = REPORT_START,
        reportEnd = REPORT_END,
        qualificationFilters = ReportingUserSimulator.mediaTypeQualificationFilters(),
        buildResultGroupSpecs = ::mediaTypeAndIqfResultGroupSpecs,
      )

    assertThat(report.state).isEqualTo(BasicReport.State.SUCCEEDED)
    assertReportGroups(report)
    assertMetricsMatchSpecs(report)
  }

  @Test
  fun `weekly report grouped by gender across EDPs succeeds`() = runBlocking {
    check(MODEL_LINE.isNotEmpty()) { "model_line must be set to run this test" }

    val report =
      reportingSystem.harness.createBasicReportOverEventGroups(
        runId = UUID.randomUUID().toString(),
        reportTitle = "Weekly cumulative and non-cumulative metrics by gender",
        eventGroupReferenceIds = WEEKLY_REPORT_EVENT_GROUP_REF_IDS,
        eventGroupEntityTypes = weeklyReportEventGroupEntityTypes,
        reportStart = WEEKLY_REPORT_START,
        reportEnd = WEEKLY_REPORT_END,
        qualificationFilters =
          listOf(
            reportingImpressionQualificationFilter {
              impressionQualificationFilter =
                ImpressionQualificationFilterKey(ReportingUserSimulator.AMI_FILTER_ID).toName()
            }
          ),
        buildResultGroupSpecs = ::weeklyResultGroupSpecs,
      )

    assertThat(report.state).isEqualTo(BasicReport.State.SUCCEEDED)
    assertWeeklyShape(report)
    assertCumulativeReachIsMonotonic(report)
    assertPopulationSizeMatchesSpec(report)
  }

  /**
   * A single result group over every reported DataProvider, weekly, grouped by gender.
   *
   * Only `reach` and `percent_reach` are requested cumulatively; the remaining basic metrics are
   * not supported for cumulative weekly.
   */
  private fun weeklyResultGroupSpecs(
    eventGroups: List<ReportingEventGroup>
  ): List<ResultGroupSpec> {
    val dataProviderNames: List<String> =
      eventGroups.map { it.cmmsDataProvider }.distinct().sorted()
    require(dataProviderNames.size == EDP_NAMES.size) {
      "Expected one DataProvider per EDP in $EDP_NAMES, got $dataProviderNames"
    }

    return listOf(
      resultGroupSpec {
        title = WEEKLY_GROUP_TITLE
        reportingUnit = reportingUnit { components += dataProviderNames }
        metricFrequency = metricFrequencySpec { weekly = WEEKLY_METRIC_FREQUENCY }
        dimensionSpec = dimensionSpec {
          grouping = DimensionSpecKt.grouping { eventTemplateFields += GENDER_FIELD_PATH }
        }
        resultGroupMetricSpec = resultGroupMetricSpec {
          populationSize = true
          reportingUnit =
            ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
              cumulative =
                ResultGroupMetricSpecKt.basicMetricSetSpec {
                  reach = true
                  percentReach = true
                }
              nonCumulative =
                ResultGroupMetricSpecKt.basicMetricSetSpec {
                  reach = true
                  kPlusReach = K_PLUS_REACH
                  impressions = true
                  averageFrequency = true
                }
              stackedIncrementalReach = true
            }
          component =
            ResultGroupMetricSpecKt.componentMetricSetSpec {
              nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
            }
        }
      }
    )
  }

  /**
   * One result group for a single DataProvider and one for every DataProvider the reported
   * EventGroups span, both over the whole reporting interval.
   */
  private fun mediaTypeAndIqfResultGroupSpecs(
    eventGroups: List<ReportingEventGroup>
  ): List<ResultGroupSpec> {
    val eventGroupsByReferenceId = eventGroups.associateBy { it.eventGroupReferenceId }
    val singleEdpDataProviders =
      SINGLE_EDP_EVENT_GROUP_REF_IDS.map { eventGroupsByReferenceId.getValue(it).cmmsDataProvider }
        .distinct()
    require(singleEdpDataProviders.size == 1) {
      "The single-EDP EventGroups span more than one DataProvider: $singleEdpDataProviders"
    }
    val dataProviderNames: List<String> =
      eventGroups.map { it.cmmsDataProvider }.distinct().sorted()
    require(dataProviderNames.size >= 2) {
      "The cross-publisher result group needs at least two DataProviders, got $dataProviderNames"
    }

    return listOf(
      resultGroupSpec {
        title = ReportingUserSimulator.SINGLE_EDP_GROUP_TITLE
        reportingUnit = reportingUnit { components += singleEdpDataProviders.single() }
        metricFrequency = metricFrequencySpec { total = true }
        dimensionSpec = dimensionSpec {}
        resultGroupMetricSpec = resultGroupMetricSpec {
          populationSize = true
          component =
            ResultGroupMetricSpecKt.componentMetricSetSpec {
              cumulative =
                ResultGroupMetricSpecKt.basicMetricSetSpec {
                  reach = true
                  kPlusReach = K_PLUS_REACH
                }
            }
        }
      },
      resultGroupSpec {
        title = ReportingUserSimulator.CROSS_PUB_GROUP_TITLE
        reportingUnit = reportingUnit { components += dataProviderNames }
        metricFrequency = metricFrequencySpec { total = true }
        dimensionSpec = dimensionSpec {}
        resultGroupMetricSpec = resultGroupMetricSpec {
          populationSize = true
          reportingUnit =
            ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
              cumulative =
                ResultGroupMetricSpecKt.basicMetricSetSpec {
                  reach = true
                  kPlusReach = K_PLUS_REACH
                }
            }
          component =
            ResultGroupMetricSpecKt.componentMetricSetSpec {
              cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
            }
        }
      },
    )
  }

  /**
   * Checks that the report covers every week of the interval for every gender, and that each
   * period's metrics cohere.
   *
   * Non-cumulative reach is only required to be non-negative: the `e7-meta-video` flight ends
   * inside the interval, so a later week legitimately reaches nobody.
   */
  private fun assertWeeklyShape(report: BasicReport) {
    val resultGroup = report.resultGroupsList.single()
    assertThat(resultGroup.title).isEqualTo(WEEKLY_GROUP_TITLE)

    // Every result belongs to exactly one group, so that genderOf identifies it.
    for (result in resultGroup.resultsList) {
      val groupings = result.metadata.dimensionSpecSummary.groupingsList
      assertWithMessage("groupings of ${result.metadata.metricEndTime}")
        .that(groupings.map { it.path })
        .containsExactly(GENDER_FIELD_PATH)
    }

    val genders: Set<String> = resultGroup.resultsList.map { genderOf(it) }.toSet()
    assertThat(genders).hasSize(EXPECTED_GENDER_COUNT)

    for (gender in genders) {
      val periods = resultGroup.resultsList.filter { genderOf(it) == gender }
      assertWithMessage("$gender periods").that(periods).hasSize(WEEKLY_PERIOD_COUNT)

      for (result in periods) {
        val metricSet = result.metricSet.reportingUnit
        val week = result.metadata.metricEndTime.seconds
        assertWithMessage("$gender week $week cumulative reach")
          .that(metricSet.cumulative.reach)
          .isGreaterThan(0L)
        assertWithMessage("$gender week $week non-cumulative reach")
          .that(metricSet.nonCumulative.reach)
          .isAtLeast(0L)
        // The union over every EDP reaches at least as many people as any one of them.
        for ((component, componentMetricSet) in
          result.metricSet.componentsList.associate { it.key to it.value }) {
          assertWithMessage("$gender week $week $component vs union")
            .that(componentMetricSet.nonCumulative.reach)
            .isAtMost(metricSet.nonCumulative.reach)
        }
      }
    }
  }

  /** Checks that cumulative reach never decreases as the interval extends. */
  private fun assertCumulativeReachIsMonotonic(report: BasicReport) {
    val resultGroup = report.resultGroupsList.single()
    for ((gender, results) in resultGroup.resultsList.groupBy { genderOf(it) }) {
      val reachByWeek: List<Long> =
        results
          .sortedBy { it.metadata.metricEndTime.seconds }
          .map { it.metricSet.reportingUnit.cumulative.reach }
      assertWithMessage("$gender cumulative reach by week").that(reachByWeek).isInOrder()
    }
  }

  /**
   * Checks each group's population against the whole synthetic population.
   *
   * Bounded rather than exact: grouping on gender splits the population, so each group reports its
   * own share.
   */
  private fun assertPopulationSizeMatchesSpec(report: BasicReport) {
    for (result in report.resultGroupsList.single().resultsList) {
      val populationSize = result.metricSet.populationSize
      val message = "${genderOf(result)} population size"
      assertWithMessage(message).that(populationSize).isGreaterThan(0L)
      assertWithMessage(message).that(populationSize).isAtMost(expectedPopulationSize)
    }
  }

  private fun genderOf(result: ResultGroup.Result): String =
    result.metadata.dimensionSpecSummary.groupingsList.single().value.enumValue

  /**
   * Checks that every line item carries data and that filtered reach is bounded by unfiltered.
   *
   * `mrc` and the custom video filter each select a subset of what `ami` selects. Equality is
   * permitted: it is the correct answer whenever a filter admits every impression.
   */
  private fun assertReportGroups(report: BasicReport) {
    assertThat(report.resultGroupsList.map { it.title })
      .containsExactly(
        ReportingUserSimulator.SINGLE_EDP_GROUP_TITLE,
        ReportingUserSimulator.CROSS_PUB_GROUP_TITLE,
      )

    for (resultGroup in report.resultGroupsList) {
      assertThat(resultGroup.resultsList).hasSize(EXPECTED_FILTER_COUNT)

      val reachByFilter: Map<String, Long> =
        resultGroup.resultsList.associate { result ->
          filterLabel(result) to reachOf(result.metricSet, resultGroup.title)
        }
      for ((label, reach) in reachByFilter) {
        assertWithMessage("${resultGroup.title}: $label reach").that(reach).isGreaterThan(0L)
      }

      val amiReach = reachByFilter.getValue(ReportingUserSimulator.AMI_FILTER_ID)
      for (label in reachByFilter.keys - ReportingUserSimulator.AMI_FILTER_ID) {
        assertWithMessage("${resultGroup.title}: $label reach vs ami")
          .that(reachByFilter.getValue(label))
          .isAtMost(amiReach)
      }
    }

    // The union over every EDP reaches at least as many people as the single EDP alone.
    assertThat(amiReachOf(report, ReportingUserSimulator.CROSS_PUB_GROUP_TITLE))
      .isAtLeast(amiReachOf(report, ReportingUserSimulator.SINGLE_EDP_GROUP_TITLE))
  }

  /**
   * Checks every metric the report requests against the values the synthetic specs imply.
   *
   * Every metric is compared before anything fails, and the message reports each one with the ratio
   * of measured to expected. A single out-of-range value says little on its own; whether the others
   * are off by the same ratio distinguishes a systematic scaling error from one bad metric.
   */
  private fun assertMetricsMatchSpecs(report: BasicReport) {
    val comparisons = mutableListOf<MetricComparison>()
    for (resultGroup in report.resultGroupsList) {
      val expectedByFilter = expectedMetrics.getValue(resultGroup.title)
      for (result in resultGroup.resultsList) {
        val label = filterLabel(result)
        val expected = expectedByFilter.getValue(label)
        val basicMetricSet = basicMetricSetOf(result.metricSet, resultGroup.title)
        val prefix = "${resultGroup.title}: $label"

        comparisons +=
          MetricComparison("$prefix reach", basicMetricSet.reach.toDouble(), expected.reach)

        assertWithMessage("$prefix k+ reach size")
          .that(basicMetricSet.kPlusReachList)
          .hasSize(expected.kPlusReach.size)
        basicMetricSet.kPlusReachList.forEachIndexed { index, kPlusReach ->
          comparisons +=
            MetricComparison(
              "$prefix ${index + 1}+ reach",
              kPlusReach.toDouble(),
              expected.kPlusReach[index],
            )
          if (index > 0) {
            // K+ reach is the count of VIDs at frequency >= k, so it cannot grow with k.
            assertWithMessage("$prefix ${index + 1}+ reach vs ${index}+")
              .that(kPlusReach)
              .isAtMost(basicMetricSet.kPlusReachList[index - 1])
          }
        }

        assertWithMessage("$prefix population size")
          .that(result.metricSet.populationSize)
          .isEqualTo(expectedPopulationSize)

        // Per-EDP values behind a reporting unit, which the union metrics above aggregate away.
        for (component in result.metricSet.componentsList) {
          logger.info(
            "$prefix component ${component.key}: reach=${component.value.cumulative.reach}"
          )
        }
      }
    }

    logger.info(comparisons.joinToString("\n", prefix = "Metrics measured against specs:\n"))
    val outOfRange = comparisons.filter { !it.inRange }
    assertWithMessage(
        outOfRange.size.toString() +
          " of " +
          comparisons.size +
          " metrics outside the expected range:\n" +
          comparisons.joinToString("\n")
      )
      .that(outOfRange)
      .isEmpty()
  }

  /** One measured metric alongside the range the synthetic specs imply for it. */
  private data class MetricComparison(
    val label: String,
    val actual: Double,
    val expected: ClosedFloatingPointRange<Double>,
  ) {
    val inRange: Boolean
      get() = actual >= expected.start && actual <= expected.endInclusive

    /** Midpoint of the expected range, which is the noiseless value the specs imply. */
    private val expectedValue: Double
      get() = (expected.start + expected.endInclusive) / 2

    override fun toString(): String {
      val ratio = if (expectedValue != 0.0) actual / expectedValue else Double.NaN
      return "%s %s: measured %.1f, expected %.1f (%.1f..%.1f), ratio %.4f"
        .format(
          if (inRange) "  ok" else "FAIL",
          label,
          actual,
          expectedValue,
          expected.start,
          expected.endInclusive,
          ratio,
        )
    }
  }

  private fun amiReachOf(report: BasicReport, groupTitle: String): Long {
    val resultGroup = report.resultGroupsList.single { it.title == groupTitle }
    val result =
      resultGroup.resultsList.single { filterLabel(it) == ReportingUserSimulator.AMI_FILTER_ID }
    return reachOf(result.metricSet, groupTitle)
  }

  /**
   * The cumulative metrics for a result, taken from the reporting unit for the cross-publisher
   * group and from the single component otherwise.
   */
  private fun basicMetricSetOf(
    metricSet: ResultGroup.MetricSet,
    groupTitle: String,
  ): ResultGroup.MetricSet.BasicMetricSet =
    if (groupTitle == ReportingUserSimulator.CROSS_PUB_GROUP_TITLE) {
      metricSet.reportingUnit.cumulative
    } else {
      metricSet.componentsList.single().value.cumulative
    }

  private fun reachOf(metricSet: ResultGroup.MetricSet, groupTitle: String): Long =
    basicMetricSetOf(metricSet, groupTitle).reach

  private fun filterLabel(result: ResultGroup.Result): String {
    val filter = result.metadata.filter
    return if (filter.hasCustom()) {
      ReportingUserSimulator.CUSTOM_VIDEO_FILTER_LABEL
    } else {
      checkNotNull(ImpressionQualificationFilterKey.fromName(filter.impressionQualificationFilter))
        .impressionQualificationFilterId
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val ZONE_ID = ZoneId.of("UTC")

    // Declared before TEST_CONFIG: reading it resolves a runtime path, so these must already be
    // initialized by the time any eager property forces it.
    private val WORKSPACE_PATH: Path = Paths.get("wfa_measurement_system")
    private val SECRET_FILES_PATH: Path = Paths.get("src", "main", "k8s", "testing", "secretfiles")

    private val CONFIG_PATH: Path =
      Paths.get("src", "test", "kotlin", "org", "wfanet", "measurement", "integration", "k8s")
    private const val TEST_CONFIG_NAME = "edpa_reporting_integration_test_config.textproto"

    val TEST_CONFIG: EdpaReportingIntegrationTestConfig by lazy {
      val configFile = runtimePath(CONFIG_PATH.resolve(TEST_CONFIG_NAME)).toFile()
      parseTextProto(configFile, EdpaReportingIntegrationTestConfig.getDefaultInstance())
    }

    /**
     * Resource name of the high overlap ModelLine, or empty where the dataset is not provisioned.
     */
    val MODEL_LINE: String = TEST_CONFIG.modelLine

    /** Resolves a workspace-relative path in the test's runfiles. */
    private fun runtimePath(workspaceRelativePath: Path): Path =
      checkNotNull(
        org.wfanet.measurement.common.getRuntimePath(WORKSPACE_PATH.resolve(workspaceRelativePath))
      ) {
        "Runtime path not found for $workspaceRelativePath"
      }

    private const val MC_TLS_CERT_NAME = "mc_tls.pem"
    private const val MC_TLS_KEY_NAME = "mc_tls.key"
    private const val REPORTING_ROOT_CERT_NAME = "reporting_root.pem"

    private val LOCAL_K8S_PATH: Path = Paths.get("src", "main", "k8s", "local")
    private val OPEN_ID_PROVIDERS_CONFIG_JSON_FILE: File =
      LOCAL_K8S_PATH.resolve("open_id_providers_config.json").toFile()
    private val OPEN_ID_PROVIDERS_TINK_FILE: File =
      Paths.get("src", "main", "k8s", "testing", "secretfiles", "open_id_provider.tink").toFile()

    private fun signingCerts(certName: String, keyName: String, trustedName: String): SigningCerts {
      val secretFiles = runtimePath(SECRET_FILES_PATH)
      return SigningCerts.fromPemFiles(
        secretFiles.resolve(certName).toFile(),
        secretFiles.resolve(keyName).toFile(),
        secretFiles.resolve(trustedName).toFile(),
      )
    }

    private val MEASUREMENT_CONSUMER_CERTS: SigningCerts by lazy {
      signingCerts(MC_TLS_CERT_NAME, MC_TLS_KEY_NAME, "mc_trusted_certs.pem")
    }
    private val REPORTING_CERTS: SigningCerts by lazy {
      signingCerts(MC_TLS_CERT_NAME, MC_TLS_KEY_NAME, REPORTING_ROOT_CERT_NAME)
    }
    private val ACCESS_CERTS: SigningCerts by lazy {
      signingCerts("access_tls.pem", "access_tls.key", REPORTING_ROOT_CERT_NAME)
    }

    private val REPORTING_TOKEN_TTL: Duration = Duration.ofMinutes(60)

    /**
     * Scopes for the Reporting access token.
     *
     * `CreateBasicReport` creates Reports, Metrics and MetricCalculationSpecs on the caller's
     * behalf, so those scopes are required in addition to the ones for the methods called directly.
     */
    private val REPORTING_TOKEN_SCOPES =
      setOf(
        "reporting.basicReports.create",
        "reporting.basicReports.get",
        "reporting.reports.create",
        "reporting.metrics.create",
        "reporting.metricCalculationSpecs.create",
        "reporting.reportingSets.createPrimitive",
        "reporting.reportingSets.createComposite",
        "reporting.eventGroups.list",
      )

    private const val EXPECTED_FILTER_COUNT = 3

    /** EDP the single-EDP result group reports on. */
    private const val SINGLE_EDP_NAME = "edp7"

    /**
     * EventGroups the media-type and IQF report covers.
     *
     * A subset of the dataset, because every (filter, reporting unit) combination is a separate
     * Measurement the EDP Aggregator fulfills by scanning impressions, and the expected values are
     * regenerated from the same specs in process. The two `e7-meta` groups reach the **same** VIDs
     * through both EDPs, so the cross-publisher reach counts them once; `meta-video` adds VIDs edp7
     * never reaches, so that reach is also strictly greater than the single-EDP reach.
     */
    private val REPORT_EVENT_GROUP_REF_IDS =
      setOf(
        "ad_group-high-overlap-e7-meta-edp7",
        "ad_group-high-overlap-e7-meta-edpa_meta",
        "ad_group-high-overlap-meta-video-edpa_meta-1",
      )

    /** Title of the weekly report's only result group. */
    private const val WEEKLY_GROUP_TITLE = "Weekly by gender"

    /** `MALE` and `FEMALE`; the synthetic population declares no other gender. */
    private const val EXPECTED_GENDER_COUNT = 2

    /** Dimension the weekly report groups on. */
    private const val GENDER_FIELD_PATH = "common.gender"

    /**
     * Reporting interval of the weekly report: whole weeks, so no partial period is reported.
     *
     * It straddles 2026-05-15, where the `e7-meta-video` segment's flight ends. Non-cumulative
     * reach therefore falls away over the last weeks while cumulative reach holds, which is what
     * distinguishes the two.
     */
    private val WEEKLY_REPORT_START: LocalDate = LocalDate.of(2026, 4, 15)

    private val WEEKLY_REPORT_END: LocalDate = LocalDate.of(2026, 5, 27)

    private val WEEKLY_METRIC_FREQUENCY: DayOfWeek = DayOfWeek.WEDNESDAY

    private const val WEEKLY_PERIOD_COUNT = 6

    /**
     * EventGroups the weekly report covers, spanning every EDP in [EDP_NAMES].
     *
     * `e7-meta-video` carries one entity key per EDP and runs to 2026-05-15; `e7-video` runs the
     * whole dataset, so edp7 and edpa_video_pub still contribute after that date.
     */
    private val WEEKLY_REPORT_EVENT_GROUP_REF_IDS =
      setOf(
        "campaign-high-overlap-e7-meta-video-edp7",
        "campaign-high-overlap-e7-meta-video-edpa_meta",
        "campaign-high-overlap-e7-meta-video-edpa_video_pub",
        "creative-id-high-overlap-e7-video-edp7-1",
        "creative-id-high-overlap-e7-video-edpa_video_pub-1",
      )

    private val SINGLE_EDP_EVENT_GROUP_REF_IDS = setOf("ad_group-high-overlap-e7-meta-edp7")

    /**
     * Highest frequency to request K+ reach for.
     *
     * Each K+ reach carries the same fixed noise as reach itself, so a frequency deep enough to
     * reach only a handful of people measures noise rather than signal.
     */
    private const val K_PLUS_REACH = 2

    /**
     * Reporting interval, within the date range of every EventGroup above.
     *
     * A VID's impressions are spread across that whole range, so a short interval samples only a
     * fraction of each VID's frequency and leaves the K+ reach metrics under their noise floor.
     * Thirty-six days puts 2+ reach at roughly 18x sigma.
     */
    private val REPORT_START: LocalDate = LocalDate.of(2026, 4, 1)
    private val REPORT_END: LocalDate = LocalDate.of(2026, 5, 7)

    private val POPULATION_SPEC_TYPE_REGISTRY: TypeRegistry =
      TypeRegistry.newBuilder().add(Common.getDescriptor()).build()

    private val IMPRESSION_TEST_DATA_CONFIG: ImpressionTestDataConfig by lazy {
      parseTextProto(
        ImpressionTestDataConfigs.resolveSpecPath(
          "high_overlap_impression_test_data_config.textproto"
        ),
        ImpressionTestDataConfig.getDefaultInstance(),
      )
    }

    private val POPULATION_SPEC: PopulationSpec by lazy {
      parseTextProto(
        ImpressionTestDataConfigs.resolveSpecPath(
          IMPRESSION_TEST_DATA_CONFIG.populationSpecResourcePath
        ),
        PopulationSpec.getDefaultInstance(),
        POPULATION_SPEC_TYPE_REGISTRY,
      )
    }

    /** EDPs this test covers, a subset of the four the high overlap dataset defines. */
    private val EDP_NAMES: Set<String> = setOf("edp7", "edpa_meta", "edpa_video_pub")

    /** Config restricted to [EDP_NAMES], empty when the dataset is not configured. */
    val PROVISIONED_CONFIG: ImpressionTestDataConfig by lazy {
      if (MODEL_LINE.isEmpty()) {
        ImpressionTestDataConfig.getDefaultInstance()
      } else {
        IMPRESSION_TEST_DATA_CONFIG.toBuilder()
          .clearEventGroups()
          .addAllEventGroups(
            IMPRESSION_TEST_DATA_CONFIG.eventGroupsList.filter { it.edpName in EDP_NAMES }
          )
          .build()
      }
    }

    val highOverlapEventGroupMap: Map<String, EventGroupConfig> by lazy {
      ImpressionTestDataConfigs.toEventGroupMap(PROVISIONED_CONFIG)
    }

    /** High overlap event group reference IDs by EDP, keyed `"${entityType}-${entityId}"`. */
    val highOverlapEventGroupRefIdsByEdp: Map<String, Set<String>> by lazy {
      PROVISIONED_CONFIG.eventGroupsList
        .groupBy { it.edpName }
        .mapValues { (_, eventGroups) ->
          eventGroups
            .flatMap { eventGroup ->
              eventGroup.entityKeySpecsList.map { "${it.entityType}-${it.entityId}" }
            }
            .toSet()
        }
    }

    /** Every date covered by the provisioned specs, keyed by the EDP's `output_base_path`. */
    val highOverlapDatesByImpressionPath: Map<String, Set<LocalDate>> by lazy {
      val datesByPath = mutableMapOf<String, MutableSet<LocalDate>>()
      for (eventGroup in PROVISIONED_CONFIG.eventGroupsList) {
        val dates = datesByPath.getOrPut(eventGroup.outputBasePath) { mutableSetOf() }
        for (entityKeySpec in eventGroup.entityKeySpecsList) {
          val spec =
            ImpressionTestDataConfigs.resolveSyntheticEventGroupSpec(
              entityKeySpec.dataSpecResourcePath
            )
          for (dateSpec in spec.dateSpecsList) {
            var date = dateSpec.dateRange.start.toLocalDate()
            val endExclusive = dateSpec.dateRange.endExclusive.toLocalDate()
            while (date.isBefore(endExclusive)) {
              dates.add(date)
              date = date.plusDays(1)
            }
          }
        }
      }
      datesByPath
    }

    /** First date the dataset has events for. Empty unless the dataset is configured. */
    private val highOverlapEarliestEventDate: LocalDate by lazy {
      highOverlapDatesByImpressionPath.values.flatten().min()
    }

    /** Entity types of the reported EventGroups; CMMS defaults `entity_type_in` to `campaign`. */
    private val reportEventGroupEntityTypes: Set<String> by lazy {
      entityTypesOf(REPORT_EVENT_GROUP_REF_IDS)
    }

    private val weeklyReportEventGroupEntityTypes: Set<String> by lazy {
      entityTypesOf(WEEKLY_REPORT_EVENT_GROUP_REF_IDS)
    }

    private fun entityTypesOf(eventGroupReferenceIds: Set<String>): Set<String> =
      PROVISIONED_CONFIG.eventGroupsList
        .flatMap { it.entityKeySpecsList }
        .filter { "${it.entityType}-${it.entityId}" in eventGroupReferenceIds }
        .map { it.entityType }
        .toSet()

    private const val BASIC_REPORT_METRIC_SPEC_CONFIG_NAME =
      "basic_report_metric_spec_config.textproto"

    /** The metric spec config the Reporting server is deployed with. */
    private val BASIC_REPORT_METRIC_SPEC_CONFIG: MetricSpecConfig by lazy {
      val configFile =
        runtimePath(SECRET_FILES_PATH.resolve(BASIC_REPORT_METRIC_SPEC_CONFIG_NAME)).toFile()
      parseTextProto(configFile, MetricSpecConfig.getDefaultInstance())
    }

    private val expectedMetrics: Map<String, Map<String, ExpectedMetrics>> by lazy {
      HighOverlapExpectedReach.computeRangesByGroupAndFilter(
        PROVISIONED_CONFIG,
        REPORT_EVENT_GROUP_REF_IDS,
        SINGLE_EDP_NAME,
        POPULATION_SPEC,
        REPORT_START,
        REPORT_END,
        BASIC_REPORT_METRIC_SPEC_CONFIG,
        K_PLUS_REACH,
      )
    }

    private val expectedPopulationSize: Long by lazy {
      HighOverlapExpectedReach.populationSize(POPULATION_SPEC)
    }

    private val provisionModelResources =
      HighOverlapModelResourcesRule(
        populationSpecProvider = { POPULATION_SPEC },
        populationDataProvider = TEST_CONFIG.populationDataProvider,
        modelLineName = MODEL_LINE,
        earliestEventDateProvider = { highOverlapEarliestEventDate },
        kingdomPublicApiTarget = TEST_CONFIG.kingdomPublicApiTarget,
        kingdomPublicApiCertHost = TEST_CONFIG.kingdomPublicApiCertHost.ifEmpty { null },
      )
    private val uploadEventGroups = UploadEventGroups()
    private val writeImpressions =
      WriteHighOverlapImpressionsRule(
        configProvider = { PROVISIONED_CONFIG },
        populationSpecProvider = { POPULATION_SPEC },
        bucket = TEST_CONFIG.storageBucket,
        modelLineProvider = { MODEL_LINE.ifEmpty { null } },
        edp7KekUri = TEST_CONFIG.edp7KekUri,
        edpaMetaKekUri = TEST_CONFIG.edpaMetaKekUri,
        edpaMetaAwsRoleArn = TEST_CONFIG.edpaMetaAwsRoleArn,
        edpaMetaAwsRegion = TEST_CONFIG.edpaMetaAwsRegion,
      )
    private val createDoneBlobs = CreateDoneBlobs()
    private val reportingSystem = ReportingSystem()

    @ClassRule
    @JvmField
    val chainedRule =
      chainRulesSequentially(
        provisionModelResources,
        uploadEventGroups,
        writeImpressions,
        createDoneBlobs,
        reportingSystem,
      )
  }
}
