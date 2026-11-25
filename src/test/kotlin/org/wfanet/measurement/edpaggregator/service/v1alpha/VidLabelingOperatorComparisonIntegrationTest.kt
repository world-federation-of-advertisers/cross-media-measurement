// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.type.DayOfWeek
import com.google.type.date
import com.google.type.dateTime
import com.google.type.interval
import com.google.type.timeZone
import java.io.File
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneOffset
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.ClassRule
import org.junit.Test
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt.ModelReleasesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.PopulationsGrpcKt.PopulationsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ReplaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createModelLineRequest
import org.wfanet.measurement.api.v2alpha.createModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.createModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.createPopulationRequest
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.common
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.display
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.testEvent
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.getModelLineRequest
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.modelRelease
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.population
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.api.v2alpha.updateEventGroupRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.config.reporting.MetricSpecConfigKt
import org.wfanet.measurement.config.reporting.metricSpecConfig
import org.wfanet.measurement.dataprovider.DataProviderData
import org.wfanet.measurement.duchy.deploy.common.postgres.testing.Schemata.DUCHY_CHANGELOG_PATH
import org.wfanet.measurement.edpaggregator.ModelLineCutover
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.client.LabeledImpressionWriter
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncLeaseRunner
import org.wfanet.measurement.edpaggregator.dataavailability.GrpcDataAvailabilitySyncLeaseClient
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ImpressionDataSourceProvider
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ModelLineDataSource
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ModelLineInfo
import org.wfanet.measurement.edpaggregator.testing.ResultsFulfillerTestHelper
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLeaseServiceGrpcKt.DataAvailabilitySyncLeaseServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpressionKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.labeledImpression
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.IMPRESSION_QUALIFICATION_FILTER_MAPPING
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.integration.common.loadSigningKey
import org.wfanet.measurement.integration.common.reporting.v2.InProcessLifeOfAReportIntegrationTest
import org.wfanet.measurement.integration.deploy.common.postgres.PostgresDuchyDependencyProviderRule
import org.wfanet.measurement.integration.deploy.gcloud.InternalReportingServicesProviderRule
import org.wfanet.measurement.integration.deploy.gcloud.KingdomDataServicesProviderRule
import org.wfanet.measurement.integration.deploy.gcloud.SpannerAccessServicesFactory
import org.wfanet.measurement.internal.reporting.v2.getBasicReportRequest as internalGetBasicReportRequest
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata.REPORTING_CHANGELOG_PATH
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.dimensionSpec
import org.wfanet.measurement.reporting.v2alpha.getMetricCalculationSpecRequest
import org.wfanet.measurement.reporting.v2alpha.getMetricRequest
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.reportingInterval
import org.wfanet.measurement.reporting.v2alpha.reportingUnit
import org.wfanet.measurement.reporting.v2alpha.resultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.resultGroupSpec

/** Operator comparison through the VID pipeline and public Reporting APIs before cutover. */
class VidLabelingOperatorComparisonIntegrationTest :
  InProcessLifeOfAReportIntegrationTest(
    KingdomDataServicesProviderRule(spannerEmulator),
    PostgresDuchyDependencyProviderRule(duchyDatabaseProvider, ALL_DUCHY_NAMES),
    SpannerAccessServicesFactory(spannerEmulator),
    InternalReportingServicesProviderRule(
      spannerEmulator,
      reportingDatabaseProvider,
      IMPRESSION_QUALIFICATION_FILTER_MAPPING,
    ),
    hmssEnabled = false,
    trusTeeEnabled = false,
    metricSpecConfigOverride = COMPARISON_METRIC_SPEC_CONFIG,
  ) {

  @Test
  fun `operator compares native PROD and DEV pipeline sources before future cutover`() =
    runBlocking {
      inProcessCmmsComponents.stopEdpSimulators()
      val eventGroup = listEventGroups().first()
      val dataProvider =
        publicDataProvidersClient
          .withCallCredentials(credentials)
          .getDataProvider(getDataProviderRequest { name = eventGroup.cmmsDataProvider })
      val displayName =
        checkNotNull(
          inProcessCmmsComponents.getDataProviderDisplayNameFromDataProviderName(dataProvider.name)
        )
      val providerData =
        DataProviderData(
          name = dataProvider.name,
          privateEncryptionKey = loadEncryptionPrivateKey("${displayName}_enc_private.tink"),
          signingKeyHandle =
            loadSigningKey("${displayName}_cs_cert.der", "${displayName}_cs_private.der"),
          certificateKey =
            checkNotNull(DataProviderCertificateKey.fromName(dataProvider.certificate)),
        )
      val modelLines =
        ModelLinesCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
          .withPrincipalName(inProcessCmmsComponents.modelProviderResourceName)
      val existingLine =
        modelLines.getModelLine(
          getModelLineRequest { name = inProcessCmmsComponents.modelLineResourceName }
        )
      val modelSuite = checkNotNull(ModelLineKey.fromName(existingLine.name)).parentKey.toName()
      suspend fun createLine(type: ModelLine.Type): ModelLine =
        modelLines.createModelLine(
          createModelLineRequest {
            parent = modelSuite
            modelLine = modelLine {
              this.type = type
              eventTemplateVersion = 1
              activeStartTime =
                FIRST_DATE.minusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
            }
          }
        )
      val legacy = createLine(ModelLine.Type.PROD)
      val replacement = createLine(ModelLine.Type.PROD)
      val dev = createLine(ModelLine.Type.DEV)
      val populationProvider =
        inProcessCmmsComponents.getPopulationData().populationDataProviderName
      val comparisonPopulation =
        PopulationsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
          .withPrincipalName(populationProvider)
          .createPopulation(
            createPopulationRequest {
              parent = populationProvider
              population = population { populationSpec = COMPARISON_POPULATION_SPEC }
            }
          )
      val comparisonRelease =
        ModelReleasesCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
          .withPrincipalName(inProcessCmmsComponents.modelProviderResourceName)
          .createModelRelease(
            createModelReleaseRequest {
              parent = modelSuite
              modelRelease = modelRelease { population = comparisonPopulation.name }
            }
          )
      for (line in listOf(legacy, replacement, dev)) {
        ModelRolloutsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
          .withPrincipalName(inProcessCmmsComponents.modelProviderResourceName)
          .createModelRollout(
            createModelRolloutRequest {
              parent = line.name
              modelRollout = modelRollout {
                modelRelease = comparisonRelease.name
                instantRolloutDate = date {
                  year = FIRST_DATE.year
                  month = FIRST_DATE.monthValue
                  day = FIRST_DATE.minusDays(1).dayOfMonth
                }
              }
            }
          )
      }

      val cutover =
        ModelLineCutover(
          legacy.name,
          legacy.name,
          replacement.name,
          FIRST_DATE.plusYears(10),
          FIRST_DATE.plusYears(10).plusDays(2),
          Duration.ofDays(2),
        )
      val pipeline =
        ComparisonPipeline(
          dataProvider.name,
          legacy.name,
          replacement.name,
          dev.name,
          cutover,
          comparisonRelease.name,
        ) { request ->
          DataProvidersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
            .withPrincipalName(dataProvider.name)
            .replaceDataAvailabilityIntervals(request)
        }
      pipeline.run {
        // The existing public EventGroup supplies the selector for both physical sources.
        val cmmsEventGroup =
          EventGroupsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
            .withPrincipalName(dataProvider.name)
            .getEventGroup(getEventGroupRequest { name = eventGroup.cmmsEventGroup })
        EventGroupsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
          .withPrincipalName(dataProvider.name)
          .updateEventGroup(
            updateEventGroupRequest {
              this.eventGroup =
                cmmsEventGroup.copy {
                  entityKey =
                    EventGroupKt.entityKey {
                      entityType = "person"
                      entityId = "comparison-campaign"
                    }
                }
            }
          )
        pipeline.exerciseAvailability {
          val current =
            DataProvidersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
              .withPrincipalName(dataProvider.name)
              .getDataProvider(getDataProviderRequest { name = dataProvider.name })
              .dataAvailabilityIntervalsList
              .associate { it.key to it.value }
          assertThat(current.keys).containsExactly(legacy.name, dev.name)
          assertThat(current.getValue(dev.name).endTime.toInstant())
            .isEqualTo(FIRST_DATE.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant())
          assertThat(current.getValue(legacy.name).endTime.toInstant())
            .isEqualTo(FIRST_DATE.plusDays(3).atStartOfDay(ZoneOffset.UTC).toInstant())
        }
        val availability =
          DataProvidersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
            .withPrincipalName(dataProvider.name)
            .getDataProvider(getDataProviderRequest { name = dataProvider.name })
        assertThat(availability.dataAvailabilityIntervalsList.map { it.key })
          .containsExactly(legacy.name, dev.name)

        val template =
          buildCreateBasicReportRequest(listOf(eventGroup)).basicReport.copy {
            reportingInterval = reportingInterval {
              reportStart = dateTime {
                year = FIRST_DATE.year
                month = FIRST_DATE.monthValue
                day = FIRST_DATE.dayOfMonth
                timeZone = timeZone { id = "UTC" }
              }
              reportEnd = date {
                year = FIRST_DATE.year
                month = FIRST_DATE.monthValue
                day = FIRST_DATE.plusDays(2).dayOfMonth
              }
            }
            resultGroupSpecs += resultGroupSpec {
              title = "comparison"
              reportingUnit = reportingUnit { components += dataProvider.name }
              metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
              dimensionSpec = dimensionSpec {}
              resultGroupMetricSpec = resultGroupMetricSpec {
                reportingUnit =
                  ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                    nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                  }
              }
            }
          }
        suspend fun createReport(id: String, line: String): BasicReport =
          publicBasicReportsClient
            .withCallCredentials(credentials)
            .createBasicReport(
              createBasicReportRequest {
                parent = inProcessCmmsComponents.getMeasurementConsumerData().name
                basicReportId = id
                basicReport = template.copy { modelLine = line }
              }
            )
        val prod = createReport("operator-prod", legacy.name)
        val comparison = createReport("operator-dev", dev.name)
        assertThat(prod.effectiveModelLine).isEqualTo(legacy.name)
        assertThat(comparison.effectiveModelLine).isEqualTo(dev.name)
        assertThat(prod.reportingInterval).isEqualTo(comparison.reportingInterval)
        assertThat(prod.resultGroupSpecsList).isEqualTo(comparison.resultGroupSpecsList)

        val measurements = listMeasurements()
        val byLine = measurements.groupBy { it.measurementSpec.unpack<MeasurementSpec>().modelLine }
        assertThat(byLine.keys).containsExactly(legacy.name, dev.name)
        pipeline.fulfillReports(
          providerData,
          inProcessCmmsComponents.kingdom.publicApiChannel,
          legacy.name,
          replacement.name,
          dev.name,
          cutover,
        )
        assertThat(pipeline.selectedSources).containsExactly(legacy.name, replacement.name)
        assertThat(pipeline.nativeReadSources).containsExactly(legacy.name, replacement.name)
        assertThat(pipeline.fulfilledModelLines).containsExactly(legacy.name, dev.name)
        val prodResources = computationResources(prod)
        val devResources = computationResources(comparison)
        assertThat(prodResources.specs).isNotEmpty()
        assertThat(devResources.specs).isNotEmpty()
        assertThat(prodResources.specs.intersect(devResources.specs)).isEmpty()
        assertThat(prodResources.metrics).isNotEmpty()
        assertThat(devResources.metrics).isNotEmpty()
        assertThat(prodResources.metrics.intersect(devResources.metrics)).isEmpty()
        assertThat(prodResources.measurements).isNotEmpty()
        assertThat(devResources.measurements).isNotEmpty()
        assertThat(prodResources.measurements.intersect(devResources.measurements)).isEmpty()
        for ((resources, line) in listOf(prodResources to legacy.name, devResources to dev.name)) {
          for (name in resources.specs) {
            val spec =
              publicMetricCalculationSpecsClient
                .withCallCredentials(credentials)
                .getMetricCalculationSpec(getMetricCalculationSpecRequest { this.name = name })
            assertThat(spec.modelLine).isEqualTo(line)
          }
          for (name in resources.measurements) {
            val measurement =
              publicMeasurementsClient
                .withAuthenticationKey(
                  inProcessCmmsComponents.getMeasurementConsumerData().apiAuthenticationKey
                )
                .getMeasurement(getMeasurementRequest { this.name = name })
            assertThat(measurement.measurementSpec.unpack<MeasurementSpec>().modelLine)
              .isEqualTo(line)
          }
          for (name in resources.metrics) {
            val metric =
              publicMetricsClient
                .withCallCredentials(credentials)
                .getMetric(getMetricRequest { this.name = name })
            assertThat(metric.modelLine).isEqualTo(line)
          }
        }
      }
    }

  private data class ComputationResources(
    val specs: Set<String>,
    val metrics: Set<String>,
    val measurements: Set<String>,
  )

  private suspend fun computationResources(basicReport: BasicReport): ComputationResources {
    val key = checkNotNull(BasicReportKey.fromName(basicReport.name))
    val internal =
      reportingServer.internalBasicReportsClient.getBasicReport(
        internalGetBasicReportRequest {
          cmmsMeasurementConsumerId = key.cmmsMeasurementConsumerId
          externalBasicReportId = key.basicReportId
        }
      )
    val reportName =
      ReportKey(internal.cmmsMeasurementConsumerId, internal.externalReportId).toName()
    val report = withTimeout(60_000L) { pollForCompletedReport(reportName) }
    assertThat(report.state).isEqualTo(Report.State.SUCCEEDED)
    val metricNames =
      report.metricCalculationResultsList
        .flatMap { it.resultAttributesList }
        .map { it.metric }
        .toSet()
    val measurementNames = mutableSetOf<String>()
    for (name in metricNames) {
      val metric =
        publicMetricsClient
          .withCallCredentials(credentials)
          .getMetric(getMetricRequest { this.name = name })
      measurementNames += metric.result.cmmsMeasurementsList
    }
    return ComputationResources(
      report.reportingMetricEntriesList.flatMap { it.value.metricCalculationSpecsList }.toSet(),
      metricNames,
      measurementNames,
    )
  }

  private class ComparisonPipeline(
    dataProviderName: String,
    private val legacy: String,
    private val replacement: String,
    private val dev: String,
    private val cutover: ModelLineCutover,
    modelRelease: String,
    publishAvailability: suspend (ReplaceDataAvailabilityIntervalsRequest) -> Unit,
  ) :
    VidLabelingPipelineTestHarness(
      PipelineHarnessConfig(
        dataProviderName = dataProviderName,
        eventDescriptor = TestEvent.getDescriptor(),
        populationSpec = COMPARISON_POPULATION_SPEC,
        eventTemplateFieldMapping = mapOf("display.viewable_fraction" to "viewable_fraction"),
        fixedDoubleColumns = mapOf("viewable_fraction" to 1.0),
        includeDefaultModelLines = false,
        externalModelLines = setOf(legacy),
        publishAvailability = publishAvailability,
        extraModelLines =
          listOf(
            ModelLineFixture(
              replacement,
              modelRelease,
              true,
              FIRST_DATE.minusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant(),
            )
          ),
        initiallyVisibleModelLines = setOf(replacement),
        initialToday = FIRST_DATE,
        dispatcherClockUsesToday = true,
        modelLineCutovers = listOf(cutover),
        modelLineMap = mapOf(replacement to listOf(dev)),
      )
    ) {
    val selectedSources = mutableSetOf<String>()
    val nativeReadSources = mutableSetOf<String>()
    val fulfilledModelLines = mutableSetOf<String>()
    val lastPublication: ReplaceDataAvailabilityIntervalsRequest
      get() = dataProvidersService.requests.last()

    private val readServer = GrpcTestServerRule {
      addService(
        object : ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase() {
          override suspend fun listImpressionMetadata(
            request: ListImpressionMetadataRequest
          ): ListImpressionMetadataResponse {
            nativeReadSources += request.filter.modelLine
            return impressionMetadataStub.listImpressionMetadata(request)
          }
        }
      )
    }

    fun run(block: suspend () -> Unit) {
      chainRulesSequentially(ruleChain, readServer)
        .apply(
          object : Statement() {
            override fun evaluate() {
              setUp()
              try {
                runBlocking { block() }
              } finally {
                tearDown()
              }
            }
          },
          Description.createTestDescription(javaClass, "operator comparison"),
        )
        .evaluate()
    }

    suspend fun exerciseAvailability(onGap: suspend () -> Unit) {
      for (day in 0..2) writeLegacy(FIRST_DATE.plusDays(day.toLong()))
      uploadReplacement(0)
      val third = uploadReplacement(2)
      val entries = lastPublication.dataAvailabilityIntervalsList.associate { it.key to it.value }
      assertThat(entries.keys).containsExactly(legacy, dev)
      assertThat(entries.getValue(legacy).endTime.toInstant())
        .isEqualTo(FIRST_DATE.plusDays(3).atStartOfDay(ZoneOffset.UTC).toInstant())
      assertThat(entries.getValue(dev).endTime.toInstant())
        .isEqualTo(FIRST_DATE.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant())
      onGap()
      val artifacts = snapshotOutputArtifacts(listMetadata())
      uploadReplacement(1)
      repeat(3) {
        buildVidLabelingMonitor().runDispatch()
        awaitPipelineIdle(allowFailures = true)
      }
      assertThat(listModelLines(third.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(
          lastPublication.dataAvailabilityIntervalsList
            .associate { it.key to it.value }
            .getValue(dev)
            .endTime
            .toInstant()
        )
        .isEqualTo(FIRST_DATE.plusDays(3).atStartOfDay(ZoneOffset.UTC).toInstant())
      assertThat(snapshotOutputArtifacts(listMetadata()).filterKeys { it in artifacts })
        .isEqualTo(artifacts)
      assertThat(listMetadata().map { it.modelLine }.toSet()).containsExactly(legacy, replacement)
      assertThat(listUploads().flatMap { listModelLines(it.name) }.map { it.cmmsModelLine }.toSet())
        .containsExactly(replacement)
    }

    private suspend fun writeLegacy(date: LocalDate) {
      val folder = "$externalOutputPrefix/model-line/${legacy.substringAfterLast('/')}/$date"
      val blobUri = "$folder/impressions.enc.recordio"
      val sidecar = "$folder/metadata.binpb"
      val impressions =
        (0 until 2).map { index ->
          labeledImpression {
            vid = 10_000L + index
            eventTime = date.atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
            event = Any.pack(testEvent { display = display { viewableFraction = 1.0f } })
            entityKeys +=
              LabeledImpressionKt.entityKey {
                entityType = "person"
                entityId = "comparison-campaign"
              }
          }
        }
      LabeledImpressionWriter.forFileSystem(kmsClient, File("/"))
        .write(
          flowOf(*impressions.toTypedArray()),
          blobUri.replace("gs://", "file:///"),
          sidecar.replace("gs://", "file:///"),
          kekUri,
          legacy,
          interval {
            startTime = date.atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
            endTime = date.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
          },
        )
      val doneUri = (folder + "/done").replace("gs://", "file:///")
      val done = fileStorage.writeBlob(blobKey(doneUri), flowOf(ByteString.EMPTY))
      val generation = checkNotNull(fileStorage.getFreshnessToken(done.blobKey)).toLong()
      DataAvailabilitySyncLeaseRunner(
          GrpcDataAvailabilitySyncLeaseClient(
            DataAvailabilitySyncLeaseServiceCoroutineStub(edpaPublicServer.channel)
          ),
          renewalDelay = { delay(it) },
        )
        .run(providerName) { lease ->
          externalDataAvailabilitySync.sync(
            doneUri,
            lease.name,
            generation,
            externalDoneBlobCreateTime = done.createTime,
            ensureLeaseActive = lease::invoke,
          )
        }
    }

    private suspend fun uploadReplacement(day: Int): RawImpressionUpload {
      val date = FIRST_DATE.plusDays(day.toLong())
      if (date.isAfter(today.get())) setToday(date)
      val folder = "operator-replacement-$day"
      writeRawFile(folder, "input.parquet", listOf("comparison-campaign"), date)
      val generation = finalizeRawUpload(folder)
      awaitPipelineIdle(allowFailures = true)
      val upload =
        listUploads().single {
          it.doneBlobUri.contains(folder) && it.doneBlobGeneration == generation
        }
      // The generated sidecars belong to the replacement's physical source.
      for (row in listMetadata().filter { it.rawImpressionUpload == upload.name }) {
        val key = blobKey(row.blobUri)
        val details = BlobDetails.parseFrom(checkNotNull(fileStorage.getBlob(key)).read().flatten())
        assertThat(details.modelLine).isEqualTo(replacement)
      }
      return upload
    }

    suspend fun fulfillReports(
      provider: DataProviderData,
      kingdomChannel: io.grpc.Channel,
      l1: String,
      r1: String,
      d1: String,
      cutover: ModelLineCutover,
    ) {
      val modelInfo =
        ModelLineInfo(
          comparisonPopulationSpec,
          TestEvent.getDescriptor(),
          InMemoryVidIndexMap.build(comparisonPopulationSpec),
          localAlias = null,
        )
      val groupCount =
        ResultsFulfillerTestHelper.fulfillTestRequisitions(
          provider,
          kingdomChannel,
          RequisitionMetadataServiceCoroutineStub(edpaPublicServer.channel),
          ImpressionDataSourceProvider(
            ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub(
              readServer.channel
            ),
            providerName,
            StorageConfig(rootDirectory = File("/")),
            modelLineDataSources =
              mapOf(
                l1 to ModelLineDataSource(externalOutputPrefix, null, null),
                r1 to ModelLineDataSource(outputPrefix, null, null),
              ),
            modelLineStorageClientFactory = { metadataStorage },
          ),
          mapOf(l1 to modelInfo, r1 to modelInfo, d1 to modelInfo.copy(localAlias = r1)),
          kmsClient,
          tempFolder.newFolder("comparison-requisitions"),
          java.time.Clock.systemUTC(),
          cutovers = listOf(cutover),
          aliases = mapOf(d1 to r1),
          onSelectedSource = { selectedSources += it },
        ) { measurementSpec, _, frequencyVector ->
          assertThat(frequencyVector.getTotalUncappedImpressions())
            .isEqualTo(if (measurementSpec.modelLine == l1) 4L else 2L)
          fulfilledModelLines += measurementSpec.modelLine
        }
      assertThat(groupCount).isEqualTo(2)
    }
  }

  companion object {
    private val COMPARISON_POPULATION_SPEC = populationSpec {
      val demographics =
        listOf(
          Common.Gender.MALE to Common.AgeGroup.YEARS_18_TO_34,
          Common.Gender.MALE to Common.AgeGroup.YEARS_35_TO_54,
          Common.Gender.MALE to Common.AgeGroup.YEARS_55_PLUS,
          Common.Gender.FEMALE to Common.AgeGroup.YEARS_18_TO_34,
          Common.Gender.FEMALE to Common.AgeGroup.YEARS_35_TO_54,
          Common.Gender.FEMALE to Common.AgeGroup.YEARS_55_PLUS,
        )
      for ((index, demographic) in demographics.withIndex()) {
        subpopulations +=
          PopulationSpecKt.subPopulation {
            attributes +=
              Any.pack(
                common {
                  gender = demographic.first
                  ageGroup = demographic.second
                  usState = Common.UsState.CALIFORNIA
                }
              )
            vidRanges +=
              PopulationSpecKt.vidRange {
                startVid = 10_000L + index * 100L
                endVidInclusive = startVid + 99L
              }
          }
      }
    }
    private val PRIVACY_PARAMS =
      MetricSpecConfigKt.differentialPrivacyParams {
        epsilon = 1.0
        delta = 1e-15
      }
    private val COMPARISON_METRIC_SPEC_CONFIG = metricSpecConfig {
      reachParams =
        MetricSpecConfigKt.reachParams {
          multipleDataProviderParams =
            MetricSpecConfigKt.samplingAndPrivacyParams {
              privacyParams = PRIVACY_PARAMS
              vidSamplingInterval =
                MetricSpecConfigKt.vidSamplingInterval {
                  fixedStart =
                    MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                      start = 0.0f
                      width = 1.0f
                    }
                }
            }
          singleDataProviderParams = multipleDataProviderParams
        }

      reachAndFrequencyParams =
        MetricSpecConfigKt.reachAndFrequencyParams {
          multipleDataProviderParams =
            MetricSpecConfigKt.reachAndFrequencySamplingAndPrivacyParams {
              reachPrivacyParams = PRIVACY_PARAMS
              frequencyPrivacyParams = PRIVACY_PARAMS
              vidSamplingInterval =
                MetricSpecConfigKt.vidSamplingInterval {
                  fixedStart =
                    MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                      start = 0.0f
                      width = 1.0f
                    }
                }
            }
          singleDataProviderParams = multipleDataProviderParams
          maximumFrequency = 10
        }

      impressionCountParams =
        MetricSpecConfigKt.impressionCountParams {
          params =
            MetricSpecConfigKt.samplingAndPrivacyParams {
              privacyParams = PRIVACY_PARAMS
              vidSamplingInterval =
                MetricSpecConfigKt.vidSamplingInterval {
                  fixedStart =
                    MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                      start = 0.0f
                      width = 1.0f
                    }
                }
            }
          maximumFrequencyPerUser = 60
        }

      watchDurationParams =
        MetricSpecConfigKt.watchDurationParams {
          params =
            MetricSpecConfigKt.samplingAndPrivacyParams {
              privacyParams = PRIVACY_PARAMS
              vidSamplingInterval =
                MetricSpecConfigKt.vidSamplingInterval {
                  fixedStart =
                    MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                      start = 0.0f
                      width = 1.0f
                    }
                }
            }
          maximumWatchDurationPerUser = com.google.protobuf.util.Durations.fromSeconds(4000)
        }

      populationCountParams =
        org.wfanet.measurement.config.reporting.MetricSpecConfig.PopulationCountParams
          .getDefaultInstance()
    }

    private val FIRST_DATE = LocalDate.of(2021, 3, 15)
    @get:ClassRule @JvmStatic val spannerEmulator = VidLabelingPipelineTestHarness.spannerEmulator
    @get:ClassRule
    @JvmStatic
    val reportingDatabaseProvider = PostgresDatabaseProviderRule(REPORTING_CHANGELOG_PATH)
    @get:ClassRule
    @JvmStatic
    val duchyDatabaseProvider = PostgresDatabaseProviderRule(DUCHY_CHANGELOG_PATH)
  }
}
