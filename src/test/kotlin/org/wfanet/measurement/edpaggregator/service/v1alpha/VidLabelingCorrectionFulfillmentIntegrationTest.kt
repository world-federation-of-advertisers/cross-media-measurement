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
import com.google.type.timeZone
import java.io.File
import java.time.LocalDate
import java.time.ZoneOffset
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
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt.ModelReleasesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.PopulationsGrpcKt.PopulationsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ReplaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.createModelLineRequest
import org.wfanet.measurement.api.v2alpha.createModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.createModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.createPopulationRequest
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.common
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.api.v2alpha.getModelLineRequest
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.modelRelease
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.population
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.config.reporting.MetricSpecConfigKt
import org.wfanet.measurement.config.reporting.metricSpecConfig
import org.wfanet.measurement.dataprovider.DataProviderData
import org.wfanet.measurement.duchy.deploy.common.postgres.testing.Schemata.DUCHY_CHANGELOG_PATH
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySync
import org.wfanet.measurement.edpaggregator.rawimpressions.EventIdDigestExtractor
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ImpressionDataSourceProvider
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ModelLineDataSource
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ModelLineInfo
import org.wfanet.measurement.edpaggregator.testing.ResultsFulfillerTestHelper
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.vidlabeling.DataAvailabilitySyncWorkItems
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
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata.REPORTING_CHANGELOG_PATH
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.dimensionSpec
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.reportingInterval
import org.wfanet.measurement.reporting.v2alpha.reportingSet
import org.wfanet.measurement.reporting.v2alpha.reportingUnit
import org.wfanet.measurement.reporting.v2alpha.resultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.resultGroupSpec
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher

class VidLabelingCorrectionFulfillmentIntegrationTest :
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

  private enum class Scenario {
    NORMAL,
    DUPLICATE,
    RESTART,
    EVICTION_RACE,
  }

  @Test
  fun `correction fulfills edited retained and added impressions exactly once`() =
    runScenario(Scenario.NORMAL)

  @Test
  fun `duplicate correction and availability deliveries preserve fulfillment`() =
    runScenario(Scenario.DUPLICATE)

  @Test
  fun `restart at the correction availability handoff preserves fulfillment`() =
    runScenario(Scenario.RESTART)

  @Test
  fun `availability racing eviction cannot restore removed impressions in fulfillment`() =
    runScenario(Scenario.EVICTION_RACE)

  private fun runScenario(scenario: Scenario) = runBlocking {
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
        certificateKey = checkNotNull(DataProviderCertificateKey.fromName(dataProvider.certificate)),
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
    val populationProvider = inProcessCmmsComponents.getPopulationData().populationDataProviderName
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
    val directRelease =
      ModelReleasesCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
        .withPrincipalName(inProcessCmmsComponents.modelProviderResourceName)
        .createModelRelease(
          createModelReleaseRequest {
            parent = modelSuite
            modelRelease = modelRelease { population = comparisonPopulation.name }
          }
        )
    for (line in listOf(legacy, replacement)) {
      ModelRolloutsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
        .withPrincipalName(inProcessCmmsComponents.modelProviderResourceName)
        .createModelRollout(
          createModelRolloutRequest {
            parent = line.name
            modelRollout = modelRollout {
              modelRelease =
                if (line.name == legacy.name) directRelease.name else comparisonRelease.name
              instantRolloutDate = date {
                year = FIRST_DATE.year
                month = FIRST_DATE.monthValue
                day = FIRST_DATE.minusDays(1).dayOfMonth
              }
            }
          }
        )
    }

    val pipeline =
      CorrectionPipeline(
        dataProvider.name,
        replacement.name,
        legacy.name,
        comparisonRelease.name,
        directRelease.name,
      ) { request ->
        DataProvidersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
          .withPrincipalName(dataProvider.name)
          .replaceDataAvailabilityIntervals(request)
      }
    pipeline.run {
      val cmmsEventGroups =
        EventGroupsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
          .withPrincipalName(dataProvider.name)
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
              day = FIRST_DATE.plusDays(1).dayOfMonth
            }
          }
          resultGroupSpecs += resultGroupSpec {
            title = "correction"
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
      var reportIndex = 0
      val selectorGroups = mutableMapOf<String, String>()
      suspend fun fulfillSelection(line: String, people: Set<String>, stage: String): Long {
        val currentGroup =
          cmmsEventGroups.getEventGroup(getEventGroupRequest { name = eventGroup.cmmsEventGroup })
        // Reuse immutable person groups across queries without reusing reporting sets.
        val selectedGroups =
          people.sorted().map { person ->
            selectorGroups[person]
              ?: cmmsEventGroups
                .createEventGroup(
                  createEventGroupRequest {
                    parent = dataProvider.name
                    this.eventGroup =
                      currentGroup.copy {
                        clearName()
                        clearEventGroupReferenceId()
                        entityKey =
                          EventGroupKt.entityKey {
                            entityType = "person"
                            entityId = person
                          }
                      }
                  }
                )
                .name
                .also { selectorGroups[person] = it }
          }
        val queryId = "$stage-${scenario.name.lowercase().replace('_', '-')}-${reportIndex++}"
        val consumer = inProcessCmmsComponents.getMeasurementConsumerData().name
        val querySet =
          publicReportingSetsClient
            .withCallCredentials(credentials)
            .createReportingSet(
              createReportingSetRequest {
                parent = consumer
                reportingSetId = queryId
                reportingSet = reportingSet {
                  campaignGroup = "$consumer/reportingSets/$queryId"
                  primitive = ReportingSetKt.primitive { this.cmmsEventGroups += selectedGroups }
                }
              }
            )
        publicBasicReportsClient
          .withCallCredentials(credentials)
          .createBasicReport(
            createBasicReportRequest {
              parent = consumer
              basicReportId = queryId
              basicReport =
                template.copy {
                  modelLine = line
                  campaignGroup = querySet.name
                }
            }
          )
        return pipeline.fulfill(
          providerData,
          inProcessCmmsComponents.kingdom.publicApiChannel,
          line,
          people,
          if (stage == "intermediate") INTERMEDIATE_PEOPLE else EXPECTED_PEOPLE,
        )
      }
      pipeline.correct(scenario) {
        for (line in listOf(replacement.name, legacy.name)) {
          for (person in INTERMEDIATE_PEOPLE) {
            assertThat(fulfillSelection(line, setOf(person), "intermediate")).isEqualTo(1L)
          }
          assertThat(fulfillSelection(line, INTERMEDIATE_PEOPLE, "intermediate")).isEqualTo(3L)
        }
      }
      for (line in listOf(replacement.name, legacy.name)) {
        var total = 0L
        for (person in listOf("A-prime", "C", "D", "A", "B")) {
          total += fulfillSelection(line, setOf(person), "correction")
        }
        assertThat(total).isEqualTo(3L)
        pipeline.assertCombinedMembership(line)
        assertThat(fulfillSelection(line, EXPECTED_PEOPLE, "aggregate")).isEqualTo(3L)
      }
    }
  }

  private class CorrectionPipeline(
    dataProvider: String,
    private val memoized: String,
    private val direct: String,
    release: String,
    directRelease: String,
    publishAvailability: suspend (ReplaceDataAvailabilityIntervalsRequest) -> Unit,
  ) :
    VidLabelingPipelineTestHarness(
      PipelineHarnessConfig(
        dataProviderName = dataProvider,
        eventDescriptor = TestEvent.getDescriptor(),
        populationSpec = COMPARISON_POPULATION_SPEC,
        eventTemplateFieldMapping = mapOf("display.viewable_fraction" to "viewable_fraction"),
        fixedDoubleColumns = mapOf("viewable_fraction" to 1.0),
        includeDefaultModelLines = false,
        extraModelLines =
          listOf(
            ModelLineFixture(
              memoized,
              release,
              true,
              FIRST_DATE.minusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant(),
            ),
            ModelLineFixture(
              direct,
              directRelease,
              false,
              FIRST_DATE.minusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant(),
            ),
          ),
        publishAvailability = publishAvailability,
        initialToday = FIRST_DATE.plusDays(2),
        dispatcherClockUsesToday = true,
        redeliveryDelayMillis = 0L,
        numberOfShards = PHASE0_SHARDS,
      )
    ) {
    fun run(block: suspend () -> Unit) {
      ruleChain
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
          Description.createTestDescription(javaClass, "correction fulfillment"),
        )
        .evaluate()
    }

    private val selectorVectors = mutableMapOf<String, MutableMap<String, Map<Int, Int>>>()

    private fun expectedVector(line: String): Map<Int, Int> {
      val indexMap = InMemoryVidIndexMap.build(comparisonPopulationSpec)
      val vids = if (line == memoized) MEMOIZED_VIDS.toList() else DIRECT_VIDS.values.toList()
      return vids.groupingBy { indexMap[it] }.eachCount()
    }

    fun assertCombinedMembership(line: String) {
      val results = checkNotNull(selectorVectors[line])
      assertThat(results.keys).containsExactly("A-prime", "C", "D", "A", "B")
      val combined = mutableMapOf<Int, Int>()
      for (vector in results.values) for ((index, count) in vector) {
        combined[index] = combined.getOrDefault(index, 0) + count
      }
      assertThat(combined).containsExactlyEntriesIn(expectedVector(line))
    }

    private val dayEndMicros: Long
      get() =
        FIRST_DATE.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() * 1000 - 1

    private fun freshController() =
      buildHealingController(
        clock =
          java.time.Clock.fixed(
            FIRST_DATE.plusDays(2).atStartOfDay(ZoneOffset.UTC).toInstant(),
            ZoneOffset.UTC,
          )
      )

    private suspend fun deliverAvailability(upload: String, allowFailures: Boolean = false) {
      for (task in listAvailabilityTasks(upload).filter { it.state == WorkItem.State.QUEUED }) {
        workItemTransport.republishWorkItem(task.name)
      }
      awaitPipelineIdle(allowFailures)
    }

    private suspend fun assertHandoffTaskIdentity(
      item: WorkItem,
      operation: UploadHealingOperation,
    ): String {
      assertThat(item.generation).isEqualTo(1L)
      assertThat(item.state).isEqualTo(WorkItem.State.QUEUED)
      val replacements =
        operation.stepsList
          .map { it.replacementRawImpressionUpload }
          .filter { it.isNotEmpty() }
          .toSet()
      val record =
        replacements.flatMap { listAvailabilityTasks(it) }.single { it.name == item.name }
      assertThat(record.doneBlobGeneration).isGreaterThan(0L)
      assertThat(setOf(memoized, direct)).contains(record.cmmsModelLine)
      val upload = listUploads().single { it.name == record.rawImpressionUpload }
      assertAvailabilityTaskIdentities(upload, FIRST_DATE)
      val modelLine =
        listModelLines(upload.name).single { it.cmmsModelLine == record.cmmsModelLine }
      val done = checkNotNull(fileStorage.getBlob(blobKey(record.doneBlobUri)))
      val request =
        DataAvailabilitySyncWorkItems.createRequest(
          providerName,
          upload.name,
          modelLine.name,
          record.cmmsModelLine,
          FIRST_DATE,
          record.doneBlobUri,
          record.doneBlobGeneration,
          done.createTime,
          emptyMap(),
        )
      assertThat(DataAvailabilitySyncWorkItems.hasSameIdentity(item, request)).isTrue()
      return upload.name
    }

    suspend fun correct(scenario: Scenario, verifyAdditiveFulfillment: suspend () -> Unit) {
      retainRawObjectVersions()
      workItemTransport.holdPublications(DataAvailabilitySyncWorkItems.QUEUE)
      // Distinct final memberships exercise all three shards, including empty v1 shards.
      val ids = listOf("event-a-2", "event-c-3", "event-d-4")
      val extractor = EventIdDigestExtractor()
      assertThat(
          ids.map {
            Math.floorMod(extractor.extract(ByteString.copyFromUtf8(it)).high, PHASE0_SHARDS)
          }
        )
        .containsExactly(0, 1, 2)
        .inOrder()
      val folder = "correction"
      val a = writeRawEvents(folder, "a.parquet", listOf(RawEventFixture(ids[0], "A", FIRST_DATE)))
      val b =
        writeRawEvents(
          folder,
          "b.parquet",
          listOf(RawEventFixture("event-b-2", "B", FIRST_DATE, timestampMicros = dayEndMicros)),
        )
      val v1Generation = finalizeRawUpload(folder)
      awaitPipelineIdle()
      val v1 = listUploads().single { it.doneBlobGeneration == v1Generation }
      val phaseParams =
        phaseWorkItems().map {
          it.workItemParams.unpack(WorkItem.WorkItemParams::class.java).appParams
        }
      val poolParams = phaseParams.filter { it.`is`(SubpoolAssignerParams::class.java) }
      assertThat(poolParams).hasSize(PHASE0_SHARDS)
      assertThat(poolParams.map { it.unpack(SubpoolAssignerParams::class.java).modelLine }.toSet())
        .containsExactly(memoized)
      if (scenario != Scenario.EVICTION_RACE) {
        deliverAvailability(v1.name)
        assertThat(listUploads().single { it.name == v1.name }.state)
          .isEqualTo(RawImpressionUpload.State.COMPLETED)
        for (line in listOf(memoized, direct)) {
          assertThat(readLabeledPeople(line).map { it.personId }).containsExactly("A", "B")
        }
      }
      val c = writeRawEvents(folder, "c.parquet", listOf(RawEventFixture(ids[1], "C", FIRST_DATE)))
      val v2Generation = finalizeRawUpload(folder)
      awaitPipelineIdle()
      val v2 = listUploads().single { it.doneBlobGeneration == v2Generation }
      assertThat(listUploadFiles(v2.name).map { it.blobUri }).containsExactly(c)
      deliverAvailability(v2.name)
      if (scenario != Scenario.EVICTION_RACE) verifyAdditiveFulfillment()
      val oldTasks = listAvailabilityTasks(v1.name) + listAvailabilityTasks(v2.name)
      val oldNames = oldTasks.map { it.name }.toSet()
      val pause =
        if (scenario == Scenario.EVICTION_RACE)
          pauseManagedAvailabilityStage(
            v1.name,
            direct,
            DataAvailabilitySync.Stage.METADATA_PERSISTENCE,
          )
        else null
      try {
        if (pause != null) {
          val task = listAvailabilityTasks(v1.name).single { it.cmmsModelLine == direct }
          assertThat(fileStorage.getBlob(blobKey(sidecarUri(b, direct, FIRST_DATE)))).isNotNull()
          workItemTransport.republishWorkItem(task.name)
          withTimeout(60_000) { pause.awaitReached() }
        }
        writeRawEvents(folder, "a.parquet", listOf(RawEventFixture(ids[0], "A-prime", FIRST_DATE)))
        checkNotNull(fileStorage.getBlob(blobKey(b))).delete()
        val d =
          writeRawEvents(
            folder,
            "d.parquet",
            listOf(RawEventFixture(ids[2], "D", FIRST_DATE, timestampMicros = dayEndMicros)),
          )
        val v3Generation = finalizeRawUpload(folder)
        if (pause == null) awaitPipelineIdle()
        val candidate = listCorrectionCandidates().single()
        assertThat(candidate.classification)
          .isEqualTo(RawImpressionUploadCorrectionCandidate.Classification.MIXED)
        if (scenario == Scenario.DUPLICATE) {
          val uploads = listUploads()
          rawWatcher.receivePath(
            "$rawPrefix/$folder/done",
            mapOf(DataWatcher.GENERATION_METADATA_KEY to v3Generation.toString()),
          )
          awaitPipelineIdle()
          assertThat(listUploads()).containsExactlyElementsIn(uploads)
          assertThat(listCorrectionCandidates()).containsExactly(candidate)
        }
        var controller = freshController()
        assertThat(controller.run().failedDataProviders).isEqualTo(0)
        val draft = listHealingOperations().single()
        for (owner in listOf(v1.name, v2.name)) {
          assertThat(
              draft.stepsList
                .filter { it.sourceRawImpressionUpload == owner }
                .map { it.cmmsModelLine }
            )
            .containsExactly(memoized, direct)
        }
        approveHealingOperation(
          draft,
          RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE,
        )
        if (scenario == Scenario.RESTART) {
          workItemTransport.loseNextPublicationResponse(DataAvailabilitySyncWorkItems.QUEUE)
        }
        try {
          assertThat(controller.run().failedDataProviders).isEqualTo(0)
          if (pause != null)
            assertThat(listHealingOperations().single().state)
              .isNotEqualTo(UploadHealingOperation.State.COMPLETE)
        } finally {
          pause?.release()
        }
        awaitPipelineIdle(allowFailures = true)

        var interruptedTask: WorkItem? = null
        var replacementName: String? = null
        var transitions = 0
        do {
          val currentOperation = listHealingOperations().single()
          val queued =
            workItemTransport.workItemsForQueue(DataAvailabilitySyncWorkItems.QUEUE).filter {
              it.state == WorkItem.State.QUEUED && it.name !in oldNames
            }
          if (
            scenario == Scenario.RESTART &&
              interruptedTask == null &&
              currentOperation.state == UploadHealingOperation.State.AVAILABILITY_HANDOFF_PENDING &&
              queued.isNotEmpty()
          ) {
            assertThat(queued).hasSize(1)
            val partial = queued.single()
            replacementName = assertHandoffTaskIdentity(partial, currentOperation)
            interruptedTask = partial
            controller = freshController()
          }
          if (currentOperation.state == UploadHealingOperation.State.COMPLETE) break
          assertThat(controller.run().failedDataProviders).isEqualTo(0)
          awaitPipelineIdle(allowFailures = true)
        } while (++transitions < 12)
        val completedOperation = listHealingOperations().single()
        assertThat(completedOperation.state).isEqualTo(UploadHealingOperation.State.COMPLETE)
        assertThat(completedOperation.name).isEqualTo(draft.name)
        if (scenario == Scenario.RESTART) {
          val partial =
            checkNotNull(interruptedTask) { "The handoff must be interrupted before COMPLETE" }
          val tasks =
            workItemTransport.workItemsForQueue(DataAvailabilitySyncWorkItems.QUEUE).filter {
              it.name !in oldNames
            }
          assertThat(tasks).hasSize(2)
          assertThat(tasks.map { assertHandoffTaskIdentity(it, completedOperation) }.toSet())
            .containsExactly(checkNotNull(replacementName))
          val records = listAvailabilityTasks(checkNotNull(replacementName))
          assertThat(records.map { it.cmmsModelLine }).containsExactly(memoized, direct)
          assertThat(tasks.single { it.name == partial.name }).isEqualTo(partial)
        }

        val rebuilt =
          listUploads().filter {
            it.name != v1.name &&
              it.name != v2.name &&
              it.state != RawImpressionUpload.State.CORRECTION_REQUIRED &&
              it.state != RawImpressionUpload.State.FAILED
          }
        for (upload in rebuilt) deliverAvailability(upload.name, allowFailures = true)
        val active = listUploads().filter { it.state == RawImpressionUpload.State.COMPLETED }
        val files = active.flatMap { listUploadFiles(it.name) }.map { it.blobUri }.toSet()
        assertThat(files).containsAtLeast(a, c, d)
        assertThat(files).doesNotContain(b)
        for (line in listOf(memoized, direct)) {
          val people = readLabeledPeople(line)
          assertThat(people.map { it.personId }).containsExactly("A-prime", "C", "D")
          if (line == memoized) {
            assertThat(people.map { it.vid }).containsExactlyElementsIn(MEMOIZED_VIDS)
          } else {
            assertThat(people.associate { it.personId to it.vid })
              .containsExactlyEntriesIn(DIRECT_VIDS)
          }
        }
        if (scenario == Scenario.DUPLICATE || scenario == Scenario.EVICTION_RACE) {
          val uploads = listUploads()
          val names =
            (if (scenario == Scenario.DUPLICATE) phaseWorkItems().map { it.name }
            else emptyList()) +
              oldTasks.map { it.name } +
              active.flatMap { listAvailabilityTasks(it.name) }.map { it.name }
          for (name in names) workItemTransport.redeliverWorkItem(name)
          awaitPipelineIdle(allowFailures = true)
          assertThat(listUploads()).containsExactlyElementsIn(uploads)
        }
      } finally {
        pause?.release()
      }
    }

    suspend fun fulfill(
      provider: DataProviderData,
      kingdomChannel: io.grpc.Channel,
      line: String,
      people: Set<String>,
      expectedPeople: Set<String> = EXPECTED_PEOPLE,
    ): Long {
      val modelInfo =
        ModelLineInfo(
          comparisonPopulationSpec,
          TestEvent.getDescriptor(),
          InMemoryVidIndexMap.build(comparisonPopulationSpec),
          localAlias = null,
        )
      var actual = 0L
      var observed = 0
      ResultsFulfillerTestHelper.fulfillTestRequisitions(
        provider,
        kingdomChannel,
        RequisitionMetadataServiceCoroutineStub(edpaPublicServer.channel),
        ImpressionDataSourceProvider(
          ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub(
            edpaPublicServer.channel
          ),
          providerName,
          StorageConfig(rootDirectory = File("/")),
          modelLineDataSources =
            mapOf(
              memoized to ModelLineDataSource(outputPrefix, null, null),
              direct to ModelLineDataSource(outputPrefix, null, null),
            ),
          modelLineStorageClientFactory = { metadataStorage },
        ),
        mapOf(memoized to modelInfo, direct to modelInfo),
        kmsClient,
        tempFolder.newFolder(
          "requisitions-${line.substringAfterLast('/')}-${expectedPeople.sorted().joinToString("-")}-${people.sorted().joinToString("-")}"
        ),
        java.time.Clock.fixed(
          FIRST_DATE.plusDays(2).atStartOfDay(ZoneOffset.UTC).toInstant(),
          ZoneOffset.UTC,
        ),
      ) { spec, _, vector ->
        assertThat(spec.modelLine).isEqualTo(line)
        val selected = people.intersect(expectedPeople)
        val expected = selected.size.toLong()
        assertThat(vector.getTotalUncappedImpressions()).isEqualTo(expected)
        val frequencies =
          vector
            .getByteArray()
            .withIndex()
            .filter { it.value.toInt() != 0 }
            .associate { it.index to it.value.toInt() }
        assertThat(vector.size).isEqualTo(600)
        val selectionKey = if (expectedPeople == INTERMEDIATE_PEOPLE) "$line/intermediate" else line
        if (expectedPeople == INTERMEDIATE_PEOPLE) {
          if (people.size == 1) {
            assertThat(frequencies).hasSize(1)
            assertThat(frequencies.values).containsExactly(1)
          } else {
            val combined = mutableMapOf<Int, Int>()
            for (person in INTERMEDIATE_PEOPLE) {
              for ((index, count) in selectorVectors.getValue(selectionKey).getValue(person)) {
                combined[index] = (combined[index] ?: 0) + count
              }
            }
            assertThat(frequencies).containsExactlyEntriesIn(combined)
          }
        } else {
          if (line == memoized && selected.size == 1) {
            assertThat(frequencies).hasSize(1)
            assertThat(frequencies.values).containsExactly(1)
            assertThat(expectedVector(line).keys).containsAtLeastElementsIn(frequencies.keys)
          } else {
            val expectedFrequencies =
              if (line == memoized) {
                if (selected.isEmpty()) emptyMap() else expectedVector(line)
              } else {
                selected
                  .map { DIRECT_VIDS.getValue(it) }
                  .groupingBy { modelInfo.vidIndexMap[it] }
                  .eachCount()
              }
            assertThat(frequencies).containsExactlyEntriesIn(expectedFrequencies)
          }
        }
        if (people.size == 1) {
          val results = selectorVectors.getOrPut(selectionKey) { mutableMapOf() }
          val key = people.single()
          results[key]?.let { assertThat(frequencies).containsExactlyEntriesIn(it) }
          results[key] = frequencies
        }
        actual = vector.getTotalUncappedImpressions()
        observed++
      }
      assertThat(observed).isGreaterThan(0)
      return actual
    }
  }

  companion object {
    private const val PHASE0_SHARDS = 3
    // Discovery order can permute the assignments, but the first three ranked VIDs are fixed.
    private val MEMOIZED_VIDS = setOf(10_056L, 10_013L, 10_084L)
    private val EXPECTED_PEOPLE = setOf("A-prime", "C", "D")
    private val INTERMEDIATE_PEOPLE = setOf("A", "B", "C")
    // A-prime and C share a direct VID, so membership assertions also matter.
    private val DIRECT_VIDS = mapOf("A-prime" to 10_093L, "C" to 10_093L, "D" to 10_098L)

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
