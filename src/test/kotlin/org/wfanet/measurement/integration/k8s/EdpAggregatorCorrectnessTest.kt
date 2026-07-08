/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import com.google.protobuf.TypeRegistry
import com.google.protobuf.timestamp
import com.google.type.interval
import io.grpc.ManagedChannel
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Paths
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.logging.Logger
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.ClassRule
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.measurement.integration.k8s.testing.EdpaCorrectnessTestConfig
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.MediaType
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.entityKey
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata as eventGroupMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup
import org.wfanet.measurement.integration.common.EventGroupConfig
import org.wfanet.measurement.integration.common.ImpressionTestDataConfigs
import org.wfanet.measurement.loadtest.dataprovider.EntityKey
import org.wfanet.measurement.loadtest.measurementconsumer.EdpAggregatorMeasurementConsumerSimulator
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerSimulator
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

class EdpAggregatorCorrectnessTest : AbstractEdpAggregatorCorrectnessTest(measurementSystem) {

  override val EVENT_GROUP_FILTERING_LAMBDA_DIRECT_MEASUREMENTS: (CmmsEventGroup) -> Boolean = {
    it.eventGroupReferenceId == EDP_NO_ENTITY_KEY_EVENT_GROUP_REF_ID
  }

  override val EVENT_GROUP_FILTERING_LAMBDA_CROSS_PUB: (CmmsEventGroup) -> Boolean = {
    it.eventGroupReferenceId in
      setOf(EDP_NO_ENTITY_KEY_EVENT_GROUP_REF_ID, EDPA_META_EVENT_GROUP_REF_ID)
  }

  private class UploadEventGroup : TestRule {

    private val bucket = TEST_CONFIG.storageBucket
    private val googleProjectId: String =
      System.getenv("GOOGLE_CLOUD_PROJECT") ?: error("GOOGLE_CLOUD_PROJECT must be set")
    private val storageClient = StorageOptions.getDefaultInstance().service

    /** Per-EDP storage config. Multiple event groups for the same EDP share one blob. */
    private data class EdpStorage(
      val objectMapKey: String,
      val objectKey: String,
      val blobUri: String,
      val eventGroupReferenceIds: Set<String>,
    )

    private val edpStorageList: List<EdpStorage> =
      listOf(
        EdpStorage(
          objectMapKey = "edp7/event-groups-map/edp7-event-group.binpb",
          objectKey = "edp7/event-groups/edp7-event-group.binpb",
          blobUri = "gs://$bucket/edp7/event-groups/edp7-event-group.binpb",
          eventGroupReferenceIds =
            setOf(EDP_NO_ENTITY_KEY_EVENT_GROUP_REF_ID, CREATIVE_ID_EVENT_GROUP_REF_ID) +
              MULTI_CREATIVE_REF_IDS,
        ),
        EdpStorage(
          objectMapKey = "edpa_meta/event-groups-map/edpa_meta-event-group.binpb",
          objectKey = "edpa_meta/event-groups/edpa_meta-event-group.binpb",
          blobUri = "gs://$bucket/edpa_meta/event-groups/edpa_meta-event-group.binpb",
          eventGroupReferenceIds = setOf(EDPA_META_EVENT_GROUP_REF_ID),
        ),
      )

    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          runBlocking {
            deleteExistingEventGroupsMaps()
            val allEventGroups = createEventGroups()

            for (edpStorage in edpStorageList) {
              val groups =
                allEventGroups.filter {
                  it.eventGroupReferenceId in edpStorage.eventGroupReferenceIds
                }
              uploadEventGroups(edpStorage, groups)
              waitForEventGroupSyncToComplete(edpStorage)
              logger.info("Event Group Sync completed for ${edpStorage.eventGroupReferenceIds}.")
            }

            logger.info("Event Group Sync completed.")
          }
          base.evaluate()
        }
      }
    }

    private suspend fun waitForEventGroupSyncToComplete(storage: EdpStorage) {
      withTimeout(EVENT_GROUP_SYNC_TIMEOUT) {
        while (!isEventGroupSyncDone(storage)) {
          logger.info("Waiting on Event Group Sync to complete...")
          delay(EVENT_GROUP_SYNC_POLLING_INTERVAL)
        }
      }
    }

    private fun isEventGroupSyncDone(storage: EdpStorage): Boolean {
      return storageClient.get(bucket, storage.objectMapKey) != null
    }

    private fun deleteExistingEventGroupsMaps() {
      edpStorageList.forEach { storage -> storageClient.delete(bucket, storage.objectMapKey) }
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

    private fun createEventGroups(): List<EventGroup> {
      return syntheticEventGroupMap.flatMap { (eventGroupReferenceId, config) ->
        when (config) {
          is EventGroupConfig.LegacySpec ->
            buildEventGroupsFromSpec(
              eventGroupReferenceId = eventGroupReferenceId,
              spec = config.spec,
              dataEntityKey = null,
              entityMetadata = null,
            )
          is EventGroupConfig.MultiEntityKey ->
            config.entityKeySpecs.flatMap { entityKeySpec ->
              buildEventGroupsFromSpec(
                eventGroupReferenceId =
                  "${entityKeySpec.entityKey.entityType}-${entityKeySpec.entityKey.entityId}",
                spec = entityKeySpec.spec,
                dataEntityKey = entityKeySpec.entityKey,
                entityMetadata = entityKeySpec.entityMetadata,
              )
            }
        }
      }
    }

    private fun buildEventGroupsFromSpec(
      eventGroupReferenceId: String,
      spec: SyntheticEventGroupSpec,
      dataEntityKey: EntityKey?,
      entityMetadata: com.google.protobuf.Struct?,
    ): List<EventGroup> {
      return spec.dateSpecsList.map { dateSpec ->
        val dateRange = dateSpec.dateRange
        val startTime =
          LocalDate.of(dateRange.start.year, dateRange.start.month, dateRange.start.day)
            .atStartOfDay(ZONE_ID)
            .toInstant()
        val endTime =
          LocalDate.of(
              dateRange.endExclusive.year,
              dateRange.endExclusive.month,
              dateRange.endExclusive.day - 1,
            )
            .atTime(23, 59, 59)
            .atZone(ZONE_ID)
            .toInstant()
        eventGroup {
          this.eventGroupReferenceId = eventGroupReferenceId
          measurementConsumer = TEST_CONFIG.measurementConsumer
          dataAvailabilityInterval = interval {
            this.startTime = timestamp { seconds = startTime.epochSecond }
            this.endTime = timestamp { seconds = endTime.epochSecond }
          }
          this.eventGroupMetadata = eventGroupMetadata {
            this.adMetadata = adMetadata {
              this.campaignMetadata = campaignMetadata {
                brand = "some-brand"
                campaign = "some-campaign"
              }
            }
            if (entityMetadata != null) {
              this.entityMetadata = entityMetadata
            }
          }
          if (dataEntityKey != null) {
            this.entityKey = entityKey {
              entityType = dataEntityKey.entityType
              entityId = dataEntityKey.entityId
            }
          }
          mediaTypes += MediaType.valueOf("VIDEO")
        }
      }
    }

    companion object {
      private const val EVENT_GROUP_SYNC_TIMEOUT = 30_000L
      private const val EVENT_GROUP_SYNC_POLLING_INTERVAL = 3000L
    }
  }

  private class CreateDoneBlobs : TestRule {

    private val bucket = TEST_CONFIG.storageBucket
    private val storageClient = StorageOptions.getDefaultInstance().service
    private val googleProjectId: String =
      System.getenv("GOOGLE_CLOUD_PROJECT") ?: error("GOOGLE_CLOUD_PROJECT must be set")

    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          runBlocking {
            logger.info(
              "Creating DONE blobs to trigger the DataAvailabilitySync if not exists already..."
            )
            createDoneBlobs()

            logger.info("Event Group Sync completed.")
          }
          base.evaluate()
        }
      }
    }

    private suspend fun createDoneBlobs() {
      buildPaths().forEach { path ->
        val doneBlobUri = SelectedStorageClient.parseBlobUri(path)
        val selectedStorageClient =
          SelectedStorageClient(
            blobUri = doneBlobUri,
            rootDirectory = null,
            projectId = googleProjectId,
          )
        logger.info("Reading DONE blob...")
        val blob = selectedStorageClient.getBlob(doneBlobUri.key)

        if (blob != null) {
          blob.delete()
        }

        logger.info("Creating a new DONE blob at path: $path...")
        selectedStorageClient.writeBlob(doneBlobUri.key, emptyFlow())
      }
    }

    companion object {
      private val bucket = TEST_CONFIG.storageBucket
      private val DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      private val START_DATE: LocalDate = LocalDate.parse("2021-03-15", DATE_FORMATTER)
      private val END_DATE: LocalDate = LocalDate.parse("2021-03-21", DATE_FORMATTER)

      // edp7's pipelined date — MUST stay in sync with SeedRawImpressionsRule.PIPELINED_DATES. For
      // this date the deployed VidLabeler drops edp7's own `done` marker, so the test must NOT. All
      // earlier dates use pre-labeled impressions staged in edp/edp7/<date>/ (like the direct EDP),
      // so the test drops an edp7 `done` for them too.
      private val EDP7_PIPELINED_DATES: Set<LocalDate> =
        setOf(LocalDate.parse("2021-03-21", DATE_FORMATTER))

      fun buildPaths(): List<String> {
        return generateSequence(START_DATE) { it.plusDays(1) }
          .takeWhile { !it.isAfter(END_DATE) }
          .flatMap { date ->
            val ds = date.format(DATE_FORMATTER)
            buildList {
              // Direct EDP: test-dropped for every date.
              add("gs://$bucket/edp/edpa_meta/$ds/done")
              // Pipelined EDP: only the pre-labeled (non-pipelined) dates get a test-dropped `done`;
              // the pipeline drops its own for EDP7_PIPELINED_DATES.
              if (date !in EDP7_PIPELINED_DATES) {
                add("gs://$bucket/edp/edp7/$ds/done")
              }
            }
          }
          .toList()
      }
    }
  }

  private class RunningMeasurementSystem : MeasurementSystem, TestRule {
    override val runId: String by lazy { UUID.randomUUID().toString() }

    private val storageClient = StorageOptions.getDefaultInstance().service
    private val bucket = TEST_CONFIG.storageBucket
    private val edp_requisitions_prefix = "edp7/requisitions/"

    private lateinit var _mcSimulator: MeasurementConsumerSimulator

    override val mcSimulator: MeasurementConsumerSimulator
      get() = _mcSimulator

    override val publicEventGroupsStub: EventGroupsGrpcKt.EventGroupsCoroutineStub by lazy {
      EventGroupsGrpcKt.EventGroupsCoroutineStub(publicApiChannel)
    }
    override val measurementConsumerName: String = TEST_CONFIG.measurementConsumer
    override val apiAuthenticationKey: String = TEST_CONFIG.apiAuthenticationKey

    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          try {
            _mcSimulator = createMcSimulator()
            base.evaluate()
          } finally {
            shutDownChannels()
          }
        }
      }
    }

    private fun triggerRequisitionFetcher() {

      // Delete existing requisitions from storage bucket
      val blobs = storageClient.list(bucket, Storage.BlobListOption.prefix(edp_requisitions_prefix))

      blobs.iterateAll().forEach { blob ->
        storageClient.delete(bucket, blob.name)
        logger.info("Deleted: ${blob.name}")
      }

      // Wait until requisitions for EDP have status == UNFULFILLED before triggering
      // `RequisitionFetcher`.
      runBlocking {
        withTimeoutOrNull(REQUISITIONS_SYNC_TIMEOUT) {
          var areRequisitionsReady: Boolean

          do {
            val jwt = TEST_CONFIG.authIdToken
            val requisitionFetcherEndpoint = TEST_CONFIG.requisitionFetcherEndpoint

            val client = HttpClient.newHttpClient()
            val request =
              HttpRequest.newBuilder()
                .uri(URI.create(requisitionFetcherEndpoint))
                .timeout(Duration.ofSeconds(120))
                .header("Authorization", "Bearer $jwt")
                .GET()
                .build()
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            check(response.statusCode() == 200)

            val blobCount =
              storageClient
                .list(bucket, Storage.BlobListOption.prefix(edp_requisitions_prefix))
                .iterateAll()
                .count()

            areRequisitionsReady = blobCount > 0

            if (!areRequisitionsReady) {
              logger.info("Waiting for requisitions to appear...")
              delay(REQUISITIONS_SYNC_POLLING_INTERVAL)
            } else {
              logger.info("Requisitions written to the Storage Client")
            }
          } while (!areRequisitionsReady)
        }
      }
    }

    private fun createMcSimulator(): MeasurementConsumerSimulator {

      val reportName =
        ReportKey(
            MeasurementConsumerKey.fromName(measurementConsumerData.name)!!.measurementConsumerId,
            "some-report-id",
          )
          .toName()

      return EdpAggregatorMeasurementConsumerSimulator(
        measurementConsumerData,
        OUTPUT_DP_PARAMS,
        DataProvidersGrpcKt.DataProvidersCoroutineStub(publicApiChannel),
        EventGroupsGrpcKt.EventGroupsCoroutineStub(publicApiChannel),
        MeasurementsGrpcKt.MeasurementsCoroutineStub(publicApiChannel),
        MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub(publicApiChannel),
        CertificatesGrpcKt.CertificatesCoroutineStub(publicApiChannel),
        MEASUREMENT_CONSUMER_SIGNING_CERTS.trustedCertificates,
        TestEvent.getDefaultInstance(),
        ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN,
        populationSpec,
        resolvedSyntheticEventGroupMap,
        reportName,
        provisionModelResources.memoizedModelLine ?: TEST_CONFIG.modelLine,
        listEventGroupsEntityTypes = listOf("campaign", "creative-id"),
        onMeasurementsCreated = ::triggerRequisitionFetcher,
      )
    }

    private fun shutDownChannels() {
      for (channel in channels) {
        channel.shutdown()
      }
    }

    companion object {
      private const val REQUISITIONS_SYNC_TIMEOUT = 60_000L * 5
      private const val REQUISITIONS_SYNC_POLLING_INTERVAL = 5000L

      private val channels = mutableListOf<ManagedChannel>()

      val publicApiChannel =
        buildMutualTlsChannel(
            TEST_CONFIG.kingdomPublicApiTarget,
            MEASUREMENT_CONSUMER_SIGNING_CERTS,
            TEST_CONFIG.kingdomPublicApiCertHost.ifEmpty { null },
          )
          .also { channels.add(it) }
          .withDefaultDeadline(RPC_DEADLINE_DURATION)

      val measurementConsumerData =
        MeasurementConsumerData(
          TEST_CONFIG.measurementConsumer,
          MC_SIGNING_KEY,
          MC_ENCRYPTION_PRIVATE_KEY,
          TEST_CONFIG.apiAuthenticationKey,
        )
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val RPC_DEADLINE_DURATION = Duration.ofSeconds(30)
    private val CONFIG_PATH =
      Paths.get("src", "test", "kotlin", "org", "wfanet", "measurement", "integration", "k8s")
    private const val TEST_CONFIG_NAME = "edpa_correctness_test_config.textproto"
    private val TEST_CONFIG: EdpaCorrectnessTestConfig by lazy {
      val configFile = getRuntimePath(CONFIG_PATH.resolve(TEST_CONFIG_NAME)).toFile()
      parseTextProto(configFile, EdpaCorrectnessTestConfig.getDefaultInstance())
    }

    private val POPULATION_SPEC_TYPE_REGISTRY: TypeRegistry =
      TypeRegistry.newBuilder().add(Person.getDescriptor()).build()

    private val IMPRESSION_TEST_DATA_CONFIG: ImpressionTestDataConfig by lazy {
      val configFile =
        getRuntimePath(CONFIG_PATH.resolve("impression_test_data_config.textproto"))!!.toFile()
      parseTextProto(configFile, ImpressionTestDataConfig.getDefaultInstance())
    }

    val populationSpec: PopulationSpec by lazy {
      parseTextProto(
        ImpressionTestDataConfigs.resolveSpecPath(
          IMPRESSION_TEST_DATA_CONFIG.populationSpecResourcePath
        ),
        PopulationSpec.getDefaultInstance(),
        POPULATION_SPEC_TYPE_REGISTRY,
      )
    }

    val syntheticEventGroupMap: Map<String, EventGroupConfig> =
      ImpressionTestDataConfigs.toEventGroupMap(IMPRESSION_TEST_DATA_CONFIG)

    val resolvedSyntheticEventGroupMap: Map<String, EventGroupConfig> =
      ImpressionTestDataConfigs.toFlatEventGroupMap(IMPRESSION_TEST_DATA_CONFIG)

    private val ZONE_ID = ZoneId.of("UTC")

    /**
     * edp7's flattened event-group configs (edpa_meta excluded), keyed by the flattened id (legacy
     * groups by event_group_reference_id; entity-key groups split so each entity key spec is its own
     * entry keyed "${entityType}-${entityId}"). Each entry carries its own entity key so the seeder
     * can stamp the pipelined day's raw impressions with the same entity keys the out-of-band
     * days-15-20 data uses.
     */
    private val edp7EventGroupConfigs: Map<String, EventGroupConfig> = run {
      val configs = linkedMapOf<String, EventGroupConfig>()
      for ((id, config) in resolvedSyntheticEventGroupMap) {
        if (id != AbstractEdpAggregatorCorrectnessTest.EDPA_META_EVENT_GROUP_REF_ID) {
          configs[id] = config
        }
      }
      configs
    }

    private val provisionModelResources =
      VidLabelerModelResourcesRule(
        TEST_CONFIG.kingdomPublicApiTarget,
        TEST_CONFIG.kingdomPublicApiCertHost.ifEmpty { null },
      )
    private val uploadEventGroup = UploadEventGroup()
    private val seedRawImpressions =
      SeedRawImpressionsRule(populationSpec, edp7EventGroupConfigs)
    private val awaitVidLabeling = AwaitVidLabelingRule()
    // Writes pre-labeled impressions stamped with the memoized model line for the dates the
    // pipeline does not produce, so the `done` blobs dropped by createDoneBlobs register them under
    // the memoized line. Must run after provisionModelResources (needs memoizedModelLine) and
    // uploadEventGroup, and before createDoneBlobs.
    private val writeReusedLabeledImpressions =
      WriteReusedLabeledImpressionsRule(
        config = IMPRESSION_TEST_DATA_CONFIG,
        populationSpec = populationSpec,
        bucket = TEST_CONFIG.storageBucket,
        memoizedModelLineProvider = { provisionModelResources.memoizedModelLine },
      )
    private val createDoneBlobs = CreateDoneBlobs()
    private val measurementSystem = RunningMeasurementSystem()

    @ClassRule
    @JvmField
    val chainedRule = chainRulesSequentially(
        provisionModelResources,
        uploadEventGroup,
        seedRawImpressions,
        awaitVidLabeling,
        writeReusedLabeledImpressions,
        createDoneBlobs,
        measurementSystem,
      )
  }
}
