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
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.MediaType
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata as eventGroupMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup
import org.wfanet.measurement.loadtest.measurementconsumer.EdpAggregatorMeasurementConsumerSimulator
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerSimulator
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

class EdpAggregatorCorrectnessTest : AbstractEdpAggregatorCorrectnessTest(measurementSystem) {

  override val EVENT_GROUP_FILTERING_LAMBDA_DIRECT_MEASUREMENTS: (CmmsEventGroup) -> Boolean = {
    it.eventGroupReferenceId == GROUP_REFERENCE_ID_EDPA_EDP1
  }

  override val EVENT_GROUP_FILTERING_LAMBDA_HMSS: (CmmsEventGroup) -> Boolean = {
    it.eventGroupReferenceId in setOf(GROUP_REFERENCE_ID_EDPA_EDP1, GROUP_REFERENCE_ID_EDPA_EDP2)
  }

  private class UploadEventGroup : TestRule {

    private val bucket = TEST_CONFIG.storageBucket
    private val googleProjectId: String =
      System.getenv("GOOGLE_CLOUD_PROJECT") ?: error("GOOGLE_CLOUD_PROJECT must be set")
    private val storageClient = StorageOptions.getDefaultInstance().service

    /** Per-eventGroupReferenceId storage config so each provider writes to its own directory. */
    private data class EventGroupStorage(
      val objectMapKey: String,
      val objectKey: String,
      val blobUri: String,
    )

    private val eventGroupStorageMap: Map<String, EventGroupStorage> =
      mapOf(
        GROUP_REFERENCE_ID_EDPA_EDP1 to
          EventGroupStorage(
            objectMapKey = "edp7/event-groups-map/edp7-event-group.binpb",
            objectKey = "edp7/event-groups/edp7-event-group.binpb",
            blobUri = "gs://$bucket/edp7/event-groups/edp7-event-group.binpb",
          ),
        GROUP_REFERENCE_ID_EDPA_EDP2 to
          EventGroupStorage(
            objectMapKey = "edpa_meta/event-groups-map/edpa_meta-event-group.binpb",
            objectKey = "edpa_meta/event-groups/edpa_meta-event-group.binpb",
            blobUri = "gs://$bucket/edpa_meta/event-groups/edpa_meta-event-group.binpb",
          ),
      )

    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          runBlocking {
            deleteExistingEventGroupsMaps()
            val allEventGroups = createEventGroups()
            val eventGroupsByReferenceId: Map<String, List<EventGroup>> =
              allEventGroups.groupBy { it.eventGroupReferenceId }

            for ((refId, groups) in eventGroupsByReferenceId) {
              val storage =
                eventGroupStorageMap[refId]
                  ?: error("Missing storage mapping for eventGroupReferenceId=$refId")

              uploadEventGroups(storage, groups)
              waitForEventGroupSyncToComplete(storage)
              logger.info("Event Group Sync completed for $refId.")
            }

            logger.info("Event Group Sync completed.")
          }
          base.evaluate()
        }
      }
    }

    private suspend fun waitForEventGroupSyncToComplete(storage: EventGroupStorage) {
      withTimeout(EVENT_GROUP_SYNC_TIMEOUT) {
        while (!isEventGroupSyncDone(storage)) {
          logger.info("Waiting on Event Group Sync to complete...")
          delay(EVENT_GROUP_SYNC_POLLING_INTERVAL)
        }
      }
    }

    private fun isEventGroupSyncDone(storage: EventGroupStorage): Boolean {
      return storageClient.get(bucket, storage.objectMapKey) != null
    }

    private fun deleteExistingEventGroupsMaps() {
      eventGroupStorageMap.values.forEach { storage ->
        storageClient.delete(bucket, storage.objectMapKey)
      }
    }

    private suspend fun uploadEventGroups(
      storage: EventGroupStorage,
      eventGroups: List<EventGroup>,
    ) {
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
      return syntheticEventGroupMap.flatMap { (eventGroupReferenceId, syntheticEventGroupSpec) ->
        syntheticEventGroupSpec.dateSpecsList.map { dateSpec ->
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
            }
            mediaTypes += MediaType.valueOf("VIDEO")
          }
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

      fun buildPaths(): List<String> {
        val basePaths =
          listOf("gs://$bucket/edp/edp7/{date}/done", "gs://$bucket/edp/edpa_meta/{date}/done")

        return generateSequence(START_DATE) { it.plusDays(1) }
          .takeWhile { !it.isAfter(END_DATE) }
          .flatMap { date ->
            basePaths.map { basePath -> basePath.replace("{date}", date.format(DATE_FORMATTER)) }
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
        syntheticPopulationSpec,
        syntheticEventGroupMap,
        reportName,
        "modelProviders/Wt5MH8egH4w/modelSuites/NrAN9F9SunM/modelLines/Esau8aCtQ78",
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

    private val TEST_DATA_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "proto",
        "wfa",
        "measurement",
        "loadtest",
        "dataprovider",
      )

    private const val GROUP_REFERENCE_ID_EDPA_EDP1 = "edpa-eg-reference-id-1"
    private const val GROUP_REFERENCE_ID_EDPA_EDP2 = "edpa-eg-reference-id-2"

    private val TEST_DATA_RUNTIME_PATH =
      org.wfanet.measurement.common.getRuntimePath(TEST_DATA_PATH)!!

    val syntheticPopulationSpec: SyntheticPopulationSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve("small_population_spec.textproto").toFile(),
        SyntheticPopulationSpec.getDefaultInstance(),
      )
    val syntheticEventGroupSpec: SyntheticEventGroupSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve("small_data_spec.textproto").toFile(),
        SyntheticEventGroupSpec.getDefaultInstance(),
      )

    val syntheticEventGroupMap =
      mapOf(
        GROUP_REFERENCE_ID_EDPA_EDP1 to syntheticEventGroupSpec,
        GROUP_REFERENCE_ID_EDPA_EDP2 to syntheticEventGroupSpec,
      )

    private val ZONE_ID = ZoneId.of("UTC")

    private val uploadEventGroup = UploadEventGroup()
    private val createDoneBlobs = CreateDoneBlobs()
    private val measurementSystem = RunningMeasurementSystem()

    @ClassRule
    @JvmField
    val chainedRule = chainRulesSequentially(uploadEventGroup, createDoneBlobs, measurementSystem)
  }
}
