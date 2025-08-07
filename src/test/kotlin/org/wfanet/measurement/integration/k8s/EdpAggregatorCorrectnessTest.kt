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
import java.util.UUID
import java.util.logging.Logger
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.ClassRule
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.measurement.integration.k8s.testing.EdpaCorrectnessTestConfig
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroup as PublicApiEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
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
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

class EdpAggregatorCorrectnessTest : AbstractEdpAggregatorCorrectnessTest(measurementSystem) {

  private class UploadEventGroup : TestRule {

    private val bucket = "secure-computation-storage-dev-bucket"
    private val eventGroupObjectMapKey = "edp7/event-groups-map/edp7-event-group.pb"
    private val eventGroupObjectKey = "edp7/event-groups/edp7-event-group.pb"
    private val eventGroupBlobUri = "gs://$bucket/$eventGroupObjectKey"
    private val googleProjectId: String =
      System.getenv("GOOGLE_CLOUD_PROJECT") ?: error("GOOGLE_CLOUD_PROJECT must be set")
    private val storageClient = StorageOptions.getDefaultInstance().service

    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          runBlocking {
            deleteExistingEventGroupsMap()
            uploadEventGroups(createEventGroups())
            waitForEventGroupSyncToComplete()
            logger.info("Event Group Sync completed.")
          }
          base.evaluate()
        }
      }
    }

    private suspend fun waitForEventGroupSyncToComplete() {
      withTimeout(EVENT_GROUP_SYNC_TIMEOUT) {
        while (!isEventGroupSyncDone()) {
          logger.info("Waiting on Event Group Sync to complete...")
          delay(EVENT_GROUP_SYNC_POLLING_INTERVAL)
        }
      }
    }

    private fun isEventGroupSyncDone(): Boolean {
      return storageClient.get(bucket, eventGroupObjectMapKey) != null
    }

    private fun deleteExistingEventGroupsMap() {
      storageClient.delete(bucket, eventGroupObjectMapKey)
    }

    private suspend fun uploadEventGroups(eventGroup: List<EventGroup>) {
      val eventGroupsBlobUri = SelectedStorageClient.parseBlobUri(eventGroupBlobUri)
      MesosRecordIoStorageClient(
          SelectedStorageClient(
            blobUri = eventGroupsBlobUri,
            rootDirectory = null,
            projectId = googleProjectId,
          )
        )
        .writeBlob(eventGroupObjectKey, eventGroup.asFlow().map { it.toByteString() })
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

  private class RunningMeasurementSystem : MeasurementSystem, TestRule {
    override val runId: String by lazy { UUID.randomUUID().toString() }

    private lateinit var _mcSimulator: MeasurementConsumerSimulator

    override val mcSimulator: MeasurementConsumerSimulator
      get() = _mcSimulator

    private val channels = mutableListOf<ManagedChannel>()

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
    }

    private fun createMcSimulator(): MeasurementConsumerSimulator {
      val measurementConsumerData =
        MeasurementConsumerData(
          TEST_CONFIG.measurementConsumer,
          MC_SIGNING_KEY,
          MC_ENCRYPTION_PRIVATE_KEY,
          TEST_CONFIG.apiAuthenticationKey,
        )

      val publicApiChannel =
        buildMutualTlsChannel(
            TEST_CONFIG.kingdomPublicApiTarget,
            MEASUREMENT_CONSUMER_SIGNING_CERTS,
            TEST_CONFIG.kingdomPublicApiCertHost.ifEmpty { null },
          )
          .also { channels.add(it) }
          .withDefaultDeadline(RPC_DEADLINE_DURATION)

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
        eventGroupFilter = EVENT_GROUP_FILTERING_LAMBDA,
        onMeasurementsCreated = ::triggerRequisitionFetcher,
      )
    }

    private fun shutDownChannels() {
      for (channel in channels) {
        channel.shutdown()
      }
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

    private const val REQUIRED_EVENT_GROUP_REFERENCE_ID = "edpa-eg-reference-id-1"
    private val EVENT_GROUP_FILTERING_LAMBDA: (PublicApiEventGroup) -> Boolean = {
      it.eventGroupReferenceId == REQUIRED_EVENT_GROUP_REFERENCE_ID
    }

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

    val syntheticEventGroupMap = mapOf("edpa-eg-reference-id-1" to syntheticEventGroupSpec)

    private val ZONE_ID = ZoneId.of("UTC")

    private val uploadEventGroup = UploadEventGroup()
    private val measurementSystem = RunningMeasurementSystem()

    @ClassRule
    @JvmField
    val chainedRule = chainRulesSequentially(uploadEventGroup, measurementSystem)
  }
}
