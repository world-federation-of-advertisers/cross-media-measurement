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

import com.google.protobuf.timestamp
import com.google.type.interval
import io.grpc.ManagedChannel
import java.nio.file.Paths
import java.time.Duration
import java.util.*
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.measurement.integration.k8s.testing.CorrectnessTestConfig
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.integration.common.SyntheticGenerationSpecs
import org.wfanet.measurement.loadtest.dataprovider.SyntheticGeneratorEventQuery
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerSimulator
import org.wfanet.measurement.loadtest.measurementconsumer.MetadataSyntheticGeneratorEventQuery
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.MediaType
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata as eventGroupMetadata
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import com.google.cloud.storage.StorageOptions
import java.util.logging.Logger
import kotlinx.coroutines.delay

class EdpAggregatorCorrectnessTest: AbstractEdpAggregatorCorrectnessTest(measurementSystem) {

  private class UploadEventGroup : TestRule {

    private val bucket = "secure-computation-storage-dev-bucket"
    private val eventGroupObjectMapKey = "edp7/event-groups-map/edp7-event-group.pb"
    private val eventGroupObjectKey = "edp7/event-groups/edp7-event-group.pb"
    private val eventGroupBlobUri = "gs://$bucket/$eventGroupObjectKey"
    private val googleProjectId = "halo-cmm-dev"
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
      while (!isEventGroupSyncDone()) {
        logger.info("Waiting on Event Group Sync to complete...")
        delay(30000)
      }
    }

    private fun isEventGroupSyncDone(): Boolean {
      return storageClient.get(bucket, eventGroupObjectMapKey) != null
    }

    private fun deleteExistingEventGroupsMap() {
      storageClient.delete(bucket, eventGroupObjectMapKey)
    }
    private suspend fun uploadEventGroups(eventGroup: List<EventGroup>) {
      val eventGroupsBlobUri =
        SelectedStorageClient.parseBlobUri(eventGroupBlobUri)
      MesosRecordIoStorageClient(
        SelectedStorageClient(
          blobUri = eventGroupsBlobUri,
          rootDirectory = null,
          projectId = googleProjectId,
        )
      ).writeBlob(eventGroupObjectKey, eventGroup.asFlow().map { it.toByteString() })

    }

    private fun createEventGroups(): List<EventGroup> {
      return listOf(
        eventGroup {
          eventGroupReferenceId = "sim-eg-reference-id-1-edp-0"
          measurementConsumer = "measurementConsumers/VCTqwV_vFXw"
          dataAvailabilityInterval = interval {
            startTime = timestamp { seconds = 200 }
            endTime = timestamp { seconds = 300 }
          }
          this.eventGroupMetadata = eventGroupMetadata {
            this.adMetadata = adMetadata {
              this.campaignMetadata = campaignMetadata {
                brand = "brand-2"
                campaign = "campaign-2"
              }
            }
          }
          mediaTypes += MediaType.valueOf("VIDEO")
        }
      )
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

      val eventQuery: SyntheticGeneratorEventQuery =
        MetadataSyntheticGeneratorEventQuery(
          SyntheticGenerationSpecs.SYNTHETIC_POPULATION_SPEC_LARGE,
          MC_ENCRYPTION_PRIVATE_KEY,
        )
      return MeasurementConsumerSimulator(
        measurementConsumerData,
        OUTPUT_DP_PARAMS,
        DataProvidersGrpcKt.DataProvidersCoroutineStub(publicApiChannel),
        EventGroupsGrpcKt.EventGroupsCoroutineStub(publicApiChannel),
        MeasurementsGrpcKt.MeasurementsCoroutineStub(publicApiChannel),
        MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub(publicApiChannel),
        CertificatesGrpcKt.CertificatesCoroutineStub(publicApiChannel),
        MEASUREMENT_CONSUMER_SIGNING_CERTS.trustedCertificates,
        eventQuery,
        ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN,
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
    private const val TEST_CONFIG_NAME = "correctness_test_config.textproto"
    private val TEST_CONFIG: CorrectnessTestConfig by lazy {
      val configFile = getRuntimePath(
        CONFIG_PATH.resolve(
          TEST_CONFIG_NAME
        )
      ).toFile()
      parseTextProto(configFile, CorrectnessTestConfig.getDefaultInstance())
    }

    private val uploadEventGroup = UploadEventGroup()
    private val measurementSystem = RunningMeasurementSystem()

    @ClassRule
    @JvmField val chainedRule = chainRulesSequentially(uploadEventGroup)
  }

}
