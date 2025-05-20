/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.common

import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.timestamp
import com.google.type.interval
import io.grpc.Channel
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.config.securecomputation.WatchedPath
import org.wfanet.measurement.edpaggregator.eventgroups.EventGroupSync
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.MediaType
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata as eventGroupMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.MappedEventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.mappedEventGroup
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionFetcher
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.loadtest.edpaggregator.ImpressionsWriter
import org.wfanet.measurement.loadtest.edpaggregator.SyntheticDataGeneration
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.loadtest.resourcesetup.Resources
import org.wfanet.measurement.loadtest.resourcesetup.Resources.Resource
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.securecomputation.datawatcher.testing.DataWatcherSubscribingStorageClient
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.InternalApiServices
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

class InProcessEdpAggregatorComponents(
  private val internalServicesRule: ProviderRule<InternalApiServices>,
  private val pubSubClient: GooglePubSubEmulatorClient,
  private val storagePath: Path,
  private val syntheticPopulationSpec: SyntheticPopulationSpec =
    SyntheticGenerationSpecs.SYNTHETIC_POPULATION_SPEC_SMALL,
  private val syntheticEventGroupMap: Map<String, SyntheticEventGroupSpec> =
    mapOf("edpa-eg-reference-id-1" to SyntheticGenerationSpecs.SYNTHETIC_DATA_SPECS_SMALL[0]),
) : TestRule {

  private val internalServices: InternalApiServices
    get() = internalServicesRule.value

  private val storageClient: StorageClient = FileSystemStorageClient(storagePath.toFile())

  private lateinit var edpResourceName: String

  private lateinit var publicApiChannel: Channel

  private val secureComputationPublicApi by lazy {
    InProcessSecureComputationPublicApi(internalServicesProvider = { internalServices })
  }

  private val workItemsClient: WorkItemsCoroutineStub by lazy {
    WorkItemsCoroutineStub(secureComputationPublicApi.publicApiChannel)
      .withPrincipalName(edpResourceName)
  }

  private val requisitionsClient: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(publicApiChannel).withPrincipalName(edpResourceName)
  }

  private val eventGroupsClient: EventGroupsCoroutineStub by lazy {
    EventGroupsCoroutineStub(publicApiChannel).withPrincipalName(edpResourceName)
  }

  private lateinit var dataWatcher: DataWatcher

  private lateinit var requisitionFetcher: RequisitionFetcher

  private lateinit var eventGroupSync: EventGroupSync

  private val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"

  private val kmsClient by lazy {
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    val kmsClient = FakeKmsClient()
    kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
    kmsClient
  }

  // TODO: Add Results Fulfiller App when ready

  val ruleChain: TestRule by lazy {
    chainRulesSequentially(internalServicesRule, secureComputationPublicApi)
  }

  private lateinit var edpDisplayNameToResourceMap: Map<String, Resources.Resource>

  fun getDataProviderResourceNames(): List<String> {
    return edpDisplayNameToResourceMap.values.map { it.name }
  }

  private val loggingName = javaClass.simpleName
  private val backgroundScope =
    CoroutineScope(
      Dispatchers.Default +
        CoroutineName(loggingName) +
        CoroutineExceptionHandler { _, e ->
          logger.log(Level.SEVERE, e) { "Error in $loggingName" }
        }
    )

  fun startDaemons(
    kingdomChannel: Channel,
    measurementConsumerData: MeasurementConsumerData,
    edpDisplayNameToResourceMap: Map<String, Resource>,
  ) = runBlocking {
    val edpShortName = "edp1"
    pubSubClient.createTopic(PROJECT_ID, FULFILLER_TOPIC_ID)
    pubSubClient.createSubscription(PROJECT_ID, SUBSCRIPTION_ID, FULFILLER_TOPIC_ID)
    edpResourceName = edpDisplayNameToResourceMap.getValue(edpShortName).name
    print(edpResourceName)
    publicApiChannel = kingdomChannel
    val resultsFulfillerParams =
      getResultsFulfillerParams(
        edpShortName,
        edpResourceName,
        DataProviderCertificateKey.fromName(
          edpDisplayNameToResourceMap.getValue(edpShortName).dataProvider.certificate
        )!!,
        "file:///$IMPRESSIONS_METADATA_BUCKET",
      )
    val watchedPaths =
      getDataWatcherResultFulfillerParamsConfig(
        blobPrefix = "file:///$REQUISITION_STORAGE_PREFIX/",
        edpResultFulfillerConfigs = mapOf(edpResourceName to resultsFulfillerParams),
      )
    for (path in watchedPaths) {
      WatchedPath.parseFrom(path.toByteString())
    }
    dataWatcher = DataWatcher(workItemsClient, watchedPaths)

    val subscribingStorageClient = DataWatcherSubscribingStorageClient(storageClient, "file:///")
    subscribingStorageClient.subscribe(dataWatcher)

    requisitionFetcher =
      RequisitionFetcher(
        requisitionsClient,
        subscribingStorageClient,
        edpResourceName,
        REQUISITION_STORAGE_PREFIX,
        10,
      )
    backgroundScope.launch {
      while (true) {
        requisitionFetcher.fetchAndStoreRequisitions()
        delay(1000)
      }
    }
    logger.info("$measurementConsumerData")
    logger.info("$edpDisplayNameToResourceMap")
    val eventGroups =
      listOf(
        eventGroup {
          eventGroupReferenceId = "edpa-eg-reference-id-1"
          measurementConsumer = measurementConsumerData.name
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
    eventGroupSync =
      EventGroupSync(
        edpResourceName,
        eventGroupsClient,
        eventGroups.asFlow(),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000L)),
      )
    val mappedEventGroups: List<MappedEventGroup> = runBlocking { eventGroupSync.sync().toList() }
    logger.info("Received mappedEventGroups: $mappedEventGroups")
    backgroundScope.launch {
      runBlocking { writeImpressionData(mappedEventGroups) }

      // TODO: Run Results Fulfiller App
    }
  }

  private suspend fun writeImpressionData(mappedEventGroups: List<MappedEventGroup>) {
    Files.createDirectories(storagePath.resolve(IMPRESSIONS_BUCKET))
    Files.createDirectories(storagePath.resolve(IMPRESSIONS_METADATA_BUCKET))

    mappedEventGroups.forEach { mappedEventGroup ->
      val events =
        SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          syntheticPopulationSpec,
          syntheticEventGroupMap.getValue(mappedEventGroup.eventGroupReferenceId),
        )
      // TODO: Change this to event-group-reference ID once the app is updated
      val impressionWriter =
        ImpressionsWriter(
          "event-group-id/${mappedEventGroup.eventGroupResource}",
          kekUri,
          kmsClient,
          IMPRESSIONS_BUCKET,
          IMPRESSIONS_METADATA_BUCKET,
          storagePath.toFile(),
          "file:///",
        )
      runBlocking { impressionWriter.writeLabeledImpressionData(events) }
    }
  }

  fun stopDaemons() {
    runBlocking {
      pubSubClient.deleteTopic(PROJECT_ID, FULFILLER_TOPIC_ID)
      pubSubClient.deleteSubscription(PROJECT_ID, SUBSCRIPTION_ID)
    }
  }

  override fun apply(statement: Statement, description: Description): Statement {
    return ruleChain.apply(statement, description)
  }

  init {
    AeadConfig.register()
    StreamingAeadConfig.register()
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val IMPRESSIONS_BUCKET = "impression-bucket"
    private const val IMPRESSIONS_METADATA_BUCKET = "impression-metadata-bucket"
    private const val REQUISITION_STORAGE_PREFIX = "requisition-storage-prefix"
  }
}
