/*
 * Copyright 2025 The Cross-Media Measurement Authors
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
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.TypeRegistry
import com.google.protobuf.timestamp
import com.google.type.interval
import io.grpc.Channel
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneId
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.config.securecomputation.WatchedPath
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.eventgroups.EventGroupSync
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.MediaType
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata as eventGroupMetadata
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.MappedEventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionFetcher
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionGrouperByReportId
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionsValidator
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ResultsFulfillerApp
import org.wfanet.measurement.edpaggregator.resultsfulfiller.testing.TestRequisitionStubFactory
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.integration.deploy.gcloud.SecureComputationServicesProviderRule
import org.wfanet.measurement.loadtest.dataprovider.LabeledEventDateShard
import org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration
import org.wfanet.measurement.loadtest.edpaggregator.testing.ImpressionsWriter
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.loadtest.resourcesetup.Resources.Resource
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.securecomputation.datawatcher.testing.DataWatcherSubscribingStorageClient
import org.wfanet.measurement.securecomputation.deploy.gcloud.publisher.GoogleWorkItemPublisher
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.InternalApiServices
import org.wfanet.measurement.securecomputation.deploy.gcloud.testing.TestIdTokenProvider
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

class InProcessEdpAggregatorComponents(
  secureComputationDatabaseAdmin: SpannerDatabaseAdmin,
  private val storagePath: Path,
  private val pubSubClient: GooglePubSubEmulatorClient,
  private val syntheticPopulationSpec: SyntheticPopulationSpec,
  private val syntheticEventGroupMap: Map<String, SyntheticEventGroupSpec>,
  private val populationSpecMap: Map<String, PopulationSpec>,
) : TestRule {

  private val internalServicesRule: ProviderRule<InternalApiServices> =
    SecureComputationServicesProviderRule(
      workItemPublisher = GoogleWorkItemPublisher(PROJECT_ID, pubSubClient),
      queueMapping = QueueMapping(QUEUES_CONFIG),
      emulatorDatabaseAdmin = secureComputationDatabaseAdmin,
    )

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

  private val kmsClients by lazy { mutableMapOf(edpResourceName to kmsClient as KmsClient) }

  private val resultFulfillerApp by lazy {
    val typeRegistry = TypeRegistry.newBuilder().add(TestEvent.getDescriptor()).build()
    val requisitionStubFactory = TestRequisitionStubFactory(publicApiChannel)
    val subscriber = Subscriber(PROJECT_ID, pubSubClient)
    val getStorageConfig = { _: ResultsFulfillerParams.StorageParams ->
      StorageConfig(rootDirectory = storagePath.toFile())
    }
    ResultsFulfillerApp(
      parser = WorkItem.parser(),
      subscriptionId = SUBSCRIPTION_ID,
      workItemsClient = workItemsClient,
      workItemAttemptsClient =
        WorkItemAttemptsCoroutineStub(secureComputationPublicApi.publicApiChannel),
      queueSubscriber = subscriber,
      kmsClients = kmsClients,
      requisitionStubFactory = requisitionStubFactory,
      typeRegistry = typeRegistry,
      getImpressionsMetadataStorageConfig = getStorageConfig,
      getImpressionsStorageConfig = getStorageConfig,
      getRequisitionsStorageConfig = getStorageConfig,
      populationSpecMap = populationSpecMap,
    )
  }

  val ruleChain: TestRule by lazy {
    chainRulesSequentially(internalServicesRule, secureComputationPublicApi)
  }

  private val loggingName = javaClass.simpleName
  private val backgroundJob = Job()
  private val backgroundScope =
    CoroutineScope(
      backgroundJob +
        Dispatchers.Default +
        CoroutineName(loggingName) +
        CoroutineExceptionHandler { _, e ->
          logger.log(Level.SEVERE, e) { "Error in $loggingName" }
        }
    )

  private val throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofSeconds(1L))

  fun startDaemons(
    kingdomChannel: Channel,
    measurementConsumerData: MeasurementConsumerData,
    edpDisplayNameToResourceMap: Map<String, Resource>,
    edpAggregatorShortName: String,
  ) = runBlocking {
    edpResourceName = edpDisplayNameToResourceMap.getValue(edpAggregatorShortName).name
    publicApiChannel = kingdomChannel
    val resultsFulfillerParams =
      getResultsFulfillerParams(
        edpAggregatorShortName,
        edpResourceName,
        DataProviderCertificateKey.fromName(
          edpDisplayNameToResourceMap.getValue(edpAggregatorShortName).dataProvider.certificate
        )!!,
        "file:///$IMPRESSIONS_METADATA_BUCKET",
        noiseType = ResultsFulfillerParams.NoiseParams.NoiseType.CONTINUOUS_GAUSSIAN,
      )
    val watchedPaths =
      getDataWatcherResultFulfillerParamsConfig(
        blobPrefix = "file:///$REQUISITION_STORAGE_PREFIX/",
        edpResultFulfillerConfigs = mapOf(edpResourceName to resultsFulfillerParams),
      )
    for (path in watchedPaths) {
      WatchedPath.parseFrom(path.toByteString())
    }
    dataWatcher =
      DataWatcher(workItemsClient, watchedPaths, idTokenProvider = TestIdTokenProvider())

    val subscribingStorageClient = DataWatcherSubscribingStorageClient(storageClient, "file:///")
    subscribingStorageClient.subscribe(dataWatcher)

    val edpPrivateKey = getDataProviderPrivateEncryptionKey(edpAggregatorShortName)

    val requisitionsValidator = RequisitionsValidator(edpPrivateKey)

    val requisitionGrouper =
      RequisitionGrouperByReportId(
        requisitionsValidator,
        eventGroupsClient,
        requisitionsClient,
        throttler,
      )

    requisitionFetcher =
      RequisitionFetcher(
        requisitionsClient,
        subscribingStorageClient,
        edpResourceName,
        REQUISITION_STORAGE_PREFIX,
        requisitionGrouper,
        ::createDeterministicId,
      )
    backgroundScope.launch {
      while (true) {
        delay(1000)
        requisitionFetcher.fetchAndStoreRequisitions()
      }
    }
    val eventGroups = buildEventGroups(measurementConsumerData)
    eventGroupSync =
      EventGroupSync(edpResourceName, eventGroupsClient, eventGroups.asFlow(), throttler)
    val mappedEventGroups: List<MappedEventGroup> = runBlocking { eventGroupSync.sync().toList() }
    logger.info("Received mappedEventGroups: $mappedEventGroups")
    backgroundScope.launch {
      runBlocking { writeImpressionData(mappedEventGroups) }
      resultFulfillerApp.run()
    }
  }

  private suspend fun refuseRequisition(
    requisitionsStub: RequisitionsCoroutineStub,
    requisition: Requisition,
    refusal: Requisition.Refusal,
  ) {
    try {
      logger.info("Requisition ${requisition.name} was refused. $refusal")
      val request = refuseRequisitionRequest {
        this.name = requisition.name
        this.refusal = RequisitionKt.refusal { justification = refusal.justification }
      }
      requisitionsStub.refuseRequisition(request)
    } catch (e: Exception) {
      logger.log(Level.SEVERE, "Error while refusing requisition ${requisition.name}", e)
    }
  }

  private fun buildEventGroups(measurementConsumerData: MeasurementConsumerData): List<EventGroup> {
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
          measurementConsumer = measurementConsumerData.name
          dataAvailabilityInterval = interval {
            this.startTime = timestamp { seconds = startTime.epochSecond }
            this.endTime = timestamp { seconds = endTime.epochSecond }
          }
          this.eventGroupMetadata = eventGroupMetadata {
            this.adMetadata = adMetadata {
              this.campaignMetadata = campaignMetadata {
                brand = "some-brand"
                campaign = "some-brand"
              }
            }
          }
          mediaTypes += MediaType.valueOf("VIDEO")
        }
      }
    }
  }

  private suspend fun writeImpressionData(mappedEventGroups: List<MappedEventGroup>) {
    withContext(Dispatchers.IO) {
      Files.createDirectories(storagePath.resolve(IMPRESSIONS_BUCKET))
      Files.createDirectories(storagePath.resolve(IMPRESSIONS_METADATA_BUCKET))
    }

    mappedEventGroups.forEach { mappedEventGroup ->
      val events: Sequence<LabeledEventDateShard<TestEvent>> =
        SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          syntheticPopulationSpec,
          syntheticEventGroupMap.getValue(mappedEventGroup.eventGroupReferenceId),
        )
      val impressionWriter =
        ImpressionsWriter(
          "event-group-reference-id/${mappedEventGroup.eventGroupReferenceId}",
          kekUri,
          kmsClient,
          IMPRESSIONS_BUCKET,
          IMPRESSIONS_METADATA_BUCKET,
          storagePath.toFile(),
          "file:///",
        )
      impressionWriter.writeLabeledImpressionData(events)
    }
  }

  fun stopDaemons() {
    backgroundJob.cancel()
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
    private val ZONE_ID = ZoneId.of("UTC")

    fun createDeterministicId(groupedRequisition: GroupedRequisitions): String {
      return "hash_value"
    }
  }
}
