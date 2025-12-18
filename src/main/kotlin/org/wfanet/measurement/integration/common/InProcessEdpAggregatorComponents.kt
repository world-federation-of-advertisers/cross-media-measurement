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
import com.google.protobuf.timestamp
import com.google.type.Interval
import com.google.type.interval
import io.grpc.Channel
import io.grpc.StatusException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneId
import java.util.*
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
import org.wfanet.measurement.api.v2alpha.DataProviderKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.replaceDataProviderCapabilitiesRequest
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
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.MappedEventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionFetcher
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionGrouperByReportId
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionsValidator
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ModelLineInfo
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ResultsFulfillerApp
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ResultsFulfillerMetrics
import org.wfanet.measurement.edpaggregator.resultsfulfiller.testing.TestRequisitionStubFactory
import org.wfanet.measurement.edpaggregator.v1alpha.CreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.createImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
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
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.InternalApiServices as InternalSecureComputationApiServices
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
  private val modelLineInfoMap: Map<String, ModelLineInfo>,
) : TestRule {

  private val storageClient: StorageClient = FileSystemStorageClient(storagePath.toFile())

  private lateinit var edpResourceNameMap: Map<String, String>

  private lateinit var publicApiChannel: Channel

  private lateinit var duchyChannelMap: Map<String, Channel>

  private val internalSecureComputationServicesRule:
    ProviderRule<InternalSecureComputationApiServices> =
    SecureComputationServicesProviderRule(
      workItemPublisher = GoogleWorkItemPublisher(PROJECT_ID, pubSubClient),
      queueMapping = QueueMapping(QUEUES_CONFIG),
      emulatorDatabaseAdmin = secureComputationDatabaseAdmin,
    )

  private val internalSecureComputationServices: InternalSecureComputationApiServices
    get() = internalSecureComputationServicesRule.value

  private val secureComputationPublicApi by lazy {
    InProcessSecureComputationPublicApi(
      internalServicesProvider = { internalSecureComputationServices }
    )
  }

  private val workItemsClient: WorkItemsCoroutineStub by lazy {
    WorkItemsCoroutineStub(secureComputationPublicApi.publicApiChannel)
  }

  private val edpAggregatorSystemApi by lazy {
    InProcessEdpAggregatorSystemApi(secureComputationDatabaseAdmin)
  }

  private val requisitionMetadataClient: RequisitionMetadataServiceCoroutineStub by lazy {
    RequisitionMetadataServiceCoroutineStub(edpAggregatorSystemApi.publicApiChannel)
  }

  private val impressionMetadataClient: ImpressionMetadataServiceCoroutineStub by lazy {
    ImpressionMetadataServiceCoroutineStub(edpAggregatorSystemApi.publicApiChannel)
  }

  private lateinit var dataWatcher: DataWatcher

  private lateinit var eventGroupSync: EventGroupSync

  private val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"

  private val kmsClient by lazy {
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    val kmsClient = FakeKmsClient()
    kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
    kmsClient
  }

  private lateinit var kmsClients: Map<String, KmsClient>

  private val resultFulfillerApp by lazy {
    val requisitionStubFactory = TestRequisitionStubFactory(publicApiChannel, duchyChannelMap)
    val subscriber =
      Subscriber(
        projectId = PROJECT_ID,
        googlePubSubClient = pubSubClient,
        maxMessages = 1,
        pullIntervalMillis = 100,
        blockingContext = Dispatchers.IO,
      )
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
      kmsClients = kmsClients.toMutableMap(),
      requisitionMetadataStub = requisitionMetadataClient,
      impressionMetadataStub = impressionMetadataClient,
      requisitionStubFactory = requisitionStubFactory,
      getImpressionsMetadataStorageConfig = getStorageConfig,
      getImpressionsStorageConfig = getStorageConfig,
      getRequisitionsStorageConfig = getStorageConfig,
      modelLineInfoMap = modelLineInfoMap,
      metrics = ResultsFulfillerMetrics.create(),
    )
  }

  val ruleChain: TestRule by lazy {
    chainRulesSequentially(
      internalSecureComputationServicesRule,
      secureComputationPublicApi,
      edpAggregatorSystemApi,
    )
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
    edpAggregatorShortNames: List<String>,
    duchyMap: Map<String, Channel>,
  ) = runBlocking {
    publicApiChannel = kingdomChannel
    duchyChannelMap = duchyMap
    edpResourceNameMap =
      edpAggregatorShortNames.associateWith { edpAggregatorShortName ->
        edpDisplayNameToResourceMap.getValue(edpAggregatorShortName).name
      }
    edpResourceNameMap.toList().forEach { (edpAggregatorShortName, edpResourceName) ->
      val dataProvidersStub: DataProvidersCoroutineStub =
        DataProvidersCoroutineStub(publicApiChannel).withPrincipalName(edpResourceName)
      dataProvidersStub.replaceDataProviderCapabilities(
        replaceDataProviderCapabilitiesRequest {
          name = edpResourceName
          capabilities = DataProviderKt.capabilities { honestMajorityShareShuffleSupported = true }
        }
      )
    }
    val watchedPaths: List<WatchedPath> = run {
      val resultsFulfillerParamsMap: Map<String, ResultsFulfillerParams> =
        edpResourceNameMap.toList().associate { (edpAggregatorShortName, edpResourceName) ->
          edpAggregatorShortName to
            getResultsFulfillerParams(
              edpAggregatorShortName,
              edpResourceName,
              DataProviderCertificateKey.fromName(
                edpDisplayNameToResourceMap
                  .getValue(edpAggregatorShortName)
                  .dataProvider
                  .certificate
              )!!,
              "file:///$IMPRESSIONS_METADATA_BUCKET-$edpAggregatorShortName",
              noiseType = ResultsFulfillerParams.NoiseParams.NoiseType.CONTINUOUS_GAUSSIAN,
            )
        }
      getDataWatcherResultFulfillerParamsConfig(
        blobPrefix = "file:///$REQUISITION_STORAGE_PREFIX",
        edpResultFulfillerConfigs = resultsFulfillerParamsMap,
      )
    }

    dataWatcher =
      DataWatcher(workItemsClient, watchedPaths, idTokenProvider = TestIdTokenProvider())

    val subscribingStorageClient = DataWatcherSubscribingStorageClient(storageClient, "file:///")
    subscribingStorageClient.subscribe(dataWatcher)
    kmsClients =
      edpResourceNameMap.toList().associate { (edpAggregatorShortName, edpResourceName) ->
        edpResourceName to kmsClient
      }
    edpResourceNameMap.toList().forEach { (edpAggregatorShortName, edpResourceName) ->
      val requisitionsClient: RequisitionsCoroutineStub =
        RequisitionsCoroutineStub(publicApiChannel).withPrincipalName(edpResourceName)

      val eventGroupsClient: EventGroupsCoroutineStub =
        EventGroupsCoroutineStub(publicApiChannel).withPrincipalName(edpResourceName)
      val edpPrivateKey = getDataProviderPrivateEncryptionKey(edpAggregatorShortName)

      val requisitionsValidator = RequisitionsValidator(edpPrivateKey)

      val requisitionGrouper =
        RequisitionGrouperByReportId(
          requisitionsValidator,
          edpResourceName,
          "$REQUISITION_STORAGE_PREFIX-$edpAggregatorShortName",
          requisitionMetadataClient,
          subscribingStorageClient,
          50,
          "$REQUISITION_STORAGE_PREFIX-$edpAggregatorShortName",
          throttler,
          eventGroupsClient,
          requisitionsClient,
        )

      val requisitionFetcher =
        RequisitionFetcher(
          requisitionsClient,
          subscribingStorageClient,
          edpResourceName,
          "$REQUISITION_STORAGE_PREFIX-$edpAggregatorShortName",
          requisitionGrouper,
        )
      backgroundScope.launch {
        while (true) {
          delay(1000)
          requisitionFetcher.fetchAndStoreRequisitions()
        }
      }
      val eventGroups = buildEventGroups(measurementConsumerData)
      eventGroupSync =
        EventGroupSync(edpResourceName, eventGroupsClient, eventGroups.asFlow(), throttler, 500)
      val mappedEventGroups: List<MappedEventGroup> = runBlocking { eventGroupSync.sync().toList() }
      logger.info("Received mappedEventGroups: $mappedEventGroups")
      runBlocking { writeImpressionData(mappedEventGroups, edpAggregatorShortName) }

      mappedEventGroups.forEach { mappedEventGroup ->
        val events =
          SyntheticDataGeneration.generateEvents(
            TestEvent.getDefaultInstance(),
            syntheticPopulationSpec,
            syntheticEventGroupMap.getValue(mappedEventGroup.eventGroupReferenceId),
          )

        val allDates: List<LocalDate> = events.map { it.localDate }.toList()
        val startDate = allDates.min()
        val endExclusive = allDates.max().plusDays(1)

        val eventGroupReferenceId = mappedEventGroup.eventGroupReferenceId
        val eventGroupPath =
          "model-line/${modelLineInfoMap.keys.first()}/event-group-reference-id/$eventGroupReferenceId"
        val impressionsMetadataBucket = "$IMPRESSIONS_METADATA_BUCKET-$edpAggregatorShortName"
        val modelLine = modelLineInfoMap.keys.first()

        val impressionsMetadata: List<ImpressionMetadata> =
          buildImpressionMetadataForDateRange(
            startInclusive = startDate,
            endExclusive = endExclusive,
            eventGroupPath = eventGroupPath,
            modelLine = modelLine,
            eventGroupReferenceId = eventGroupReferenceId,
            impressionsMetadataBucket = impressionsMetadataBucket,
          )
        logger.info("Storing impression metadata for edp: $edpResourceName")
        saveImpressionMetadata(impressionsMetadata, edpResourceName)
      }
    }
    backgroundScope.launch { resultFulfillerApp.run() }
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

  private suspend fun buildImpressionMetadataForDateRange(
    startInclusive: LocalDate,
    endExclusive: LocalDate,
    eventGroupPath: String,
    modelLine: String,
    eventGroupReferenceId: String,
    impressionsMetadataBucket: String,
    zoneId: ZoneId = ZONE_ID,
  ): List<ImpressionMetadata> {

    fun dailyInterval(day: LocalDate): Interval = interval {
      val start = day.atStartOfDay(zoneId).toInstant()
      val end = day.atTime(23, 59, 59).atZone(zoneId).toInstant()
      startTime = timestamp { seconds = start.epochSecond }
      endTime = timestamp { seconds = end.epochSecond }
    }

    val out = mutableListOf<ImpressionMetadata>()
    var day = startInclusive
    while (day.isBefore(endExclusive)) {
      val ds = day.toString()

      val impressionMetadataBlobKey = "ds/$ds/$eventGroupPath/metadata.binpb"

      val impressionsFileUri = "file:///$impressionsMetadataBucket/$impressionMetadataBlobKey"
      val perDayInterval = dailyInterval(day)

      val impressionMetadata = impressionMetadata {
        blobUri = impressionsFileUri
        blobTypeUrl = BLOB_TYPE_URL
        this.eventGroupReferenceId = eventGroupReferenceId
        this.modelLine = modelLine
        interval = perDayInterval
      }
      logger.info("Impression metadata object: $impressionMetadata")
      out += impressionMetadata
      day = day.plusDays(1)
    }
    return out
  }

  private suspend fun saveImpressionMetadata(
    impressionMetadataList: List<ImpressionMetadata>,
    dataProviderName: String,
  ) {
    val createImpressionMetadataRequests: MutableList<CreateImpressionMetadataRequest> =
      mutableListOf()
    impressionMetadataList.forEach {
      createImpressionMetadataRequests.add(
        createImpressionMetadataRequest {
          parent = dataProviderName
          this.impressionMetadata = it
          requestId = UUID.randomUUID().toString()
        }
      )
    }
    try {
      throttler.onReady {
        createImpressionMetadataRequests.forEach {
          impressionMetadataClient.createImpressionMetadata(it)
        }
      }
    } catch (e: StatusException) {
      throw Exception("Error creating Impressions Metadata", e)
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

  private suspend fun writeImpressionData(
    mappedEventGroups: List<MappedEventGroup>,
    edpAggregatorShortName: String,
  ) {
    withContext(Dispatchers.IO) {
      Files.createDirectories(storagePath.resolve("$IMPRESSIONS_BUCKET-$edpAggregatorShortName"))
      Files.createDirectories(
        storagePath.resolve("$IMPRESSIONS_METADATA_BUCKET-$edpAggregatorShortName")
      )
    }

    mappedEventGroups.forEach { mappedEventGroup ->
      val events: Sequence<LabeledEventDateShard<TestEvent>> =
        SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          syntheticPopulationSpec,
          syntheticEventGroupMap.getValue(mappedEventGroup.eventGroupReferenceId),
        )
      val modelLineName = modelLineInfoMap.keys.first()
      val impressionWriter =
        ImpressionsWriter(
          mappedEventGroup.eventGroupReferenceId,
          "model-line/$modelLineName/event-group-reference-id/${mappedEventGroup.eventGroupReferenceId}",
          kekUri,
          kmsClient,
          "$IMPRESSIONS_BUCKET-$edpAggregatorShortName",
          "$IMPRESSIONS_METADATA_BUCKET-$edpAggregatorShortName",
          storagePath.toFile(),
          "file:///",
        )
      impressionWriter.writeLabeledImpressionData(events, "some-model-line", null)
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
    private const val BLOB_TYPE_URL =
      "type.googleapis.com/wfa.measurement.securecomputation.impressions.BlobDetails"
    private const val IMPRESSIONS_BUCKET = "impression-bucket"
    private const val IMPRESSIONS_METADATA_BUCKET = "impression-metadata-bucket"
    private const val REQUISITION_STORAGE_PREFIX = "requisition-storage-prefix"
    private val ZONE_ID = ZoneId.of("UTC")

    // TODO: Lookup/Create an entry in the metadata store
    fun createGroupedRequisitionId(groupedRequisition: GroupedRequisitions): String {
      return groupedRequisition.requisitionsList
        .map { it.requisition.unpack(Requisition::class.java).name }
        .sorted()
        .first()
    }
  }
}
