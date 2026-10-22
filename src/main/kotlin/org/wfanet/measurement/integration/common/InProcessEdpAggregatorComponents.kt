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

import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.Struct
import com.google.protobuf.timestamp
import com.google.protobuf.util.JsonFormat
import com.google.type.Interval
import com.google.type.interval
import io.grpc.Channel
import io.grpc.StatusException
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.ClientAccountsGrpcKt.ClientAccountsCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt.filter as listEventGroupsFilter
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.UnlinkedClientAccountsGrpcKt.UnlinkedClientAccountsCoroutineStub
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.TestEvent
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.replaceDataProviderCapabilitiesRequest
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.throttler.testing.FakeThrottler
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.client.LabeledImpressionWriter
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityBlobs
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySync
import org.wfanet.measurement.edpaggregator.eventgroups.EventGroupSync
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.MediaType
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.entityKey as edpaEntityKey
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata as eventGroupMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.MappedEventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroups
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionFetcher
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionGrouperByReportId
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionsValidator
import org.wfanet.measurement.edpaggregator.requisitionfetcher.SecureComputationRequisitionWorkItemDispatcher
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ModelLineInfo
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ResultsFulfillerApp
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ResultsFulfillerMetrics
import org.wfanet.measurement.edpaggregator.resultsfulfiller.TrusTeeConfig
import org.wfanet.measurement.edpaggregator.resultsfulfiller.testing.TestRequisitionStubFactory
import org.wfanet.measurement.edpaggregator.v1alpha.CreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpressionKt
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.createImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.entityKey
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.labeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.integration.deploy.gcloud.SecureComputationServicesProviderRule
import org.wfanet.measurement.loadtest.dataprovider.EntityKey
import org.wfanet.measurement.loadtest.dataprovider.EntityKeyedLabeledEventDateShard
import org.wfanet.measurement.loadtest.dataprovider.EntityKeysWithLabeledEvents
import org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration
import org.wfanet.measurement.loadtest.edpaggregator.testing.ImpressionsWriter
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.loadtest.resourcesetup.Resources.Resource
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.deploy.gcloud.publisher.GoogleWorkItemPublisher
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.InternalApiServices as InternalSecureComputationApiServices
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.storage.BlobMetadataStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

class InProcessEdpAggregatorComponents(
  secureComputationDatabaseAdmin: SpannerDatabaseAdmin,
  private val storagePath: Path,
  private val pubSubClient: GooglePubSubEmulatorClient,
  private val populationSpec: PopulationSpec,
  private val eventGroupConfigsByEdp: Map<String, Map<String, EventGroupConfig>>,
  private val modelLineInfoMap: Map<String, ModelLineInfo>,
  private val externalKmsClient: FakeKmsClient,
  private val eventGroupReferenceIdEntityKeyMapping:
    ResultsFulfillerParams.EventGroupReferenceIdEntityKeyMapping?,
  private val wholeBlobEntityTypes: Set<String>,
) : TestRule {
  private val modelLineName: String by lazy { requireSingleModelLineName(modelLineInfoMap.keys) }

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

  private lateinit var eventGroupSync: EventGroupSync

  private val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"

  private val kmsClient: FakeKmsClient
    get() = externalKmsClient

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
      trusTeeConfigs =
        kmsClients.mapValues { (_, kmsClient) ->
          TrusTeeConfig(
            kmsClient = kmsClient,
            workloadIdentityProvider = "test-wip",
            impersonatedServiceAccount = "test-sa@example.com",
            awsKmsParams = null,
          )
        },
      requisitionMetadataStub = requisitionMetadataClient,
      impressionMetadataStub = impressionMetadataClient,
      requisitionStubFactory = requisitionStubFactory,
      getImpressionsMetadataStorageConfig = getStorageConfig,
      getImpressionsStorageConfig = getStorageConfig,
      getRequisitionsStorageConfig = getStorageConfig,
      modelLineInfoMap = modelLineInfoMap,
      metrics = ResultsFulfillerMetrics.create(),
      requisitionsThrottler = FakeThrottler(),
      kingdomThrottler = FakeThrottler(),
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
    edpCapabilities: Map<String, DataProvider.Capabilities>,
    duchyMap: Map<String, Channel>,
    edpNoise: Map<String, ResultsFulfillerParams.NoiseParams.NoiseType>,
    edpMultiPartyNoiseTypes: Map<String, List<ResultsFulfillerParams.NoiseParams.NoiseType>>,
    edpsWithDeferredUploads: Set<String>,
    resultMinimumThresholdsByEdp: Map<String, ResultsFulfillerParams.KAnonymityParams> = emptyMap(),
  ) = runBlocking {
    require(edpNoise.keys == edpCapabilities.keys) {
      "edpNoise keys ${edpNoise.keys} must match edpCapabilities keys ${edpCapabilities.keys}"
    }
    require(resultMinimumThresholdsByEdp.keys.all { it in edpCapabilities }) {
      "resultMinimumThresholdsByEdp keys must identify configured EDPs"
    }
    require(edpsWithDeferredUploads.all { it in edpCapabilities }) {
      "edpsWithDeferredUploads must identify configured EDPs"
    }
    publicApiChannel = kingdomChannel
    duchyChannelMap = duchyMap
    edpResourceNameMap =
      edpCapabilities.keys.associateWith { edpAggregatorShortName ->
        edpDisplayNameToResourceMap.getValue(edpAggregatorShortName).name
      }
    edpResourceNameMap.toList().forEach { (edpAggregatorShortName, edpResourceName) ->
      val dataProvidersStub: DataProvidersCoroutineStub =
        DataProvidersCoroutineStub(publicApiChannel).withPrincipalName(edpResourceName)
      dataProvidersStub.replaceDataProviderCapabilities(
        replaceDataProviderCapabilitiesRequest {
          name = edpResourceName
          capabilities = edpCapabilities.getValue(edpAggregatorShortName)
        }
      )
    }
    val resultsFulfillerParamsMap: Map<String, ResultsFulfillerParams> =
      edpResourceNameMap.toList().associate { (edpAggregatorShortName, edpResourceName) ->
        edpAggregatorShortName to
          getResultsFulfillerParams(
            edpAggregatorShortName,
            edpResourceName,
            DataProviderCertificateKey.fromName(
              edpDisplayNameToResourceMap.getValue(edpAggregatorShortName).dataProvider.certificate
            )!!,
            "file:///$IMPRESSIONS_METADATA_BUCKET-$edpAggregatorShortName",
            noiseType = edpNoise.getValue(edpAggregatorShortName),
            supportedMultiPartyNoiseTypes =
              edpMultiPartyNoiseTypes.getOrDefault(edpAggregatorShortName, emptyList()),
            resultMinimumThresholds = resultMinimumThresholdsByEdp[edpAggregatorShortName],
            eventGroupReferenceIdEntityKeyMapping = eventGroupReferenceIdEntityKeyMapping,
            wholeBlobEntityTypes = wholeBlobEntityTypes,
          )
      }
    kmsClients =
      edpResourceNameMap.toList().associate { (edpAggregatorShortName, edpResourceName) ->
        edpResourceName to kmsClient
      }
    edpResourceNameMap.toList().forEach { (edpAggregatorShortName, edpResourceName) ->
      val requisitionsClient: RequisitionsCoroutineStub =
        RequisitionsCoroutineStub(publicApiChannel).withPrincipalName(edpResourceName)

      val eventGroupsClient: EventGroupsCoroutineStub =
        EventGroupsCoroutineStub(publicApiChannel).withPrincipalName(edpResourceName)
      val clientAccountsClient: ClientAccountsCoroutineStub =
        ClientAccountsCoroutineStub(publicApiChannel).withPrincipalName(edpResourceName)
      val unlinkedClientAccountsClient: UnlinkedClientAccountsCoroutineStub =
        UnlinkedClientAccountsCoroutineStub(publicApiChannel).withPrincipalName(edpResourceName)
      val edpPrivateKey = getDataProviderPrivateEncryptionKey(edpAggregatorShortName)

      val requisitionsValidator = RequisitionsValidator(edpPrivateKey)

      val requisitionGrouper =
        RequisitionGrouperByReportId(
          requisitionValidator = requisitionsValidator,
          requisitionsClient = requisitionsClient,
          eventGroupsClient = eventGroupsClient,
          kingdomMutationThrottler = throttler,
          kingdomEventGroupThrottler = throttler,
        )

      val requisitionFetcher =
        RequisitionFetcher(
          requisitionsStub = requisitionsClient,
          requisitionMetadataStub = requisitionMetadataClient,
          storageClient = storageClient,
          dataProviderName = edpResourceName,
          storagePathPrefix = "$REQUISITION_STORAGE_PREFIX-$edpAggregatorShortName",
          directStoragePathPrefix = "$REQUISITION_STORAGE_PREFIX-v2-$edpAggregatorShortName",
          blobUriPrefix = "file://",
          requisitionValidator = requisitionsValidator,
          requisitionGrouper = requisitionGrouper,
          metadataThrottler = throttler,
          workItemDispatcher =
            SecureComputationRequisitionWorkItemDispatcher(
              workItemsStub = workItemsClient,
              queue = FULFILLER_TOPIC_ID,
              resultsFulfillerParams = resultsFulfillerParamsMap.getValue(edpAggregatorShortName),
              controlPlaneThrottler = FakeThrottler(),
            ),
          requisitionRefusalDuration = RequisitionFetcher.DEFAULT_REQUISITION_REFUSAL_DURATION,
          clock = Clock.systemUTC(),
          responsePageSize = 50,
        )
      backgroundScope.launch {
        while (true) {
          delay(1000)
          requisitionFetcher.fetchAndStoreRequisitions()
        }
      }
      if (edpAggregatorShortName in edpsWithDeferredUploads) {
        return@forEach
      }
      val eventGroups = buildEventGroups(measurementConsumerData, edpAggregatorShortName)
      eventGroupSync =
        EventGroupSync(
          edpResourceName,
          eventGroupsClient,
          clientAccountsClient,
          unlinkedClientAccountsClient,
          eventGroups.asFlow(),
          throttler,
          entityKeyTypes = emptyList(),
          listEventGroupPageSize = 500,
          eventGroupReferenceIdEntityKeyMapping = null,
        )
      val mappedEventGroups: List<MappedEventGroup> = runBlocking { eventGroupSync.sync().toList() }
      logger.info("Received mappedEventGroups: $mappedEventGroups")
      runBlocking { writeImpressionData(mappedEventGroups, edpAggregatorShortName) }

      val originalConfigs: Map<String, EventGroupConfig> =
        eventGroupConfigsByEdp.getValue(edpAggregatorShortName)
      val configByRefId: Map<String, EventGroupConfig> =
        resolveSpecsByReferenceId(edpAggregatorShortName)

      // Map each derived ref ID back to the original ref ID from the config.
      val originalRefIdByDerivedRefId: Map<String, String> =
        originalConfigs
          .flatMap { (originalRefId, config) ->
            when (config) {
              is EventGroupConfig.LegacySpec -> listOf(originalRefId to originalRefId)
              is EventGroupConfig.MultiEntityKey ->
                config.entityKeySpecs.flatMap { entityKeySpec ->
                  entityKeySpec.allEntityKeys.map { entityKey ->
                    "${entityKey.entityType}/${entityKey.entityId}" to originalRefId
                  }
                }
            }
          }
          .toMap()

      // Create impression metadata in a single pass over mapped event groups.
      val processedOriginalRefIds = mutableSetOf<String>()
      val impressionsMetadataBucket = "$IMPRESSIONS_METADATA_BUCKET-$edpAggregatorShortName"
      mappedEventGroups.forEach { mappedEventGroup ->
        val mappedRefId = mappedEventGroup.eventGroupReferenceId
        val resolvedConfig = configByRefId[mappedRefId] ?: return@forEach

        when (resolvedConfig) {
          is EventGroupConfig.MultiEntityKey -> {
            val originalRefId = originalRefIdByDerivedRefId.getValue(mappedRefId)
            if (!processedOriginalRefIds.add(originalRefId)) return@forEach
            val originalConfig =
              originalConfigs.getValue(originalRefId) as EventGroupConfig.MultiEntityKey
            val allDates: List<LocalDate> =
              originalConfig.entityKeySpecs
                .flatMap { entityKeySpec ->
                  SyntheticDataGeneration.generateEvents(
                      TestEvent.getDefaultInstance(),
                      populationSpec,
                      entityKeySpec.spec,
                    )
                    .map { it.localDate }
                    .toList()
                }
                .distinct()
            val startDate = allDates.min()
            val endExclusive = allDates.max().plusDays(1)
            val eventGroupPath = "model-line/$modelLineName/event-group-reference-id/$originalRefId"
            val legacyEndDate = originalConfig.legacyEndDate
            val legacyMetadata: List<ImpressionMetadata> =
              if (legacyEndDate == null) {
                emptyList()
              } else {
                val legacyDates: List<LocalDate> =
                  SyntheticDataGeneration.generateEvents(
                      TestEvent.getDefaultInstance(),
                      populationSpec,
                      legacyEndDate.legacySpec.spec,
                    )
                    .map { it.localDate }
                    .toList()
                val legacyStartDate = legacyDates.min()
                require(legacyEndDate.date > legacyStartDate && legacyEndDate.date < endExclusive) {
                  "Legacy end date ${legacyEndDate.date} for $originalRefId must be after " +
                    "$legacyStartDate and before $endExclusive"
                }
                buildImpressionMetadataForDateRange(
                  startInclusive = legacyStartDate,
                  endExclusive = minOf(legacyEndDate.date, legacyDates.max().plusDays(1)),
                  eventGroupPath = eventGroupPath,
                  modelLine = modelLineName,
                  eventGroupReferenceId = originalRefId,
                  impressionsMetadataBucket = impressionsMetadataBucket,
                )
              }
            val entityKeys: List<org.wfanet.measurement.edpaggregator.v1alpha.EntityKey> =
              originalConfig.entityKeySpecs
                .flatMap { it.allEntityKeys }
                .distinct()
                .map { key ->
                  entityKey {
                    entityType = key.entityType
                    entityId = key.entityId
                  }
                }
            val impressionsMetadata: List<ImpressionMetadata> =
              legacyMetadata +
                buildImpressionMetadataForDateRange(
                  startInclusive = maxOf(startDate, legacyEndDate?.date ?: startDate),
                  endExclusive = endExclusive,
                  eventGroupPath = eventGroupPath,
                  modelLine = modelLineName,
                  eventGroupReferenceId = if (legacyEndDate == null) originalRefId else "",
                  impressionsMetadataBucket = impressionsMetadataBucket,
                  entityKeys = entityKeys,
                )
            logger.info(
              "Storing impression metadata for edp: $edpResourceName (shared ref=$originalRefId)"
            )
            saveImpressionMetadata(impressionsMetadata, edpResourceName)
          }
          is EventGroupConfig.LegacySpec -> {
            val allDates: List<LocalDate> =
              SyntheticDataGeneration.generateEvents(
                  TestEvent.getDefaultInstance(),
                  populationSpec,
                  resolvedConfig.spec,
                )
                .map { it.localDate }
                .toList()
            val startDate = allDates.min()
            val endExclusive = allDates.max().plusDays(1)
            val eventGroupPath = "model-line/$modelLineName/event-group-reference-id/$mappedRefId"
            val impressionsMetadata: List<ImpressionMetadata> =
              buildImpressionMetadataForDateRange(
                startInclusive = startDate,
                endExclusive = endExclusive,
                eventGroupPath = eventGroupPath,
                modelLine = modelLineName,
                eventGroupReferenceId = mappedRefId,
                impressionsMetadataBucket = impressionsMetadataBucket,
              )
            logger.info("Storing impression metadata for edp: $edpResourceName")
            saveImpressionMetadata(impressionsMetadata, edpResourceName)
          }
        }
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
    entityKeys: List<org.wfanet.measurement.edpaggregator.v1alpha.EntityKey> = emptyList(),
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
        this.entityKeys += entityKeys
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

  private fun buildEventGroups(
    measurementConsumerData: MeasurementConsumerData,
    edpAggregatorShortName: String,
  ): List<EventGroup> {
    return eventGroupConfigsByEdp.getValue(edpAggregatorShortName).flatMap {
      (eventGroupReferenceId, config) ->
      when (config) {
        is EventGroupConfig.LegacySpec ->
          buildEventGroupsForSpec(
            spec = config.spec,
            entityKey = null,
            eventGroupReferenceId = eventGroupReferenceId,
            entityMetadata = null,
            measurementConsumerData = measurementConsumerData,
          )
        is EventGroupConfig.MultiEntityKey ->
          config.entityKeySpecs
            .flatMap { entityKeySpec ->
              entityKeySpec.allEntityKeys.flatMap { entityKey ->
                buildEventGroupsForSpec(
                  spec = entityKeySpec.spec,
                  entityKey = entityKey,
                  eventGroupReferenceId = "${entityKey.entityType}/${entityKey.entityId}",
                  entityMetadata = entityKeySpec.entityMetadata,
                  measurementConsumerData = measurementConsumerData,
                )
              }
            }
            .distinct()
      }
    }
  }

  private fun buildEventGroupsForSpec(
    spec: SyntheticEventGroupSpec,
    entityKey: EntityKey?,
    eventGroupReferenceId: String,
    entityMetadata: Struct?,
    measurementConsumerData: MeasurementConsumerData,
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
        if (eventGroupReferenceId.isNotEmpty()) {
          this.eventGroupReferenceId = eventGroupReferenceId
        }
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
          if (entityMetadata != null) {
            this.entityMetadata = entityMetadata
          }
        }
        if (entityKey != null) {
          this.entityKey = edpaEntityKey {
            entityType = entityKey.entityType
            entityId = entityKey.entityId
          }
        }
        mediaTypes += MediaType.valueOf("VIDEO")
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

    val originalConfigs: Map<String, EventGroupConfig> =
      eventGroupConfigsByEdp.getValue(edpAggregatorShortName)

    // Write multi-entity-key configs as shared blobs under their original ref ID.
    val writtenMultiEntityKeyRefIds = mutableSetOf<String>()
    for ((refId, config) in originalConfigs) {

      when (config) {
        is EventGroupConfig.MultiEntityKey -> {
          val legacyEndDate = config.legacyEndDate
          writtenMultiEntityKeyRefIds += refId
          for (entityKeySpec in config.entityKeySpecs) {
            for (entityKey in entityKeySpec.allEntityKeys) {
              writtenMultiEntityKeyRefIds += "${entityKey.entityType}/${entityKey.entityId}"
            }
          }
          if (legacyEndDate != null) {
            val legacyEvents =
              SyntheticDataGeneration.generateEvents(
                  TestEvent.getDefaultInstance(),
                  populationSpec,
                  legacyEndDate.legacySpec.spec,
                )
                .filter { it.localDate < legacyEndDate.date }
                .map { shard ->
                  EntityKeyedLabeledEventDateShard(
                    shard.localDate,
                    sequenceOf(EntityKeysWithLabeledEvents(emptyList(), shard.labeledEvents)),
                  )
                }
            ImpressionsWriter(
                refId,
                "model-line/$modelLineName/event-group-reference-id/$refId",
                kekUri,
                kmsClient,
                "$IMPRESSIONS_BUCKET-$edpAggregatorShortName",
                "$IMPRESSIONS_METADATA_BUCKET-$edpAggregatorShortName",
                storagePath.toFile(),
                "file:///",
                includeEventGroupReferenceIdInImpressions = false,
              )
              .writeLabeledImpressionData(legacyEvents, modelLineName, null)
          }
          val impressionWriter =
            ImpressionsWriter(
              if (legacyEndDate == null) refId else "",
              "model-line/$modelLineName/event-group-reference-id/$refId",
              kekUri,
              kmsClient,
              "$IMPRESSIONS_BUCKET-$edpAggregatorShortName",
              "$IMPRESSIONS_METADATA_BUCKET-$edpAggregatorShortName",
              storagePath.toFile(),
              "file:///",
              includeEventGroupReferenceIdInImpressions = false,
            )
          val shardsByDate =
            mutableMapOf<LocalDate, MutableList<EntityKeysWithLabeledEvents<TestEvent>>>()
          for (entityKeySpec in config.entityKeySpecs) {
            val events =
              SyntheticDataGeneration.generateEvents(
                TestEvent.getDefaultInstance(),
                populationSpec,
                entityKeySpec.spec,
              )
            for (shard in events) {
              if (legacyEndDate != null && shard.localDate < legacyEndDate.date) continue
              shardsByDate
                .getOrPut(shard.localDate) { mutableListOf() }
                .add(EntityKeysWithLabeledEvents(entityKeySpec.allEntityKeys, shard.labeledEvents))
            }
          }
          val entityKeyedEvents: Sequence<EntityKeyedLabeledEventDateShard<TestEvent>> =
            shardsByDate.entries
              .sortedBy { it.key }
              .asSequence()
              .map { (date, groups) -> EntityKeyedLabeledEventDateShard(date, groups.asSequence()) }
          impressionWriter.writeLabeledImpressionData(entityKeyedEvents, modelLineName, null)
        }
        is EventGroupConfig.LegacySpec -> {}
      }
    }

    // Write legacy (non-multi-entity-key) configs as before.
    val configByRefId: Map<String, EventGroupConfig> =
      resolveSpecsByReferenceId(edpAggregatorShortName)
    mappedEventGroups.forEach { mappedEventGroup ->
      val refId = mappedEventGroup.eventGroupReferenceId
      if (refId in writtenMultiEntityKeyRefIds) return@forEach
      val resolvedConfig = configByRefId[refId] ?: return@forEach
      val entityKey =
        when (resolvedConfig) {
          is EventGroupConfig.LegacySpec -> null
          is EventGroupConfig.MultiEntityKey -> resolvedConfig.entityKeySpecs.single().entityKey
        }
      val spec =
        when (resolvedConfig) {
          is EventGroupConfig.LegacySpec -> resolvedConfig.spec
          is EventGroupConfig.MultiEntityKey -> resolvedConfig.entityKeySpecs.single().spec
        }
      val impressionWriter =
        ImpressionsWriter(
          refId,
          "model-line/$modelLineName/event-group-reference-id/$refId",
          kekUri,
          kmsClient,
          "$IMPRESSIONS_BUCKET-$edpAggregatorShortName",
          "$IMPRESSIONS_METADATA_BUCKET-$edpAggregatorShortName",
          storagePath.toFile(),
          "file:///",
          includeEventGroupReferenceIdInImpressions = false,
        )
      val entityKeyedEvents: Sequence<EntityKeyedLabeledEventDateShard<TestEvent>> =
        SyntheticDataGeneration.generateEvents(TestEvent.getDefaultInstance(), populationSpec, spec)
          .map { shard ->
            EntityKeyedLabeledEventDateShard(
              shard.localDate,
              sequenceOf(EntityKeysWithLabeledEvents(listOfNotNull(entityKey), shard.labeledEvents)),
            )
          }
      impressionWriter.writeLabeledImpressionData(entityKeyedEvents, modelLineName, null)
    }
  }

  private fun resolveSpecsByReferenceId(
    edpAggregatorShortName: String
  ): Map<String, EventGroupConfig> {
    return eventGroupConfigsByEdp
      .getValue(edpAggregatorShortName)
      .flatMap { (refId, config) ->
        when (config) {
          is EventGroupConfig.LegacySpec -> listOf(refId to config)
          is EventGroupConfig.MultiEntityKey ->
            config.entityKeySpecs.flatMap { entityKeySpec ->
              entityKeySpec.allEntityKeys.map { entityKey ->
                "${entityKey.entityType}/${entityKey.entityId}" to
                  config.copy(entityKeySpecs = listOf(entityKeySpec))
              }
            }
        }
      }
      .toMap()
  }

  /**
   * Exports the entity-keyed test impressions for [edpAggregatorShortName] using the public
   * [LabeledImpressionWriter].
   *
   * @return the metadata URIs that can be passed to the VID-labeled impression verifier
   */
  suspend fun exportLabeledImpressions(edpAggregatorShortName: String): List<String> {
    val writer = LabeledImpressionWriter.forFileSystem(kmsClient, storagePath.toFile())
    val metadataUris = mutableListOf<String>()
    val impressionsMetadataBucket = "$IMPRESSIONS_METADATA_BUCKET-$edpAggregatorShortName"
    val configs = eventGroupConfigsByEdp.getValue(edpAggregatorShortName)
    withContext(Dispatchers.IO) {
      Files.createDirectories(storagePath.resolve(impressionsMetadataBucket))
    }

    val modelLineId = checkNotNull(ModelLineKey.fromName(modelLineName)).modelLineId
    for ((configIndex, config) in configs.values.withIndex()) {
      if (config !is EventGroupConfig.MultiEntityKey) {
        continue
      }
      val groupsByDate =
        mutableMapOf<LocalDate, MutableList<EntityKeysWithLabeledEvents<TestEvent>>>()
      for (entityKeySpec in config.entityKeySpecs) {
        val shards =
          SyntheticDataGeneration.generateEvents(
            TestEvent.getDefaultInstance(),
            populationSpec,
            entityKeySpec.spec,
          )
        for (shard in shards) {
          groupsByDate
            .getOrPut(shard.localDate) { mutableListOf() }
            .add(EntityKeysWithLabeledEvents(entityKeySpec.allEntityKeys, shard.labeledEvents))
        }
      }

      for ((date, groups) in groupsByDate.toSortedMap()) {
        val parentKey = "$ONBOARDING_PREFIX/model-line/$modelLineId/$date"
        val blobUri =
          "file:///$impressionsMetadataBucket/$parentKey/impressions-$configIndex.enc.recordio"
        val metadataUri =
          "file:///$impressionsMetadataBucket/$parentKey/metadata-$configIndex.binpb"
        val interval = interval {
          startTime = date.atStartOfDay(ZONE_ID).toInstant().toProtoTime()
          endTime = date.plusDays(1).atStartOfDay(ZONE_ID).toInstant().toProtoTime()
        }
        val impressions = flow {
          for (group in groups) {
            val entityKeys =
              group.entityKeys.map { entityKey ->
                LabeledImpressionKt.entityKey {
                  entityType = entityKey.entityType
                  entityId = entityKey.entityId
                }
              }
            for (event in group.labeledEvents) {
              emit(
                labeledImpression {
                  vid = event.vid
                  this.event = Any.pack(event.message)
                  eventTime = event.timestamp.toProtoTime()
                  this.entityKeys += entityKeys
                }
              )
            }
          }
        }
        writer.write(impressions, blobUri, metadataUri, kekUri, modelLineName, interval)
        metadataUris += metadataUri
      }
    }
    return metadataUris
  }

  /** Results of publishing uploaded impressions through data-availability discovery. */
  data class ImpressionPublicationResult(
    /** Sync outcome keyed by completion-marker URI. */
    val outcomes: Map<String, DataAvailabilitySync.Outcome>,
    /** Custom lifecycle timestamp keyed by object URI. */
    val customCreateTimes: Map<String, Instant>,
  )

  /**
   * Publishes each uploaded date's `done` marker and runs the data-availability discovery job.
   *
   * @return the sync outcomes and resulting custom lifecycle timestamps
   */
  suspend fun publishImpressions(
    edpAggregatorShortName: String,
    metadataUris: List<String>,
  ): ImpressionPublicationResult {
    val bucket = "$IMPRESSIONS_METADATA_BUCKET-$edpAggregatorShortName"
    val bucketStorage =
      MetadataStorageClient(FileSystemStorageClient(storagePath.resolve(bucket).toFile()))
    val doneKeys =
      metadataUris
        .map { metadataUri ->
          val blobUri = SelectedStorageClient.parseBlobUri(metadataUri)
          require(blobUri.bucket == bucket)
          "${blobUri.key.substringBeforeLast('/')}/done"
        }
        .distinct()
        .sorted()
    for (doneKey in doneKeys) {
      bucketStorage.writeBlob(doneKey, ByteString.EMPTY)
    }
    val dataProviderName = edpResourceNameMap.getValue(edpAggregatorShortName)
    val sync =
      DataAvailabilitySync(
        edpImpressionPath = ONBOARDING_PREFIX,
        storageClient = bucketStorage,
        dataProvidersStub =
          DataProvidersCoroutineStub(publicApiChannel).withPrincipalName(dataProviderName),
        impressionMetadataServiceStub = impressionMetadataClient,
        dataProviderName = dataProviderName,
        throttler = FakeThrottler(),
        impressionMetadataBatchSize = 100,
        modelLineMap = emptyMap(),
        errorIfGapsExist = true,
        publishEventGroupActivities = null,
      )
    val outcomes =
      doneKeys.associate { doneKey ->
        val doneUri = "file:///$bucket/$doneKey"
        val outcome = sync.sync(doneUri)
        if (outcome == DataAvailabilitySync.Outcome.PUBLISHED) {
          check(
            DataAvailabilityBlobs.isDataAvailabilityPublished(
              checkNotNull(bucketStorage.getBlob(doneKey))
            )
          ) {
            "Publication marker missing for $doneUri"
          }
        }
        doneUri to outcome
      }
    return ImpressionPublicationResult(
      outcomes,
      bucketStorage.snapshotCustomCreateTimes().mapKeys { (blobKey, _) ->
        "file:///$bucket/$blobKey"
      },
    )
  }

  /** Lists the impression metadata imported for the EDP through the public EDPA API. */
  suspend fun listImpressionMetadata(edpAggregatorShortName: String): List<ImpressionMetadata> {
    val metadata = mutableListOf<ImpressionMetadata>()
    var nextPageToken = ""
    do {
      val response =
        impressionMetadataClient.listImpressionMetadata(
          listImpressionMetadataRequest {
            parent = edpResourceNameMap.getValue(edpAggregatorShortName)
            pageSize = 100
            pageToken = nextPageToken
          }
        )
      metadata += response.impressionMetadataList
      nextPageToken = response.nextPageToken
    } while (nextPageToken.isNotEmpty())
    return metadata
  }

  /** Returns the EDP's availability intervals from the public Kingdom API. */
  suspend fun getDataAvailabilityIntervals(edpAggregatorShortName: String): Map<String, Interval> {
    val dataProviderName = edpResourceNameMap.getValue(edpAggregatorShortName)
    return DataProvidersCoroutineStub(publicApiChannel)
      .withPrincipalName(dataProviderName)
      .getDataProvider(getDataProviderRequest { name = dataProviderName })
      .dataAvailabilityIntervalsList
      .associate { it.key to it.value }
  }

  /** Adds in-process custom metadata to filesystem objects used by the discovery job. */
  private class MetadataStorageClient(private val delegate: StorageClient) :
    BlobMetadataStorageClient, StorageClient by delegate {
    private val metadataByKey = mutableMapOf<String, Map<String, String>>()
    private val customCreateTimes = mutableMapOf<String, Instant>()

    fun snapshotCustomCreateTimes(): Map<String, Instant> = customCreateTimes.toMap()

    override suspend fun updateBlobMetadata(
      blobKey: String,
      customCreateTime: Instant?,
      metadata: Map<String, String>,
    ) {
      checkNotNull(delegate.getBlob(blobKey)) { "Blob not found: $blobKey" }
      metadataByKey[blobKey] = metadataByKey.getOrDefault(blobKey, emptyMap()) + metadata
      if (customCreateTime != null) {
        customCreateTimes[blobKey] = customCreateTime
      }
    }

    override suspend fun getBlob(blobKey: String): StorageClient.Blob? =
      delegate.getBlob(blobKey)?.let { withMetadata(it) }

    override suspend fun listBlobs(prefix: String?): Flow<StorageClient.Blob> =
      delegate.listBlobs(prefix).map { withMetadata(it) }

    private fun withMetadata(blob: StorageClient.Blob): StorageClient.Blob =
      object : StorageClient.Blob by blob {
        override val metadata: Map<String, String>
          get() = metadataByKey.getOrDefault(blob.blobKey, emptyMap())
      }
  }

  /**
   * Writes the configured EventGroups for [edpAggregatorShortName] to an onboarding JSON export.
   *
   * @return the file containing the EventGroups that can be passed to the EventGroup verifier and
   *   reader
   */
  suspend fun exportEventGroups(
    edpAggregatorShortName: String,
    measurementConsumerData: MeasurementConsumerData,
  ): File =
    withContext(Dispatchers.IO) {
      val exportDirectory = storagePath.resolve("onboarding-event-groups")
      Files.createDirectories(exportDirectory)
      val exportFile = exportDirectory.resolve("$edpAggregatorShortName.json")
      val json =
        JsonFormat.printer()
          .print(
            eventGroups {
              eventGroups += buildEventGroups(measurementConsumerData, edpAggregatorShortName)
            }
          )
      Files.writeString(exportFile, json)
      exportFile.toFile()
    }

  /**
   * Synchronizes [sources] for [edpAggregatorShortName] into the running in-process Kingdom.
   *
   * @return the [MappedEventGroup]s emitted by [EventGroupSync]
   */
  fun syncEventGroups(
    edpAggregatorShortName: String,
    sources: List<EventGroup>,
    entityKeyTypes: List<String> = emptyList(),
  ): List<MappedEventGroup> = runBlocking {
    val edpResourceName = edpResourceNameMap.getValue(edpAggregatorShortName)
    val eventGroupsClient =
      EventGroupsCoroutineStub(publicApiChannel).withPrincipalName(edpResourceName)
    val clientAccountsClient =
      ClientAccountsCoroutineStub(publicApiChannel).withPrincipalName(edpResourceName)
    val unlinkedClientAccountsClient =
      UnlinkedClientAccountsCoroutineStub(publicApiChannel).withPrincipalName(edpResourceName)
    EventGroupSync(
        edpResourceName,
        eventGroupsClient,
        clientAccountsClient,
        unlinkedClientAccountsClient,
        sources.asFlow(),
        throttler,
        entityKeyTypes = entityKeyTypes,
        listEventGroupPageSize = 500,
        eventGroupReferenceIdEntityKeyMapping = null,
      )
      .sync()
      .toList()
  }

  /**
   * Lists the CMMS [CmmsEventGroup]s the given EDP owns on the in-process Kingdom, filtered to
   * [entityTypes], via the raw v2alpha `EventGroups.ListEventGroups` (not the Reporting API).
   *
   * [entityTypes] must enumerate every entity type the caller may have created: the Kingdom
   * defaults `entity_type_in` to `["campaign"]` when unset, so a non-campaign row (e.g.
   * `creative-id`) is hidden unless its type is listed here. Passing the caller's own type set
   * keeps this lister from silently under-counting as new entity types are exercised.
   *
   * Test-only seam for asserting the exact Kingdom row count and resource-name set, which is how
   * "no EventGroup explosion / no duplicate rows" is proven independent of any Reporting-side
   * filtering or de-duplication.
   */
  fun listCmmsEventGroups(
    edpAggregatorShortName: String,
    entityTypes: List<String>,
  ): List<CmmsEventGroup> = runBlocking {
    val edpResourceName = edpResourceNameMap.getValue(edpAggregatorShortName)
    val eventGroupsClient =
      EventGroupsCoroutineStub(publicApiChannel).withPrincipalName(edpResourceName)
    val eventGroups = mutableListOf<CmmsEventGroup>()
    var pageToken = ""
    while (true) {
      val response =
        throttler.onReady {
          eventGroupsClient.listEventGroups(
            listEventGroupsRequest {
              parent = edpResourceName
              pageSize = 1000
              this.pageToken = pageToken
              filter = listEventGroupsFilter { entityTypeIn += entityTypes }
            }
          )
        }
      eventGroups += response.eventGroupsList
      if (response.nextPageToken.isEmpty()) break
      pageToken = response.nextPageToken
    }
    eventGroups
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
    internal fun requireSingleModelLineName(modelLineNames: Set<String>): String {
      require(modelLineNames.size == 1) {
        "InProcessEdpAggregatorComponents supports exactly one model line, found: " +
          modelLineNames.sorted()
      }
      return modelLineNames.single()
    }

    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val BLOB_TYPE_URL =
      "type.googleapis.com/wfa.measurement.securecomputation.impressions.BlobDetails"
    private const val IMPRESSIONS_BUCKET = "impression-bucket"
    private const val IMPRESSIONS_METADATA_BUCKET = "impression-metadata-bucket"
    private const val REQUISITION_STORAGE_PREFIX = "requisition-storage-prefix"
    private const val ONBOARDING_PREFIX = "onboarding"
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
