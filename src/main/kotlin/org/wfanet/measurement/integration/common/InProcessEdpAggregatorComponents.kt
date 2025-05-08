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
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.unpack
import com.google.protobuf.timestamp
import com.google.type.interval
import io.grpc.Channel
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneId
import java.util.Timer
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMap
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.config.securecomputation.WatchedPath
import org.wfanet.measurement.consent.client.dataprovider.decryptRequisitionSpec
import org.wfanet.measurement.edpaggregator.eventgroups.EventGroupSync
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.MappedEventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup.MediaType
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionFetcher
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ResultsFulfillerTestApp
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.labeledImpression
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.loadtest.config.TestIdentifiers.SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX
import org.wfanet.measurement.loadtest.dataprovider.EventQuery
import org.wfanet.measurement.loadtest.dataprovider.SyntheticGeneratorEventQuery
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.loadtest.resourcesetup.Resources
import org.wfanet.measurement.loadtest.resourcesetup.Resources.Resource
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.securecomputation.datawatcher.testing.DataWatcherSubscribingStorageClient
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.InternalApiServices
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.api.v2alpha.EncryptedMessage
import org.wfanet.measurement.consent.client.dataprovider.encryptMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupKt as CmmsEventGroupKt
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata as eventGroupMetadata
import org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration

class InProcessEdpAggregatorComponents(
  private val internalServicesRule: ProviderRule<InternalApiServices>,
  private val pubSubClient: GooglePubSubEmulatorClient,
  private val storagePath: Path,
  private val syntheticPopulationSpec: SyntheticPopulationSpec =
    SyntheticGenerationSpecs.SYNTHETIC_POPULATION_SPEC_SMALL,
  private val syntheticEventGroupSpecs: List<SyntheticEventGroupSpec> =
    SyntheticGenerationSpecs.SYNTHETIC_DATA_SPECS_SMALL,
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

  private val resultFulfillerApp by lazy {
    val subscriber = Subscriber(PROJECT_ID, pubSubClient)
    ResultsFulfillerTestApp(
      parser = WorkItem.parser(),
      subscriptionId = SUBSCRIPTION_ID,
      workItemsClient = workItemsClient,
      workItemAttemptsClient =
        WorkItemAttemptsCoroutineStub(secureComputationPublicApi.publicApiChannel),
      queueSubscriber = subscriber,
      cmmsChannel = publicApiChannel,
      fileSystemRootDirectory = storagePath.toFile(),
      kmsClient = kmsClient,
    )
  }

  val ruleChain: TestRule by lazy {
    chainRulesSequentially(internalServicesRule, secureComputationPublicApi)
  }

  private lateinit var edpDisplayNameToResourceMap: Map<String, Resources.Resource>
  lateinit var externalEventGroups: List<CmmsEventGroup>

  private suspend fun createAllResources() {}

  fun getDataProviderResourceNames(): List<String> {
    return edpDisplayNameToResourceMap.values.map { it.name }
  }

  lateinit var requisitionFetcherTimer: Timer
  lateinit var eventGroupSyncTimer: Timer
  private val loggingName = javaClass.simpleName
  private val backgroundScope =
    CoroutineScope(
      Dispatchers.Default +
        CoroutineName(loggingName) +
        CoroutineExceptionHandler { _, e ->
          logger.log(Level.SEVERE, e) { "Error in $loggingName" }
        }
    )
  private var readyToFulfillRequisitions: Boolean = false

  fun startDaemons(
    kingdomChannel: Channel,
    measurementConsumerData: MeasurementConsumerData,
    edpDisplayNameToResourceMap: Map<String, Resource>,
  ) = runBlocking {
    // Create all resources
    createAllResources()
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
          eventGroupReferenceId = "sim-eg-reference-id-1-edp-0"
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
    val cmmsEventGroup =
      mappedEventGroups.filter { it.eventGroupReferenceId == "sim-eg-reference-id-1-edp-0" }.single()
    // Wait for requisitions to be created
    backgroundScope.launch {
      while (!readyToFulfillRequisitions) {
        readyToFulfillRequisitions = runBlocking {
          writeImpressionData(
            cmmsEventGroup =
              checkNotNull(CmmsEventGroupKey.fromName(cmmsEventGroup.eventGroupResource)),
            privateEncryptionKey = getDataProviderPrivateEncryptionKey(edpShortName),
            syntheticDataSpec = syntheticEventGroupSpecs[0],
          )
        }
        delay(1000)
      }
      resultFulfillerApp.run()
    }
  }

  private suspend fun buildEventGroupSpecs(
    requisitionSpec: RequisitionSpec
  ): List<EventQuery.EventGroupSpec> {
    // TODO(@SanjayVas): Cache EventGroups.
    return requisitionSpec.events.eventGroupsList.map {
      val eventGroup = eventGroupsClient.getEventGroup(getEventGroupRequest { name = it.key })

      if (!eventGroup.eventGroupReferenceId.startsWith(SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX)) {
        throw Exception("EventGroup ${it.key} not supported by this simulator")
      }

      EventQuery.EventGroupSpec(eventGroup, it.value)
    }
  }

  private suspend fun writeImpressionData(
    cmmsEventGroup: CmmsEventGroupKey,
    privateEncryptionKey: PrivateKeyHandle,
    syntheticDataSpec: SyntheticEventGroupSpec,
  ): Boolean {
    Files.createDirectories(storagePath.resolve(IMPRESSIONS_BUCKET))
    val files =
      try {
        Files.walk(storagePath.resolve(REQUISITION_STORAGE_PREFIX))
      } catch (e: NoSuchFileException) {
        return false
      }
    val requisitionSpecs: Flow<RequisitionSpec> =
      files
        .filter { Files.isRegularFile(it) }
        .map { it.toString() }
        .iterator()
        .asFlow()
        .map { fileName: String ->
          val requisitionBytes: ByteString = storageClient.getBlob(fileName)!!.read().flatten()

          val requisition = Any.parseFrom(requisitionBytes).unpack(Requisition::class.java)
          val signedRequisitionSpec: SignedMessage =
            decryptRequisitionSpec(requisition.encryptedRequisitionSpec, privateEncryptionKey)
          signedRequisitionSpec.unpack()
        }

    val eventGroupSpecs: List<EventQuery.EventGroupSpec> =
      requisitionSpecs.toList().flatMap { it: RequisitionSpec -> buildEventGroupSpecs(it) }
    val eventQuery =
      object : SyntheticGeneratorEventQuery(syntheticPopulationSpec, TestEvent.getDescriptor()) {
        override fun getSyntheticDataSpec(eventGroup: CmmsEventGroup) = syntheticDataSpec
      }
    val unfilteredEvents: List<LabeledImpression> =
      eventGroupSpecs
        .flatMap { SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          syntheticPopulationSpec,
          syntheticDataSpec,
          cmmsEventGroup.collectionInterval.toRange(),
        ) }
        .map { event ->
          labeledImpression {
            eventTime = event.timestamp.toProtoTime()
            vid = event.vid
            this.event = Any.pack(event.message)
          }
        }
    val estZoneId = ZoneId.of("UTC")
    val groupedImpressions: Map<LocalDate, List<LabeledImpression>> =
      unfilteredEvents.groupBy { LocalDate.ofInstant(it.eventTime.toInstant(), estZoneId) }

    groupedImpressions.forEach { (date, impressions) ->
      val ds = date.toString()
      println("Date: $ds")
      println("Impressions: ${impressions.size}")

      val impressionsBlobKey = "ds/$ds/event-group-id/${cmmsEventGroup.toName()}/impressions"
      val impressionsFileUri = "file:///$IMPRESSIONS_BUCKET/$impressionsBlobKey"
      val impressionsStorageClient = SelectedStorageClient(impressionsFileUri, storagePath.toFile())

      // Set up streaming encryption
      val tinkKeyTemplateType = "AES128_GCM_HKDF_1MB"
      val aeadKeyTemplate = KeyTemplates.get(tinkKeyTemplateType)
      val keyEncryptionHandle = KeysetHandle.generateNew(aeadKeyTemplate)
      val serializedEncryptionKey =
        ByteString.copyFrom(
          TinkProtoKeysetFormat.serializeEncryptedKeyset(
            keyEncryptionHandle,
            kmsClient.getAead(kekUri),
            byteArrayOf(),
          )
        )
      val aeadStorageClient =
        impressionsStorageClient.withEnvelopeEncryption(kmsClient, kekUri, serializedEncryptionKey)

      // Wrap aead client in mesos client
      val mesosRecordIoStorageClient = MesosRecordIoStorageClient(aeadStorageClient)

      // Write impressions to storage
      mesosRecordIoStorageClient.writeBlob(
        impressionsBlobKey,
        impressions.asFlow().map { it.toByteString() },
      )
      val impressionsMetaDataBlobKey = "ds/$ds/event-group-id/${cmmsEventGroup.toName()}/metadata"

      val impressionsMetadataFileUri =
        "file:///$IMPRESSIONS_METADATA_BUCKET/$impressionsMetaDataBlobKey"

      logger.info("IMPRESSIONS IMPRESSIONS")
      logger.info("Writing impressions to $impressionsMetadataFileUri")

      // Create the impressions metadata store
      Files.createDirectories(storagePath.resolve(IMPRESSIONS_METADATA_BUCKET))
      val impressionsMetadataStorageClient =
        SelectedStorageClient(impressionsMetadataFileUri, storagePath.toFile())

      val encryptedDek =
        EncryptedDek.newBuilder().setKekUri(kekUri).setEncryptedDek(serializedEncryptionKey).build()
      val blobDetails = blobDetails {
        this.blobUri = impressionsFileUri
        this.encryptedDek = encryptedDek
      }
      runBlocking {
        impressionsMetadataStorageClient.writeBlob(
          impressionsMetaDataBlobKey,
          blobDetails.toByteString(),
        )
      }
    }

    return true
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
    private val LAST_EVENT_DATE = LocalDate.now()
    private val FIRST_EVENT_DATE = LAST_EVENT_DATE.minusDays(1)
    private const val IMPRESSIONS_BUCKET = "impression-bucket"
    private const val IMPRESSIONS_METADATA_BUCKET = "impression-metadata-bucket"
    private const val REQUISITION_STORAGE_PREFIX = "requisition-storage-prefix"

    private const val IMPRESSIONS_METADATA_FILE_URI_PREFIX = "file:///$IMPRESSIONS_METADATA_BUCKET"
  }
}
