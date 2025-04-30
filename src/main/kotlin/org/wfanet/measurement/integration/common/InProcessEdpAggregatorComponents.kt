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
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import com.google.type.interval
import io.grpc.Channel
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.LocalDate
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
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EventGroup as ExternalEventGroup
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.edpaggregator.eventgroups.EventGroupSync
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionFetcher
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ResultsFulfillerTestApp
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
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

class InProcessEdpAggregatorComponents(
  private val internalServicesRule: ProviderRule<InternalApiServices>,
  private val pubSubClient: GooglePubSubEmulatorClient,
  private val storageClient: StorageClient,
  private val storagePath: Path,
) : TestRule {

  private val internalServices: InternalApiServices
    get() = internalServicesRule.value

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
  lateinit var externalEventGroups: List<ExternalEventGroup>

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

  fun startDaemons(
    kingdomChannel: Channel,
    measurementConsumerData: MeasurementConsumerData,
    edpDisplayNameToResourceMap: Map<String, Resource>,
  ) = runBlocking {
    // Create all resources
    createAllResources()
    pubSubClient.createTopic(PROJECT_ID, FULFILLER_TOPIC_ID)
    pubSubClient.createSubscription(PROJECT_ID, SUBSCRIPTION_ID, FULFILLER_TOPIC_ID)
    edpResourceName = edpDisplayNameToResourceMap["edp1"]!!.name
    print(edpResourceName)
    publicApiChannel = kingdomChannel
    val labeledImpressionBlobUriPrefix = writeImpressionData()
    val resultsFulfillerParams = getResultsFulfillerParams(
      edpResourceName,
      DataProviderCertificateKey.fromName(
        edpDisplayNameToResourceMap["edp1"]!!.dataProvider.certificate
      )!!,
    labeledImpressionBlobUriPrefix,
    )
    val watchedPaths =
      getDataWatcherConfig(
        blobPrefix = "some-storage-prefix",
        edpResultFulfillerConfigs =
          mapOf(
            edpResourceName to
              resultsFulfillerParams
          ),
      )
    dataWatcher = DataWatcher(workItemsClient, watchedPaths)

    val subscribingStorageClient =
      DataWatcherSubscribingStorageClient(storageClient, storagePath.toString())
    subscribingStorageClient.subscribe(dataWatcher)

    requisitionFetcher =
      RequisitionFetcher(
        requisitionsClient,
        subscribingStorageClient,
        edpResourceName,
        "some-storage-prefix",
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
          eventGroupReferenceId = "sim-eg-reference-id-1"
          measurementConsumer = measurementConsumerData.name
          dataAvailabilityInterval = interval {
            startTime = timestamp { seconds = 200 }
            endTime = timestamp { seconds = 300 }
          }
        }
      )
    eventGroupSync =
      EventGroupSync(
        edpResourceName,
        eventGroupsClient,
        eventGroups.asFlow(),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000L)),
      )
    runBlocking { eventGroupSync.sync().collect {} }
    backgroundScope.launch { resultFulfillerApp.run() }
  }

  fun writeImpressionData(): String {
    // Create impressions storage client
    Files.createDirectories(storagePath.resolve(IMPRESSIONS_BUCKET))
    val impressionsStorageClient = SelectedStorageClient(IMPRESSIONS_FILE_URI, storagePath.toFile())
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

    val impressions: Flow<ByteString> = getLabeledImpressions().map { it.toByteString() }

    runBlocking {
      // Write impressions to storage
      mesosRecordIoStorageClient.writeBlob(IMPRESSIONS_BLOB_KEY, impressions)
    }
    // TODO: Use registered event group id
    val eventGroupName = "${edpResourceName}/eventGroups/name"
    val impressionsMetaDataBlobKey =
      "ds/${TIME_RANGE.start}/event-group-id/${eventGroupName}/metadata"

    val impressionsMetadataFileUri =
      "file:///$IMPRESSIONS_METADATA_BUCKET/$impressionsMetaDataBlobKey"

    // Create the impressions metadata store
    Files.createDirectories(storagePath.resolve(IMPRESSIONS_METADATA_BUCKET))
    val impressionsMetadataStorageClient =
      SelectedStorageClient(impressionsMetadataFileUri, storagePath.toFile())

    val encryptedDek =
      EncryptedDek.newBuilder().setKekUri(kekUri).setEncryptedDek(serializedEncryptionKey).build()
    val blobDetails = blobDetails {
      this.blobUri = IMPRESSIONS_FILE_URI
      this.encryptedDek = encryptedDek
    }
    runBlocking {
      impressionsMetadataStorageClient.writeBlob(
        impressionsMetaDataBlobKey,
        blobDetails.toByteString(),
      )
    }
    return "file:///${IMPRESSIONS_METADATA_BUCKET}"
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
    private val TIME_RANGE = OpenEndTimeRange.fromClosedDateRange(FIRST_EVENT_DATE..LAST_EVENT_DATE)
    private const val IMPRESSIONS_BUCKET = "impression-bucket"
    private const val IMPRESSIONS_BLOB_KEY = "impressions"
    private const val IMPRESSIONS_FILE_URI = "file:///$IMPRESSIONS_BUCKET/$IMPRESSIONS_BLOB_KEY"
    private const val IMPRESSIONS_METADATA_BUCKET = "impression-metadata-bucket"

    private const val IMPRESSIONS_METADATA_FILE_URI_PREFIX = "file:///$IMPRESSIONS_METADATA_BUCKET"
  }
}
