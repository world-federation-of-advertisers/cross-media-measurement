package org.wfanet.measurement.edpaggregator.resultsfulfiller

import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionResponse
import com.google.common.truth.Truth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.type.interval
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDate
import kotlin.random.Random
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.stub
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.events
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParamsKt.connectionDetails
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParamsKt.consentDetails
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.resultsFulfillerParams
import org.wfanet.measurement.gcloud.pubsub.Publisher
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItemAttempt
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

class ResultsFulfillerAppTest {
  private lateinit var emulatorClient: GooglePubSubEmulatorClient

  private val workItemsServiceMock = mockService<WorkItemsCoroutineImplBase>()
  private val workItemAttemptsServiceMock = mockService<WorkItemAttemptsCoroutineImplBase>()

  @get:Rule
  val grpcTestServer = GrpcTestServerRule {
    addService(workItemsServiceMock)
    addService(workItemAttemptsServiceMock)
  }

  @Before
  fun setupPubSubResources() {
    runBlocking {
      emulatorClient =
        GooglePubSubEmulatorClient(
          host = pubSubEmulatorProvider.host,
          port = pubSubEmulatorProvider.port,
        )
      emulatorClient.createTopic(PROJECT_ID, TOPIC_ID)
      emulatorClient.createSubscription(PROJECT_ID, SUBSCRIPTION_ID, TOPIC_ID)
    }
  }

  @After
  fun cleanPubSubResources() {
    runBlocking {
      emulatorClient.deleteTopic(PROJECT_ID, TOPIC_ID)
      emulatorClient.deleteSubscription(PROJECT_ID, SUBSCRIPTION_ID)
    }
  }

  private val requisitionsServiceMock: RequisitionsGrpcKt.RequisitionsCoroutineImplBase = mockService {
    onBlocking { fulfillDirectRequisition(any()) }.thenReturn(fulfillDirectRequisitionResponse {})
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(requisitionsServiceMock) }
  private val requisitionsStub: RequisitionsGrpcKt.RequisitionsCoroutineStub by lazy {
    RequisitionsGrpcKt.RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun `runWork processes requisition successfully`() = runBlocking {
    val pubSubClient = Subscriber(projectId = PROJECT_ID, googlePubSubClient = emulatorClient)
    val publisher = Publisher<WorkItem>(PROJECT_ID, emulatorClient)
    val workItemsStub = WorkItemsCoroutineStub(grpcTestServer.channel)
    val workItemAttemptsStub = WorkItemAttemptsCoroutineStub(grpcTestServer.channel)

    val testWorkItemAttempt = workItemAttempt {
      name = "workItems/workItem/workItemAttempts/workItemAttempt"
    }
    val workItemParams = createWorkItemParams()
    val workItem = createWorkItem(workItemParams)
    workItemAttemptsServiceMock.stub {
      onBlocking { createWorkItemAttempt(any()) } doReturn testWorkItemAttempt
    }
    workItemAttemptsServiceMock.stub {
      onBlocking { completeWorkItemAttempt(any()) } doReturn testWorkItemAttempt
    }
    workItemAttemptsServiceMock.stub {
      onBlocking { failWorkItemAttempt(any()) } doReturn testWorkItemAttempt
    }
    workItemsServiceMock.stub { onBlocking { failWorkItem(any()) } doReturn workItem }

    // Create requisitions storage client
    val requisitionsTmpPath = Files.createTempDirectory(null).toFile()
    Files.createDirectories(requisitionsTmpPath.resolve(REQUISITIONS_BUCKET).toPath())
    val requisitionsStorageClient = SelectedStorageClient(REQUISITIONS_FILE_URI, requisitionsTmpPath)

    // Add requisitions to storage
    requisitionsStorageClient.writeBlob(
      REQUISITIONS_BLOB_KEY,
      REQUISITION.toString().toByteStringUtf8()
    )

    // Create impressions storage client
    val impressionsTmpPath = Files.createTempDirectory(null).toFile()
    Files.createDirectories(impressionsTmpPath.resolve(IMPRESSIONS_BUCKET).toPath())
    val impressionsStorageClient = SelectedStorageClient(IMPRESSIONS_FILE_URI, impressionsTmpPath)

    // Set up KMS
    val kmsClient = FakeKmsClient()
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "kek"
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))

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

    val impressions =
      List(130) {
        LABELED_IMPRESSION.copy {
          vid = it.toLong()
          eventTime = TIME_RANGE.start.toProtoTime()
        }
      }

    val impressionsFlow = flow {
      impressions.forEach { impression -> emit(impression.toByteString()) }
    }

    // Write impressions to storage
    mesosRecordIoStorageClient.writeBlob(IMPRESSIONS_BLOB_KEY, impressionsFlow)

    // Create the impressions metadata store
    val metadataTmpPath = Files.createTempDirectory(null).toFile()
    Files.createDirectories(metadataTmpPath.resolve(IMPRESSIONS_METADATA_BUCKET).toPath())
    val impressionsMetadataStorageClient =
      SelectedStorageClient(IMPRESSIONS_METADATA_FILE_URI, metadataTmpPath)

    val encryptedDek =
      EncryptedDek.newBuilder().setKekUri(kekUri).setEncryptedDek(serializedEncryptionKey).build()
    val blobDetails =
      BlobDetails.newBuilder()
        .setBlobUri(IMPRESSIONS_FILE_URI)
        .setEncryptedDek(encryptedDek)
        .build()

    impressionsMetadataStorageClient.writeBlob(
      IMPRESSION_METADATA_BLOB_KEY,
      blobDetails.toString().toByteStringUtf8()
    )

    val app = ResultsFulfillerTestApp(
      subscriptionId = SUBSCRIPTION_ID,
      queueSubscriber = pubSubClient,
      parser = WorkItem.parser(),
      workItemsStub,
      workItemAttemptsStub,
      // TODO: Add CommonServer to test so there are valid targets
      "",
      "",
      SECRET_FILES_PATH.resolve("kingdom_root.pem").toFile(),
      requisitionsTmpPath,
      impressionsTmpPath,
      metadataTmpPath,
      kmsClient
    )
    val job = launch { app.run() }

    publisher.publishMessage(TOPIC_ID, workItem)

    val processedMessage = app.messageProcessed.await()

    assertThat(processedMessage).isEqualTo(workItemParams)

    job.cancelAndJoin()
  }

  private fun createWorkItemParams(): WorkItemParams {
    return workItemParams {
      appParams = resultsFulfillerParams {
        storageDetails = ResultsFulfillerParamsKt.storageDetails {
          labeledImpressionsBlobDetailsUriPrefix = IMPRESSIONS_METADATA_FILE_URI_PREFIX
        }
        connectionDetails = connectionDetails {
          clientCert = SECRET_FILES_PATH.resolve("edp1_tls.pem").toFile().readByteString()
          clientPrivateKey = SECRET_FILES_PATH.resolve("edp1_tls.key").toFile().readByteString()
        }
        consentDetails = consentDetails {
          resultCsCertDer = EDP_RESULT_SIGNING_KEY.toString().encodeToByteArray().toByteString()
          resultCsPrivateKeyDer = DATA_PROVIDER_CERTIFICATE_KEY.toString().encodeToByteArray().toByteString()
          privateEncryptionKey = PRIVATE_ENCRYPTION_KEY.toString().encodeToByteArray().toByteString()
          edpCertificateName = DATA_PROVIDER_CERTIFICATE_KEY.toName()
        }
      }.pack()

      dataPathParams = WorkItemKt.WorkItemParamsKt.dataPathParams {
        dataPath = REQUISITIONS_FILE_URI
      }
    }
  }

  private fun createWorkItem(workItemParams: WorkItemParams): WorkItem {
    val packedWorkItemParams = Any.pack(workItemParams)
    return workItem {
      name = "workItems/workItem"
      this.workItemParams = packedWorkItemParams
    }
  }

  init {
    AeadConfig.register()
    StreamingAeadConfig.register()
  }

  companion object {

    private const val PROJECT_ID = "test-project"
    private const val SUBSCRIPTION_ID = "test-subscription"
    private const val TOPIC_ID = "test-topic"

    @get:ClassRule
    @JvmStatic val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()

    private val LAST_EVENT_DATE = LocalDate.now()
    private val FIRST_EVENT_DATE = LAST_EVENT_DATE.minusDays(1)
    private val TIME_RANGE = OpenEndTimeRange.fromClosedDateRange(FIRST_EVENT_DATE..LAST_EVENT_DATE)
    private const val REACH_TOLERANCE = 6.0
    private const val FREQUENCY_DISTRIBUTION_TOLERANCE = 1.0

    private val PERSON = person {
      ageGroup = Person.AgeGroup.YEARS_18_TO_34
      gender = Person.Gender.MALE
      socialGradeGroup = Person.SocialGradeGroup.A_B_C1
    }

    private val TEST_EVENT = testEvent { person = PERSON }

    private val LABELED_IMPRESSION =
      LabeledImpression.newBuilder()
        .setEventTime(Timestamp.getDefaultInstance())
        .setVid(10L)
        .setEvent(
          TEST_EVENT.pack()
        )
        .build()

    private const val EDP_ID = "someDataProvider"
    private const val EDP_NAME = "dataProviders/$EDP_ID"
    private const val EVENT_GROUP_NAME = "$EDP_NAME/eventGroups/name"
    private const val EDP_DISPLAY_NAME = "edp1"
    private val PRIVATE_ENCRYPTION_KEY =
      loadEncryptionPrivateKey("${EDP_DISPLAY_NAME}_enc_private.tink")
    private val SECRET_FILES_PATH: Path =
      checkNotNull(
        getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )
      )
    private const val MEASUREMENT_CONSUMER_ID = "mc"
    private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
    private const val REQUISITION_NAME = "$DATA_PROVIDER_NAME/requisitions/foo"
    private val MC_SIGNING_KEY = loadSigningKey("${MEASUREMENT_CONSUMER_ID}_cs_cert.der", "${MEASUREMENT_CONSUMER_ID}_cs_private.der")
    private val DATA_PROVIDER_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRET_FILES_PATH.resolve("${EDP_DISPLAY_NAME}_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val MC_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val MC_PRIVATE_KEY: TinkPrivateKeyHandle =
      loadPrivateKey(SECRET_FILES_PATH.resolve("mc_enc_private.tink").toFile())
    private val REQUISITION_SPEC = requisitionSpec {
      events = events {
        eventGroups += eventGroupEntry {
          key = EVENT_GROUP_NAME
          value =
            RequisitionSpecKt.EventGroupEntryKt.value {
              collectionInterval = interval {
                startTime = TIME_RANGE.start.toProtoTime()
                endTime = TIME_RANGE.endExclusive.toProtoTime()
              }
              filter = eventFilter { expression = "person.gender==1" }
            }
        }
      }
      measurementPublicKey = MC_PUBLIC_KEY.pack()
      nonce = Random.Default.nextLong()
    }
    private val ENCRYPTED_REQUISITION_SPEC =
      encryptRequisitionSpec(
        signRequisitionSpec(REQUISITION_SPEC, MC_SIGNING_KEY),
        DATA_PROVIDER_PUBLIC_KEY,
      )
    private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-12
    }
    private val MEASUREMENT_SPEC = measurementSpec {
      measurementPublicKey = MC_PUBLIC_KEY.pack()
      reachAndFrequency = reachAndFrequency {
        reachPrivacyParams = OUTPUT_DP_PARAMS
        frequencyPrivacyParams = OUTPUT_DP_PARAMS
        maximumFrequency = 10
      }
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }
      nonceHashes += Hashing.hashSha256(REQUISITION_SPEC.nonce)
    }

    private val NOISE_MECHANISM = ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN

    private val REQUISITION = requisition {
      name = REQUISITION_NAME
      measurement = "$MEASUREMENT_CONSUMER_NAME/measurements/BBBBBBBBBHs"
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
      measurementSpec = signMeasurementSpec(MEASUREMENT_SPEC, MC_SIGNING_KEY)
      encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
      protocolConfig = protocolConfig {
        protocols +=
          ProtocolConfigKt.protocol {
            direct =
              ProtocolConfigKt.direct {
                noiseMechanisms += NOISE_MECHANISM
                deterministicCountDistinct =
                  ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
                deterministicDistribution =
                  ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
              }
          }
      }
      dataProviderCertificate = "$DATA_PROVIDER_NAME/certificates/AAAAAAAAAcg"
      dataProviderPublicKey = DATA_PROVIDER_PUBLIC_KEY.pack()
    }

    private val EDP_RESULT_SIGNING_KEY =
      loadSigningKey(
        "${EDP_DISPLAY_NAME}_result_cs_cert.der",
        "${EDP_DISPLAY_NAME}_result_cs_private.der",
      )
    private val DATA_PROVIDER_CERTIFICATE_KEY =
      DataProviderCertificateKey(EDP_ID, externalIdToApiId(8L))

    private fun loadSigningKey(
      certDerFileName: String,
      privateKeyDerFileName: String,
    ): SigningKeyHandle {
      return org.wfanet.measurement.common.crypto.testing.loadSigningKey(
        SECRET_FILES_PATH.resolve(certDerFileName).toFile(),
        SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile(),
      )
    }

    private const val REQUISITIONS_BUCKET = "requisitions-bucket"
    private const val REQUISITIONS_BLOB_KEY = "requisitions"
    private const val REQUISITIONS_FILE_URI = "file:///$REQUISITIONS_BUCKET/$REQUISITIONS_BLOB_KEY"

    private const val IMPRESSIONS_BUCKET = "impression-bucket"
    private const val IMPRESSIONS_BLOB_KEY = "impressions"
    private const val IMPRESSIONS_FILE_URI = "file:///$IMPRESSIONS_BUCKET/$IMPRESSIONS_BLOB_KEY"

    private const val IMPRESSIONS_METADATA_BUCKET = "impression-metadata-bucket"
    private val IMPRESSION_METADATA_BLOB_KEY =
      "ds/${TIME_RANGE.start}/event-group-id/$EVENT_GROUP_NAME/metadata"

    private val IMPRESSIONS_METADATA_FILE_URI =
      "file:///$IMPRESSIONS_METADATA_BUCKET/$IMPRESSION_METADATA_BLOB_KEY"

    private const val IMPRESSIONS_METADATA_FILE_URI_PREFIX = "file:///$IMPRESSIONS_METADATA_BUCKET"
  }
}
