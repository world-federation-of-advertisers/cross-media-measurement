package org.wfanet.measurement.securecomputation.teeapps.resultsfulfillment


import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.type.interval
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDate
import kotlin.random.Random
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import com.google.protobuf.Any
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.events
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionResponse
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketFilter
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManager
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.TestInMemoryBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.TestPrivacyBucketMapper
import org.wfanet.measurement.gcloud.pubsub.Publisher
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.encryptedDEK
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfig
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfigKt.triggeredApp
import org.wfanet.measurement.securecomputation.teeapps.v1alpha.requisitionsList
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.virtualpeople.common.Gender
import org.wfanet.virtualpeople.common.ageRange
import org.wfanet.virtualpeople.common.demoBucket
import org.wfanet.virtualpeople.common.labelerOutput
import org.wfanet.virtualpeople.common.personLabelAttributes
import org.wfanet.virtualpeople.common.virtualPersonActivity

@RunWith(JUnit4::class)
class ResultsFulfillerAppTest {
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { fulfillDirectRequisition(any()) }.thenReturn(fulfillDirectRequisitionResponse {})
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(requisitionsServiceMock)
  }
  private val requisitionsStub: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }
  private val shardedStorageClient: StorageClient = InMemoryStorageClient()
  private val backingStore = TestInMemoryBackingStore()
  private val privacyBudgetManager =
    PrivacyBudgetManager(PrivacyBucketFilter(TestPrivacyBucketMapper()), backingStore, 10.0f, 0.02f)

  private val projectId = "test-project-id"
  private val topicId = "test-topic-id"
  private val subscriptionId = "test-subscription-id"

  @Rule @JvmField val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()

  private lateinit var workItemsService: GooglePubSubWorkItemsService
  private lateinit var googlePubSubClient: GooglePubSubEmulatorClient

  @Before
  fun setup() {
    googlePubSubClient =
      GooglePubSubEmulatorClient(
        host = pubSubEmulatorProvider.host,
        port = pubSubEmulatorProvider.port,
      )
    workItemsService = GooglePubSubWorkItemsService(projectId, googlePubSubClient)
    runBlocking {
      googlePubSubClient.createTopic(projectId, topicId)
      googlePubSubClient.createSubscription(projectId, subscriptionId, topicId)
    }
  }

  @After
  fun cleanPubSubResources() {
    runBlocking {
      googlePubSubClient.deleteTopic(projectId, topicId)
      googlePubSubClient.deleteSubscription(projectId, subscriptionId)
    }
  }

  @Test
  fun `runWork processes requisition successfully`() = runBlocking {
    val queueSubscriber = Subscriber(projectId, googlePubSubClient)
    val publisher = Publisher<DataWatcherConfig.TriggeredApp>(projectId, googlePubSubClient)
    val inMemoryStorageClient = InMemoryStorageClient()
    val requisitionsPath = "$STORAGE_PATH/${REQUISITION.name}"
    val labeledImpressionPathPrefix = "$EVENT_GROUP_STORAGE_PREFIX/ds/${TIME_RANGE.start}/event-group-id/${EVENT_GROUP_NAME}"

    // Add requisitions to storage
    inMemoryStorageClient.writeBlob(
      requisitionsPath,
      requisitionList.toString().toByteStringUtf8(),
    )

    // Add labeled impressions to storage
    inMemoryStorageClient.writeBlob(
      "$labeledImpressionPathPrefix/sharded-impressions",
      LABELER_OUTPUT.toString().toByteStringUtf8()
    )


    val encryptedDek = encryptedDEK {
      blobKey = "$labeledImpressionPathPrefix/sharded-impressions"
    }

    // Add encryptedDek to storage
    inMemoryStorageClient.writeBlob(
      "$labeledImpressionPathPrefix/metadata",
      encryptedDek.toString().toByteStringUtf8()
    )

    val resultsFulfillerApp = ResultsFulfillerApp(
      inMemoryStorageClient,
      PRIVATE_ENCRYPTION_KEY,
      requisitionsStub,
      DATA_PROVIDER_CERTIFICATE_KEY,
      EDP_RESULT_SIGNING_KEY,
      privacyBudgetManager,
      MC_NAME,
      EVENT_GROUP_STORAGE_PREFIX,
      subscriptionId,
      queueSubscriber,
      DataWatcherConfig.TriggeredApp.parser()
    )
    val job = launch { resultsFulfillerApp.run() }

    val work = triggeredApp {
      path = requisitionsPath
    }

    publisher.publishMessage(topicId, work)
    delay(5000)
    //TODO: Add test
    job.cancelAndJoin()
  }

  companion object {
    private val requisitionList = requisitionsList {
      requisitions += listOf(Any.pack(REQUISITION))
    }
    private val PEOPLE = listOf(
      virtualPersonActivity {
        virtualPersonId = 1
        label = personLabelAttributes {
          demo = demoBucket {
            gender = Gender.GENDER_MALE
            age = ageRange {
              minAge = 18
              maxAge = 25
            }
          }
        }
      }
    )
    private val LABELER_OUTPUT = labelerOutput {
      people += PEOPLE
    }
    private val EVENT_GROUP_STORAGE_PREFIX = "test/test-event-groups"
    private val STORAGE_PATH = "test/test-requisitions"
    private val LAST_EVENT_DATE = LocalDate.now()
    private val FIRST_EVENT_DATE = LAST_EVENT_DATE.minusDays(1)
    private val TIME_RANGE = OpenEndTimeRange.fromClosedDateRange(FIRST_EVENT_DATE..LAST_EVENT_DATE)
    private const val EDP_ID = "someDataProvider"
    private const val EDP_NAME = "dataProviders/$EDP_ID"
    private const val EVENT_GROUP_NAME = "$EDP_NAME/eventGroups/name"
    private const val EDP_DISPLAY_NAME = "edp1"
    private val PRIVATE_ENCRYPTION_KEY = loadEncryptionPrivateKey("${EDP_DISPLAY_NAME}_enc_private.tink")
    private val SECRET_FILES_PATH: Path =
      checkNotNull(
        getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )
      )
    private const val MC_ID = "mc"
    private const val MC_NAME = "measurementConsumers/$MC_ID"
    private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
    private const val REQUISITION_NAME = "$DATA_PROVIDER_NAME/requisitions/foo"
    private val MC_SIGNING_KEY = loadSigningKey("${MC_ID}_cs_cert.der", "${MC_ID}_cs_private.der")
    private val DATA_PROVIDER_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("${EDP_DISPLAY_NAME}_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val MC_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val MC_PRIVATE_KEY =
      loadPrivateKey(SECRET_FILES_PATH.resolve("mc_enc_private.tink").toFile())
    private val REQUISITION_SPEC = requisitionSpec {
      events =
        events {
          eventGroups += eventGroupEntry {
            key = EVENT_GROUP_NAME
            value =
              RequisitionSpecKt.EventGroupEntryKt.value {
                collectionInterval = interval {
                  startTime = TIME_RANGE.start.toProtoTime()
                  endTime = TIME_RANGE.endExclusive.toProtoTime()
                }
                filter = eventFilter {
                  expression = ""
                }
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
    private val REQUISITION = requisition {
      name = REQUISITION_NAME
      measurement = "$MEASUREMENT_CONSUMER_NAME/measurements/BBBBBBBBBHs"
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
      measurementSpec = signMeasurementSpec(
        measurementSpec {
          measurementPublicKey = MC_PUBLIC_KEY.pack()
          reachAndFrequency = reachAndFrequency {
            reachPrivacyParams = differentialPrivacyParams {
              epsilon = 1.0
              delta = 1E-12
            }
            frequencyPrivacyParams = differentialPrivacyParams {
              epsilon = 1.0
              delta = 1E-12
            }
            maximumFrequency = 10
          }
          vidSamplingInterval = vidSamplingInterval {
            start = 0.0f
            width = 1.0f
          }
          nonceHashes += Hashing.hashSha256(REQUISITION_SPEC.nonce)
        }
        , MC_SIGNING_KEY)
      encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
      protocolConfig = protocolConfig {
        protocols += ProtocolConfigKt.protocol {
          direct = ProtocolConfigKt.direct {
            noiseMechanisms += ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
          }
        }
      }
      dataProviderCertificate = "$DATA_PROVIDER_NAME/certificates/AAAAAAAAAcg"
      dataProviderPublicKey = DATA_PROVIDER_PUBLIC_KEY.pack()
    }
    private val EDP_SIGNING_KEY =
      loadSigningKey("${EDP_DISPLAY_NAME}_cs_cert.der", "${EDP_DISPLAY_NAME}_cs_private.der")
    private val EDP_RESULT_SIGNING_KEY =
      loadSigningKey(
        "${EDP_DISPLAY_NAME}_result_cs_cert.der",
        "${EDP_DISPLAY_NAME}_result_cs_private.der",
      )
    private val DATA_PROVIDER_CERTIFICATE_KEY =
      DataProviderCertificateKey(EDP_ID, externalIdToApiId(8L))
    private val DATA_PROVIDER_RESULT_CERTIFICATE_KEY =
      DataProviderCertificateKey(EDP_ID, externalIdToApiId(9L))
    private fun loadSigningKey(
      certDerFileName: String,
      privateKeyDerFileName: String,
    ): SigningKeyHandle {
      return org.wfanet.measurement.common.crypto.testing.loadSigningKey(
        SECRET_FILES_PATH.resolve(certDerFileName).toFile(),
        SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile(),
      )
    }
  }
}
