package org.wfanet.measurement.securecomputation.teeapps.resultsfulfillment


import com.google.common.truth.Truth.assertThat
import org.wfanet.measurement.api.v2alpha.testing.MeasurementResultSubject.Companion.assertThat
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
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.FulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.events
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.copy
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionResponse
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.testing.verifyAndCapture
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.gcloud.pubsub.Publisher
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.wfanet.measurement.integration.common.SyntheticGenerationSpecs
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.loadtest.config.TestIdentifiers
import org.wfanet.measurement.loadtest.dataprovider.EventQuery
import org.wfanet.measurement.loadtest.dataprovider.MeasurementResults
import org.wfanet.measurement.loadtest.dataprovider.SyntheticGeneratorEventQuery
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.encryptedDEK
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfig
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfigKt.triggeredApp
import org.wfanet.measurement.securecomputation.teeapps.v1alpha.requisitionsList
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.virtualpeople.common.DemoBucket
import org.wfanet.virtualpeople.common.Gender
import org.wfanet.virtualpeople.common.ageRange
import org.wfanet.virtualpeople.common.copy
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
    delay(8000)

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()
    val expectedReach: Long = computeExpectedReach(REQUISITION_SPEC.events.eventGroupsList, MEASUREMENT_SPEC)
    val expectedFrequencyDistribution: Map<Long, Double> =
      computeExpectedFrequencyDistribution(
        REQUISITION_SPEC.events.eventGroupsList,
        MEASUREMENT_SPEC,
      )
    assertThat(result.reach.noiseMechanism == NOISE_MECHANISM)
    assertThat(result.reach.hasDeterministicCountDistinct())
    assertThat(result.frequency.noiseMechanism == NOISE_MECHANISM)
    assertThat(result.frequency.hasDeterministicDistribution())
    assertThat(result)
      .reachValue()
      .isWithin(REACH_TOLERANCE / MEASUREMENT_SPEC.vidSamplingInterval.width)
      .of(expectedReach)
    assertThat(result)
      .frequencyDistribution()
      .isWithin(FREQUENCY_DISTRIBUTION_TOLERANCE)
      .of(expectedFrequencyDistribution)

    job.cancelAndJoin()
  }

  @Test
  fun `runWork processes requisition with high volume of impressions successfully`() = runBlocking {
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

    val labelerOutput = labelerOutput {
      people += List(1000) { VIRTUAL_PERSON.copy { virtualPersonId = it+1L } }
    }

    // Add labeled impressions to storage
    inMemoryStorageClient.writeBlob(
      "$labeledImpressionPathPrefix/sharded-impressions",
      labelerOutput.toString().toByteStringUtf8()
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

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()
    val expectedReach: Long =
      computeExpectedReach(REQUISITION_SPEC.events.eventGroupsList, MEASUREMENT_SPEC)
    val expectedFrequencyDistribution: Map<Long, Double> =
      computeExpectedFrequencyDistribution(
        REQUISITION_SPEC.events.eventGroupsList,
        MEASUREMENT_SPEC,
      )
    assertThat(result.reach.noiseMechanism == NOISE_MECHANISM)
    assertThat(result.reach.hasDeterministicCountDistinct())
    assertThat(result.frequency.noiseMechanism == NOISE_MECHANISM)
    assertThat(result.frequency.hasDeterministicDistribution())
    assertThat(result)
      .reachValue()
      .isWithin(REACH_TOLERANCE / MEASUREMENT_SPEC.vidSamplingInterval.width)
      .of(expectedReach)
    assertThat(result)
      .frequencyDistribution()
      .isWithin(FREQUENCY_DISTRIBUTION_TOLERANCE)
      .of(expectedFrequencyDistribution)

    job.cancelAndJoin()
  }

  private fun computeExpectedReach(
    eventGroupsList: List<RequisitionSpec.EventGroupEntry>,
    measurementSpec: MeasurementSpec,
  ): Long {
    val sampledVids = sampleVids(eventGroupsList, measurementSpec)
    val sampledReach = MeasurementResults.computeReach(sampledVids)
    return (sampledReach / measurementSpec.vidSamplingInterval.width).toLong()
  }

  private fun computeExpectedFrequencyDistribution(
    eventGroupsList: List<RequisitionSpec.EventGroupEntry>,
    measurementSpec: MeasurementSpec,
  ): Map<Long, Double> {
    val sampledVids = sampleVids(eventGroupsList, measurementSpec)
    val (_, frequencyMap) =
      MeasurementResults.computeReachAndFrequency(
        sampledVids,
        measurementSpec.reachAndFrequency.maximumFrequency,
      )
    return frequencyMap.mapKeys { it.key.toLong() }
  }

  private fun sampleVids(
    eventGroupsList: List<RequisitionSpec.EventGroupEntry>,
    measurementSpec: MeasurementSpec,
  ): Iterable<Long> {
    val eventGroupSpecs =
      eventGroupsList.map {
        EventQuery.EventGroupSpec(
          eventGroup {
            name = it.key
            eventGroupReferenceId = TestIdentifiers.SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX
          },
          it.value,
        )
      }
    return org.wfanet.measurement.loadtest.common.sampleVids(
      syntheticGeneratorEventQuery,
      eventGroupSpecs,
      measurementSpec.vidSamplingInterval.start,
      measurementSpec.vidSamplingInterval.width,
    )
  }

  companion object {

    private val LAST_EVENT_DATE = LocalDate.now()
    private val FIRST_EVENT_DATE = LAST_EVENT_DATE.minusDays(1)
    private val TIME_RANGE = OpenEndTimeRange.fromClosedDateRange(FIRST_EVENT_DATE..LAST_EVENT_DATE)
    private const val REACH_TOLERANCE = 1.0
    private const val FREQUENCY_DISTRIBUTION_TOLERANCE = 1.0


    private val SYNTHETIC_DATA_SPEC =
      SyntheticGenerationSpecs.SYNTHETIC_DATA_SPECS_SMALL.first().copy {
        dateSpecs.forEachIndexed { index, dateSpec ->
          dateSpecs[index] =
            dateSpec.copy {
              dateRange =
                SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
                  start = FIRST_EVENT_DATE.toProtoDate()
                  endExclusive = (LAST_EVENT_DATE.plusDays(1)).toProtoDate()
                }
            }
        }
      }

    private val syntheticGeneratorEventQuery =
      object :
        SyntheticGeneratorEventQuery(
          SyntheticGenerationSpecs.SYNTHETIC_POPULATION_SPEC_SMALL,
          TestEvent.getDescriptor(),
        ) {
        override fun getSyntheticDataSpec(eventGroup: EventGroup): SyntheticEventGroupSpec {
          return SYNTHETIC_DATA_SPEC
        }
      }

    private val VIRTUAL_PERSON =
      virtualPersonActivity {
        virtualPersonId = 10
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
    private val PEOPLE = listOf(VIRTUAL_PERSON)
    private val LABELER_OUTPUT = labelerOutput {
      people += PEOPLE
    }
    private val EVENT_GROUP_STORAGE_PREFIX = "test/test-event-groups"
    private val STORAGE_PATH = "test/test-requisitions"
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
    private val DATA_PROVIDER_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRET_FILES_PATH.resolve("${EDP_DISPLAY_NAME}_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val MC_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val MC_PRIVATE_KEY: TinkPrivateKeyHandle =
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

    private val REQUISITION: Requisition = requisition {
      name = REQUISITION_NAME
      measurement = "$MEASUREMENT_CONSUMER_NAME/measurements/BBBBBBBBBHs"
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
      measurementSpec = signMeasurementSpec(MEASUREMENT_SPEC, MC_SIGNING_KEY)
      encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
      protocolConfig = protocolConfig {
        protocols += ProtocolConfigKt.protocol {
          direct =
            ProtocolConfigKt.direct {
              noiseMechanisms +=  NOISE_MECHANISM
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

    private val requisitionList = requisitionsList {
      requisitions += listOf(Any.pack(REQUISITION))
    }
  }
}
