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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import com.google.type.interval
import io.grpc.BindableService
import java.nio.file.Path
import java.nio.file.Paths
import java.security.cert.X509Certificate
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.random.Random
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import org.junit.Rule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionResponse
import org.wfanet.measurement.api.v2alpha.GetEventGroupRequest
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.ReplaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.liquidLegionsV2
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.value
import org.wfanet.measurement.api.v2alpha.RequisitionKt.duchyEntry
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.elGamalPublicKey
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.copy
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionResponse
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.liquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.listModelLinesResponse
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.common.HexString
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificateCollection
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.duchy.signElgamalPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.dataprovider.DataProviderData
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketFilter
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManager
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.TestInMemoryBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.TestPrivacyBucketMapper
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.integration.common.SyntheticGenerationSpecs
import org.wfanet.measurement.loadtest.config.TestIdentifiers

/**
 * Abstract test base for EDP simulator tests.
 *
 * Note that this does not contain any test methods, as most of the functionality of
 * [AbstractEdpSimulator] is not overridable by subclasses. Therefore, it only needs to be tested
 * for the one implementation.
 */
@RunWith(JUnit4::class)
abstract class AbstractEdpSimulatorTest {
  private val modelLinesServiceMock: ModelLinesGrpcKt.ModelLinesCoroutineImplBase = mockService {
    onBlocking { listModelLines(any()) } doReturn
      listModelLinesResponse {
        modelLines += modelLine { name = MODEL_LINE_NAME }
        modelLines += modelLine { name = MODEL_LINE_2_NAME }
      }
  }
  private val certificatesServiceMock: CertificatesCoroutineImplBase = mockService {
    onBlocking {
        getCertificate(eq(getCertificateRequest { name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME }))
      }
      .thenReturn(MEASUREMENT_CONSUMER_CERTIFICATE)
    onBlocking { getCertificate(eq(getCertificateRequest { name = DUCHY_ONE_CERTIFICATE.name })) }
      .thenReturn(DUCHY_ONE_CERTIFICATE)
    onBlocking { getCertificate(eq(getCertificateRequest { name = DUCHY_TWO_CERTIFICATE.name })) }
      .thenReturn(DUCHY_TWO_CERTIFICATE)
    onBlocking {
        getCertificate(eq(getCertificateRequest { name = DATA_PROVIDER_CERTIFICATE.name }))
      }
      .thenReturn(DATA_PROVIDER_CERTIFICATE)
    onBlocking {
        getCertificate(eq(getCertificateRequest { name = DATA_PROVIDER_RESULT_CERTIFICATE.name }))
      }
      .thenReturn(DATA_PROVIDER_RESULT_CERTIFICATE)
  }
  protected val dataProvidersServiceMock: DataProvidersCoroutineImplBase = mockService {
    onBlocking { replaceDataAvailabilityIntervals(any()) }
      .thenAnswer {
        val request = it.arguments[0] as ReplaceDataAvailabilityIntervalsRequest
        dataProvider { dataAvailabilityIntervals += request.dataAvailabilityIntervalsList }
      }
  }
  protected val measurementConsumersServiceMock:
    MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase =
    mockService {
      onBlocking { getMeasurementConsumer(any()) }
        .thenReturn(
          measurementConsumer {
            publicKey = signEncryptionPublicKey(MC_PUBLIC_KEY, MC_SIGNING_KEY)
            certificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
          }
        )
    }
  protected val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService {
    onBlocking { getEventGroup(any()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<GetEventGroupRequest>(0)
        eventGroup {
          name = request.name
          eventGroupReferenceId = TestIdentifiers.SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX
        }
      }
  }
  protected val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { listRequisitions(any()) }
      .thenReturn(listRequisitionsResponse { requisitions += REQUISITION })
    onBlocking { fulfillDirectRequisition(any()) }.thenReturn(fulfillDirectRequisitionResponse {})
  }
  protected val fakeRequisitionFulfillmentService = FakeRequisitionFulfillmentService()

  protected open val services: Iterable<BindableService> =
    listOf(
      measurementConsumersServiceMock,
      certificatesServiceMock,
      modelLinesServiceMock,
      dataProvidersServiceMock,
      eventGroupsServiceMock,
      requisitionsServiceMock,
      fakeRequisitionFulfillmentService,
    )

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    for (service in services) {
      addService(service)
    }
  }

  protected val certificatesStub: CertificatesCoroutineStub by lazy {
    CertificatesCoroutineStub(grpcTestServerRule.channel)
  }

  protected val modelLinesStub: ModelLinesGrpcKt.ModelLinesCoroutineStub by lazy {
    ModelLinesGrpcKt.ModelLinesCoroutineStub(grpcTestServerRule.channel)
  }

  protected val dataProvidersStub: DataProvidersCoroutineStub by lazy {
    DataProvidersCoroutineStub(grpcTestServerRule.channel)
  }

  protected val eventGroupsStub: EventGroupsCoroutineStub by lazy {
    EventGroupsCoroutineStub(grpcTestServerRule.channel)
  }

  protected val requisitionsStub: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }

  protected val requisitionFulfillmentStub: RequisitionFulfillmentCoroutineStub by lazy {
    RequisitionFulfillmentCoroutineStub(grpcTestServerRule.channel)
  }

  protected val requisitionFulfillmentStubMap =
    mapOf(
      DuchyKey(DUCHY_ONE_ID).toName() to requisitionFulfillmentStub,
      DuchyKey(DUCHY_TWO_ID).toName() to requisitionFulfillmentStub,
    )

  protected val backingStore = TestInMemoryBackingStore()
  protected val privacyBudgetManager =
    PrivacyBudgetManager(PrivacyBucketFilter(TestPrivacyBucketMapper()), backingStore, 10.0f, 0.02f)

  protected class FakeRequisitionFulfillmentService : RequisitionFulfillmentCoroutineImplBase() {
    data class FulfillRequisitionInvocation(val requests: List<FulfillRequisitionRequest>)

    private val _fullfillRequisitionInvocations = mutableListOf<FulfillRequisitionInvocation>()
    val fullfillRequisitionInvocations: List<FulfillRequisitionInvocation>
      get() = _fullfillRequisitionInvocations

    override suspend fun fulfillRequisition(
      requests: Flow<FulfillRequisitionRequest>
    ): FulfillRequisitionResponse {
      // Consume flow before returning.
      _fullfillRequisitionInvocations.add(FulfillRequisitionInvocation(requests.toList()))
      return FulfillRequisitionResponse.getDefaultInstance()
    }
  }

  companion object {
    const val EDP_ID = "someDataProvider"
    const val EDP_NAME = "dataProviders/$EDP_ID"
    const val EDP_DISPLAY_NAME = "edp1"
    const val EVENT_GROUP_NAME = "$EDP_NAME/eventGroups/name"
    const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
    const val MC_NAME = MEASUREMENT_CONSUMER_NAME
    const val MC_DISPLAY_NAME = "mc"
    const val MEASUREMENT_NAME = "$MEASUREMENT_CONSUMER_NAME/measurements/BBBBBBBBBHs"
    const val MEASUREMENT_CONSUMER_CERTIFICATE_NAME =
      "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
    const val DUCHY_ONE_ID = "worker1"
    const val DUCHY_TWO_ID = "worker2"
    const val DUCHY_ONE_NAME = "duchies/$DUCHY_ONE_ID"
    const val DUCHY_TWO_NAME = "duchies/$DUCHY_TWO_ID"
    const val MODEL_LINE_NAME = "modelProviders/foo/modelSuites/bar/modelLines/baz"
    const val MODEL_LINE_2_NAME = "modelProviders/foo/modelSuites/bar2/modelLines/baz"
    const val LLV2_DECAY_RATE = 12.0
    const val LLV2_MAX_SIZE = 100_000L

    val DATA_PROVIDER_CERTIFICATE_KEY = DataProviderCertificateKey(EDP_ID, externalIdToApiId(8L))
    val DATA_PROVIDER_RESULT_CERTIFICATE_KEY =
      DataProviderCertificateKey(EDP_ID, externalIdToApiId(9L))

    val SECRET_FILES_PATH: Path =
      checkNotNull(
        getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )
      )

    val TRUSTED_CERTIFICATES: Map<ByteString, X509Certificate> =
      readCertificateCollection(SECRET_FILES_PATH.resolve("edp_trusted_certs.pem").toFile())
        .associateBy { requireNotNull(it.authorityKeyIdentifier) }

    val EDP_SIGNING_KEY =
      loadSigningKey("${EDP_DISPLAY_NAME}_cs_cert.der", "${EDP_DISPLAY_NAME}_cs_private.der")
    val EDP_RESULT_SIGNING_KEY =
      loadSigningKey(
        "${EDP_DISPLAY_NAME}_result_cs_cert.der",
        "${EDP_DISPLAY_NAME}_result_cs_private.der",
      )
    val MC_SIGNING_KEY =
      loadSigningKey("${MC_DISPLAY_NAME}_cs_cert.der", "${MC_DISPLAY_NAME}_cs_private.der")
    val DUCHY_ONE_SIGNING_KEY =
      loadSigningKey("${DUCHY_ONE_ID}_cs_cert.der", "${DUCHY_ONE_ID}_cs_private.der")
    val DUCHY_TWO_SIGNING_KEY =
      loadSigningKey("${DUCHY_TWO_ID}_cs_cert.der", "${DUCHY_TWO_ID}_cs_private.der")

    val DATA_PROVIDER_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("${EDP_DISPLAY_NAME}_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    val MC_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()

    val MC_PRIVATE_KEY = loadPrivateKey(SECRET_FILES_PATH.resolve("mc_enc_private.tink").toFile())

    val EDP_DATA =
      DataProviderData(
        EDP_NAME,
        loadEncryptionPrivateKey("${EDP_DISPLAY_NAME}_enc_private.tink"),
        EDP_RESULT_SIGNING_KEY,
        DATA_PROVIDER_RESULT_CERTIFICATE_KEY,
      )

    val DATA_PROVIDER_CERTIFICATE = certificate {
      name = DATA_PROVIDER_CERTIFICATE_KEY.toName()
      x509Der = EDP_SIGNING_KEY.certificate.encoded.toByteString()
      subjectKeyIdentifier = EDP_SIGNING_KEY.certificate.subjectKeyIdentifier!!
    }
    val DATA_PROVIDER_RESULT_CERTIFICATE = certificate {
      name = DATA_PROVIDER_RESULT_CERTIFICATE_KEY.toName()
      x509Der = EDP_RESULT_SIGNING_KEY.certificate.encoded.toByteString()
      subjectKeyIdentifier = EDP_RESULT_SIGNING_KEY.certificate.subjectKeyIdentifier!!
    }
    val MEASUREMENT_CONSUMER_CERTIFICATE_DER =
      SECRET_FILES_PATH.resolve("mc_cs_cert.der").toFile().readByteString()

    val MEASUREMENT_CONSUMER_CERTIFICATE = certificate {
      name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      x509Der = MEASUREMENT_CONSUMER_CERTIFICATE_DER
    }
    val DUCHY_ONE_CERTIFICATE = certificate {
      name = DuchyCertificateKey(DUCHY_ONE_ID, externalIdToApiId(6L)).toName()
      x509Der = DUCHY_ONE_SIGNING_KEY.certificate.encoded.toByteString()
    }
    val DUCHY_TWO_CERTIFICATE = certificate {
      name = DuchyCertificateKey(DUCHY_TWO_ID, externalIdToApiId(6L)).toName()
      x509Der = DUCHY_TWO_SIGNING_KEY.certificate.encoded.toByteString()
    }

    val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-12
    }

    val NOISE_MECHANISM = ProtocolConfig.NoiseMechanism.DISCRETE_GAUSSIAN

    val LAST_EVENT_DATE: LocalDate = LocalDate.now()
    val FIRST_EVENT_DATE: LocalDate = LAST_EVENT_DATE.minusDays(1)
    val TIME_RANGE = OpenEndTimeRange.fromClosedDateRange(FIRST_EVENT_DATE..LAST_EVENT_DATE)

    val REQUISITION_SPEC = requisitionSpec {
      events =
        RequisitionSpecKt.events {
          eventGroups += eventGroupEntry {
            key = EVENT_GROUP_NAME
            value =
              RequisitionSpecKt.EventGroupEntryKt.value {
                collectionInterval = interval {
                  startTime = TIME_RANGE.start.toProtoTime()
                  endTime = TIME_RANGE.endExclusive.toProtoTime()
                }
                filter = eventFilter {
                  expression =
                    "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE} && " +
                      "person.gender == ${Person.Gender.FEMALE_VALUE}"
                }
              }
          }
        }
      measurementPublicKey = MC_PUBLIC_KEY.pack()
      nonce = Random.nextLong()
    }

    val ENCRYPTED_REQUISITION_SPEC =
      encryptRequisitionSpec(
        signRequisitionSpec(REQUISITION_SPEC, MC_SIGNING_KEY),
        DATA_PROVIDER_PUBLIC_KEY,
      )

    val MEASUREMENT_SPEC = measurementSpec {
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

    val CONSENT_SIGNALING_ELGAMAL_PUBLIC_KEY = elGamalPublicKey {
      ellipticCurveId = 415
      generator =
        HexString("036B17D1F2E12C4247F8BCE6E563A440F277037D812DEB33A0F4A13945D898C296").bytes
      element =
        HexString("0277BF406C5AA4376413E480E0AB8B0EFCA999D362204E6D1686E0BE567811604D").bytes
    }

    val LIQUID_LEGIONS_SKETCH_PARAMS = liquidLegionsSketchParams {
      decayRate = LLV2_DECAY_RATE
      maxSize = LLV2_MAX_SIZE
      samplingIndicatorSize = 10_000_000
    }

    val REQUISITION = requisition {
      name = "${EDP_NAME}/requisitions/foo"
      measurement = MEASUREMENT_NAME
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      measurementSpec = signMeasurementSpec(MEASUREMENT_SPEC, MC_SIGNING_KEY)
      encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
      protocolConfig = protocolConfig {
        protocols +=
          ProtocolConfigKt.protocol {
            liquidLegionsV2 =
              ProtocolConfigKt.liquidLegionsV2 {
                noiseMechanism = NOISE_MECHANISM
                sketchParams = LIQUID_LEGIONS_SKETCH_PARAMS
                ellipticCurveId = 415
              }
          }
      }
      dataProviderCertificate = DATA_PROVIDER_CERTIFICATE.name
      dataProviderPublicKey = DATA_PROVIDER_PUBLIC_KEY.pack()
      duchies += duchyEntry {
        key = DUCHY_ONE_NAME
        value = value {
          duchyCertificate = DUCHY_ONE_CERTIFICATE.name
          liquidLegionsV2 = liquidLegionsV2 {
            elGamalPublicKey =
              signElgamalPublicKey(CONSENT_SIGNALING_ELGAMAL_PUBLIC_KEY, DUCHY_ONE_SIGNING_KEY)
          }
        }
      }
    }

    val SYNTHETIC_DATA_TIME_ZONE: ZoneId = ZoneOffset.UTC

    val SYNTHETIC_DATA_SPEC =
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

    val POPULATION_SPEC = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1L
          endVidInclusive = 1000L
        }
      }
    }
    val VID_INDEX_MAP = InMemoryVidIndexMap.build(POPULATION_SPEC)

    /** Dummy [Throttler] for satisfying signatures without being used. */
    val dummyThrottler =
      object : Throttler {
        override suspend fun <T> onReady(block: suspend () -> T): T {
          throw UnsupportedOperationException("Should not be called")
        }
      }

    fun loadEncryptionPrivateKey(fileName: String): TinkPrivateKeyHandle {
      return loadPrivateKey(SECRET_FILES_PATH.resolve(fileName).toFile())
    }

    fun loadSigningKey(certDerFileName: String, privateKeyDerFileName: String): SigningKeyHandle {
      return loadSigningKey(
        SECRET_FILES_PATH.resolve(certDerFileName).toFile(),
        SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile(),
      )
    }
  }
}
