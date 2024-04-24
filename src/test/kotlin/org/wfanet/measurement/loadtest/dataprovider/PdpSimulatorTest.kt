// Copyright 2022 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.loadtest.dataprovider

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.any
import com.google.protobuf.kotlin.toByteString
import java.lang.UnsupportedOperationException
import java.nio.file.Path
import java.nio.file.Paths
import java.security.cert.X509Certificate
import kotlin.random.Random
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.stub
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.FulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt.ModelReleasesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.ReplaceDataAvailabilityIntervalRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionResponse
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.ProtoReflection
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
import org.wfanet.measurement.common.testing.verifyAndCapture
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec

private const val MC_ID = "mc"
private const val MC_NAME = "measurementConsumers/$MC_ID"
private const val PDP_DISPLAY_NAME = "pdp1"
private val SECRET_FILES_PATH: Path =
  checkNotNull(
    getRuntimePath(
      Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
    )
  )
private const val PDP_ID = "someDataProvider"
private const val PDP_NAME = "dataProviders/$PDP_ID"

private val MEASUREMENT_CONSUMER_CERTIFICATE_DER =
  SECRET_FILES_PATH.resolve("mc_cs_cert.der").toFile().readByteString()
private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val MEASUREMENT_NAME = "$MC_NAME/measurements/BBBBBBBBBHs"
private const val MEASUREMENT_CONSUMER_CERTIFICATE_NAME =
  "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
private val MEASUREMENT_CONSUMER_CERTIFICATE = certificate {
  name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
  x509Der = MEASUREMENT_CONSUMER_CERTIFICATE_DER
}

private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"
private const val MODEL_SUITE_NAME = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAHs"
private const val MODEL_LINE_NAME = "${MODEL_SUITE_NAME}/modelLines/AAAAAAAAAHs"

private val PERSON_1 = person {
  ageGroup = Person.AgeGroup.YEARS_18_TO_34
  gender = Person.Gender.MALE
}

private val PERSON_2 = person {
  ageGroup = Person.AgeGroup.YEARS_35_TO_54
  gender = Person.Gender.MALE
}

private val PERSON_3 = person {
  ageGroup = Person.AgeGroup.YEARS_18_TO_34
  gender = Person.Gender.FEMALE
}

val ATTRIBUTE_1 = any {
  typeUrl = ProtoReflection.getTypeUrl(Person.getDescriptor())
  value = PERSON_1.toByteString()
}

val ATTRIBUTE_2 = any {
  typeUrl = ProtoReflection.getTypeUrl(Person.getDescriptor())
  value = PERSON_2.toByteString()
}

val ATTRIBUTE_3 = any {
  typeUrl = ProtoReflection.getTypeUrl(Person.getDescriptor())
  value = PERSON_3.toByteString()
}

val VID_RANGE_1 = vidRange {
  startVid = 0
  endVidInclusive = 99
}

val VID_RANGE_2 = vidRange {
  startVid = 100
  endVidInclusive = 299
}

val VID_RANGE_3 = vidRange {
  startVid = 300
  endVidInclusive = 599
}

// Male 18-34
val SUB_POPULATION_1 = subPopulation {
  attributes += listOf(ATTRIBUTE_1)
  vidRanges += listOf(VID_RANGE_1)
}

// Male 35-54
val SUB_POPULATION_2 = subPopulation {
  attributes += listOf(ATTRIBUTE_2)
  vidRanges += listOf(VID_RANGE_2)
}

// Female 18-34
val SUB_POPULATION_3 = subPopulation {
  attributes += listOf(ATTRIBUTE_3)
  vidRanges += listOf(VID_RANGE_3)
}

val POPULATION_SPEC_1 = populationSpec {
  subpopulations += listOf(SUB_POPULATION_1, SUB_POPULATION_2, SUB_POPULATION_3)
}

val POPULATION_ID_1 = "1234"

private val POPULATION_SPEC_MAP =
  mapOf<String, PopulationSpec>(
    POPULATION_ID_1 to POPULATION_SPEC_1,
  )

@RunWith(JUnit4::class)
class PdpSimulatorTest {
  private val certificatesServiceMock: CertificatesCoroutineImplBase = mockService {
    onBlocking {
        getCertificate(eq(getCertificateRequest { name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME }))
      }
      .thenReturn(MEASUREMENT_CONSUMER_CERTIFICATE)
    onBlocking {
        getCertificate(eq(getCertificateRequest { name = DATA_PROVIDER_CERTIFICATE.name }))
      }
      .thenReturn(DATA_PROVIDER_CERTIFICATE)
    onBlocking {
        getCertificate(eq(getCertificateRequest { name = DATA_PROVIDER_RESULT_CERTIFICATE.name }))
      }
      .thenReturn(DATA_PROVIDER_RESULT_CERTIFICATE)
  }
  private val dataProvidersServiceMock: DataProvidersCoroutineImplBase = mockService {
    onBlocking { replaceDataAvailabilityInterval(any()) }
      .thenAnswer {
        val request = it.arguments[0] as ReplaceDataAvailabilityIntervalRequest
        dataProvider { dataAvailabilityInterval = request.dataAvailabilityInterval }
      }
  }

  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { listRequisitions(any()) }
      .thenReturn(listRequisitionsResponse { requisitions += REQUISITION })
    onBlocking { fulfillDirectRequisition(any()) }.thenReturn(fulfillDirectRequisitionResponse {})
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(certificatesServiceMock)
    addService(dataProvidersServiceMock)
    addService(requisitionsServiceMock)
  }

  private val certificatesStub: CertificatesCoroutineStub by lazy {
    CertificatesCoroutineStub(grpcTestServerRule.channel)
  }

  private val dataProvidersStub: DataProvidersCoroutineStub by lazy {
    DataProvidersCoroutineStub(grpcTestServerRule.channel)
  }

  private val modelRolloutsStub: ModelRolloutsCoroutineStub by lazy {
    ModelRolloutsCoroutineStub(grpcTestServerRule.channel)
  }

  private val modelReleasesStub: ModelReleasesCoroutineStub by lazy {
    ModelReleasesCoroutineStub(grpcTestServerRule.channel)
  }

  private val requisitionsStub: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun `gets correct population of all 18-34 age range`() {
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += REQUISITION })
    }

    val simulator =
      PdpSimulator(
        PDP_DATA,
        certificatesStub,
        dataProvidersStub,
        requisitionsStub,
        dummyThrottler,
        TRUSTED_CERTIFICATES,
        MC_NAME,
        POPULATION_SPEC_MAP,
        POPULATION_ID_1
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()

    // Result should be the sum of SUB_POPULATION_1 and SUB_POPULATION_3
    assertThat(result.population.value).isEqualTo(400)
  }

  @Test
  fun `gets correct population of all males`() {
    val requisitionSpec =
      REQUISITION_SPEC.copy {
        population =
          RequisitionSpecKt.population {
            filter = eventFilter { expression = "person.gender == ${Person.Gender.MALE_VALUE}" }
          }
      }

    val encryptedRequisitionSpec =
      encryptRequisitionSpec(
        signRequisitionSpec(requisitionSpec, MC_SIGNING_KEY),
        DATA_PROVIDER_PUBLIC_KEY,
      )

    val requisition = REQUISITION.copy { this.encryptedRequisitionSpec = encryptedRequisitionSpec }

    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }

    val simulator =
      PdpSimulator(
        PDP_DATA,
        certificatesStub,
        dataProvidersStub,
        requisitionsStub,
        dummyThrottler,
        TRUSTED_CERTIFICATES,
        MC_NAME,
        POPULATION_SPEC_MAP,
        POPULATION_ID_1
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()

    // Result should be the sum of SUB_POPULATION_1 and SUB_POPULATION_2
    assertThat(result.population.value).isEqualTo(300)
  }

  @Test
  fun `gets correct population of all FEmales`() {
    val requisitionSpec =
      REQUISITION_SPEC.copy {
        population =
          RequisitionSpecKt.population {
            filter = eventFilter { expression = "person.gender == ${Person.Gender.FEMALE_VALUE}" }
          }
      }

    val encryptedRequisitionSpec =
      encryptRequisitionSpec(
        signRequisitionSpec(requisitionSpec, MC_SIGNING_KEY),
        DATA_PROVIDER_PUBLIC_KEY,
      )

    val requisition = REQUISITION.copy { this.encryptedRequisitionSpec = encryptedRequisitionSpec }

    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }

    val simulator =
      PdpSimulator(
        PDP_DATA,
        certificatesStub,
        dataProvidersStub,
        requisitionsStub,
        dummyThrottler,
        TRUSTED_CERTIFICATES,
        MC_NAME,
        POPULATION_SPEC_MAP,
        POPULATION_ID_1
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()

    // Result should be SUB_POPULATION_2
    assertThat(result.population.value).isEqualTo(300)
  }

  companion object {
    private val MC_SIGNING_KEY = loadSigningKey("${MC_ID}_cs_cert.der", "${MC_ID}_cs_private.der")
    private val PDP_SIGNING_KEY =
      loadSigningKey("${PDP_DISPLAY_NAME}_cs_cert.der", "${PDP_DISPLAY_NAME}_cs_private.der")
    private val PDP_RESULT_SIGNING_KEY =
      loadSigningKey(
        "${PDP_DISPLAY_NAME}_result_cs_cert.der",
        "${PDP_DISPLAY_NAME}_result_cs_private.der",
      )
    private val DATA_PROVIDER_CERTIFICATE_KEY =
      DataProviderCertificateKey(PDP_ID, externalIdToApiId(8L))
    private val DATA_PROVIDER_RESULT_CERTIFICATE_KEY =
      DataProviderCertificateKey(PDP_ID, externalIdToApiId(9L))

    private val DATA_PROVIDER_CERTIFICATE = certificate {
      name = DATA_PROVIDER_CERTIFICATE_KEY.toName()
      x509Der = PDP_SIGNING_KEY.certificate.encoded.toByteString()
      subjectKeyIdentifier = PDP_SIGNING_KEY.certificate.subjectKeyIdentifier!!
    }
    private val DATA_PROVIDER_RESULT_CERTIFICATE = certificate {
      name = DATA_PROVIDER_RESULT_CERTIFICATE_KEY.toName()
      x509Der = PDP_RESULT_SIGNING_KEY.certificate.encoded.toByteString()
      subjectKeyIdentifier = PDP_RESULT_SIGNING_KEY.certificate.subjectKeyIdentifier!!
    }
    private val PDP_DATA =
      DataProviderData(
        PDP_NAME,
        PDP_DISPLAY_NAME,
        loadEncryptionPrivateKey("${PDP_DISPLAY_NAME}_enc_private.tink"),
        PDP_RESULT_SIGNING_KEY,
        DATA_PROVIDER_RESULT_CERTIFICATE_KEY,
      )

    private val MC_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val MC_PRIVATE_KEY =
      loadPrivateKey(SECRET_FILES_PATH.resolve("mc_enc_private.tink").toFile())
    private val DATA_PROVIDER_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("${PDP_DISPLAY_NAME}_enc_public.tink").toFile())
        .toEncryptionPublicKey()

    private val REQUISITION_SPEC = requisitionSpec {
      population =
        RequisitionSpecKt.population {
          filter = eventFilter {
            expression = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
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
      modelLine = MODEL_LINE_NAME
    }

    private val REQUISITION = requisition {
      name = "${PDP_NAME}/requisitions/foo"
      measurement = MEASUREMENT_NAME
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      measurementSpec = signMeasurementSpec(MEASUREMENT_SPEC, MC_SIGNING_KEY)
      encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
      protocolConfig = protocolConfig {
        protocols +=
          ProtocolConfigKt.protocol {
            direct =
              ProtocolConfigKt.direct {
                deterministicCountDistinct =
                  ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
                deterministicDistribution =
                  ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
              }
          }
      }
      dataProviderCertificate = DATA_PROVIDER_CERTIFICATE.name
      dataProviderPublicKey = DATA_PROVIDER_PUBLIC_KEY.pack()
    }

    private val TRUSTED_CERTIFICATES: Map<ByteString, X509Certificate> =
      readCertificateCollection(SECRET_FILES_PATH.resolve("edp_trusted_certs.pem").toFile())
        .associateBy { requireNotNull(it.authorityKeyIdentifier) }

    /** Dummy [Throttler] for satisfying signatures without being used. */
    private val dummyThrottler =
      object : Throttler {
        override suspend fun <T> onReady(block: suspend () -> T): T {
          throw UnsupportedOperationException("Should not be called")
        }
      }

    private fun loadSigningKey(
      certDerFileName: String,
      privateKeyDerFileName: String,
    ): SigningKeyHandle {
      return loadSigningKey(
        SECRET_FILES_PATH.resolve(certDerFileName).toFile(),
        SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile(),
      )
    }

    private fun loadEncryptionPrivateKey(fileName: String): TinkPrivateKeyHandle {
      return loadPrivateKey(SECRET_FILES_PATH.resolve(fileName).toFile())
    }
  }
}
