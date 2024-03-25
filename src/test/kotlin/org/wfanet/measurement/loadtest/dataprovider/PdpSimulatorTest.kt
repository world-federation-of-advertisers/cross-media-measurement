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
import com.google.protobuf.Timestamp
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.timestamp
import com.google.type.interval
import java.lang.UnsupportedOperationException
import java.nio.file.Path
import java.nio.file.Paths
import java.security.cert.X509Certificate
import java.time.Instant
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
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt.ModelReleasesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt.ModelReleasesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
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
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionResponse
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.listModelRolloutsResponse
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.modelRelease
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.populations.testing.populationBucket
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.unpack
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
import org.wfanet.measurement.common.toProtoTime
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

private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()

private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
private const val POPULATION_NAME = "$DATA_PROVIDER_NAME/populations/AAAAAAAAAHs"
private const val POPULATION_NAME_2 = "$DATA_PROVIDER_NAME/populations/AAAAAAAAAJs"

private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"
private const val MODEL_SUITE_NAME = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAHs"
private const val MODEL_RELEASE_NAME = "$MODEL_SUITE_NAME/modelReleases/AAAAAAAAAHs"
private const val MODEL_RELEASE_NAME_2 = "$MODEL_SUITE_NAME/modelReleases/AAAAAAAAAJs"
private const val MODEL_RELEASE_NAME_3 = "$MODEL_SUITE_NAME/modelReleases/AAAAAAAAAKs"

private const val MODEL_LINE_NAME = "${MODEL_SUITE_NAME}/modelLines/AAAAAAAAAHs"
private const val MODEL_ROLLOUT_NAME = "${MODEL_LINE_NAME}/modelRollouts/AAAAAAAAAHs"

private val MODEL_RELEASE_1 = modelRelease {
  name = MODEL_RELEASE_NAME
  createTime = CREATE_TIME
  population = POPULATION_NAME
}

private val MODEL_RELEASE_2 = modelRelease {
  name = MODEL_RELEASE_NAME_2
  createTime = CREATE_TIME
  population = POPULATION_NAME_2
}

private val MODEL_ROLLOUT = modelRollout {
  name = MODEL_ROLLOUT_NAME
  modelRelease = MODEL_RELEASE_NAME
}

private val EVENT_1 = testEvent {
  person = person {
    ageGroup = Person.AgeGroup.YEARS_18_TO_34
    gender = Person.Gender.MALE
  }
}
private val EVENT_2 = testEvent {
  person = person {
    ageGroup = Person.AgeGroup.YEARS_35_TO_54
    gender = Person.Gender.MALE
  }
}
private val EVENT_3 = testEvent {
  person = person {
    ageGroup = Person.AgeGroup.YEARS_18_TO_34
    gender = Person.Gender.FEMALE
  }
}

private const val POPULATION_SIZE_1 = 100L
private const val POPULATION_SIZE_2 = 200L
private const val POPULATION_SIZE_3 = 300L

private val VALID_START_TIME_1 = timestamp { seconds = 100L }
private val VALID_START_TIME_2 = timestamp { seconds = 200L }
private val VALID_START_TIME_3 = timestamp { seconds = 300L }

private val VALID_END_TIME_1 = timestamp { seconds = 400L }
private val VALID_END_TIME_2 = timestamp { seconds = 500L }

private val POPULATION_BUCKET_1 = populationBucket {
  event = EVENT_1
  populationSize = POPULATION_SIZE_1
  validStartTime = VALID_START_TIME_1
  validEndTime = VALID_END_TIME_1
  modelReleases += listOf(MODEL_RELEASE_NAME, MODEL_RELEASE_NAME_3)
}

private val POPULATION_BUCKET_2 = populationBucket {
  event = EVENT_2
  populationSize = POPULATION_SIZE_2
  validStartTime = VALID_START_TIME_2
  validEndTime = VALID_END_TIME_1
  modelReleases += listOf(MODEL_RELEASE_NAME, MODEL_RELEASE_NAME_2)
}

private val POPULATION_BUCKET_3 = populationBucket {
  event = EVENT_3
  populationSize = POPULATION_SIZE_3
  validStartTime = VALID_START_TIME_3
  validEndTime = VALID_END_TIME_2
  modelReleases += listOf(MODEL_RELEASE_NAME)
}

private val POPULATION_BUCKETS_LIST =
  listOf(POPULATION_BUCKET_1, POPULATION_BUCKET_2, POPULATION_BUCKET_3)

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

  private val modelRolloutsServiceStub: ModelRolloutsCoroutineImplBase = mockService {
    onBlocking { listModelRollouts(any()) }
      .thenReturn(listModelRolloutsResponse { modelRollouts += listOf(MODEL_ROLLOUT) })
  }

  private val modelReleasesServiceStub: ModelReleasesCoroutineImplBase = mockService {
    onBlocking { getModelRelease(any()) }.thenReturn(MODEL_RELEASE_1)
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
    addService(modelRolloutsServiceStub)
    addService(modelReleasesServiceStub)
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
        MC_NAME,
        certificatesStub,
        dataProvidersStub,
        modelRolloutsStub,
        modelReleasesStub,
        requisitionsStub,
        dummyThrottler,
        TRUSTED_CERTIFICATES,
        POPULATION_BUCKETS_LIST,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()

    // Result should be the sum of POPULATION_1 and POPULATION_3
    assertThat(result.population.value).isEqualTo(POPULATION_SIZE_1 + POPULATION_SIZE_3)
  }

  @Test
  fun `gets correct population of all males`() {
    val requisitionSpec =
      REQUISITION_SPEC.copy {
        population =
          RequisitionSpecKt.population {
            filter = eventFilter { expression = "person.gender == ${Person.Gender.MALE_VALUE}" }
            interval = interval {
              startTime = VALID_START_TIME_1
              endTime = VALID_END_TIME_2
            }
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
        MC_NAME,
        certificatesStub,
        dataProvidersStub,
        modelRolloutsStub,
        modelReleasesStub,
        requisitionsStub,
        dummyThrottler,
        TRUSTED_CERTIFICATES,
        POPULATION_BUCKETS_LIST,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()

    // Result should be the sum of POPULATION_BUCKET_1 and POPULATION_BUCKET_2
    assertThat(result.population.value).isEqualTo(POPULATION_SIZE_1 + POPULATION_SIZE_2)
  }

  @Test
  fun `gets correct population of all males within interval`() {
    val requisitionSpec =
      REQUISITION_SPEC.copy {
        population =
          RequisitionSpecKt.population {
            filter = eventFilter { expression = "person.gender == ${Person.Gender.MALE_VALUE}" }
            interval = interval {
              startTime = VALID_START_TIME_2
              endTime = VALID_END_TIME_2
            }
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
        MC_NAME,
        certificatesStub,
        dataProvidersStub,
        modelRolloutsStub,
        modelReleasesStub,
        requisitionsStub,
        dummyThrottler,
        TRUSTED_CERTIFICATES,
        POPULATION_BUCKETS_LIST,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()

    // Result should be only POPULATION_BUCKET_2 because it is the only bucket in this time range
    assertThat(result.population.value).isEqualTo(POPULATION_SIZE_2)
  }

  @Test
  fun `gets correct population of all males using different ModelRelease`() {
    modelReleasesServiceStub.stub {
      onBlocking { getModelRelease(any()) }.thenReturn(MODEL_RELEASE_2)
    }

    val requisitionSpec =
      REQUISITION_SPEC.copy {
        population =
          RequisitionSpecKt.population {
            filter = eventFilter { expression = "person.gender == ${Person.Gender.MALE_VALUE}" }
            interval = interval {
              startTime = VALID_START_TIME_1
              endTime = VALID_END_TIME_2
            }
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
        MC_NAME,
        certificatesStub,
        dataProvidersStub,
        modelRolloutsStub,
        modelReleasesStub,
        requisitionsStub,
        dummyThrottler,
        TRUSTED_CERTIFICATES,
        POPULATION_BUCKETS_LIST,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()

    // Result should be only POPULATION_BUCKET_2 because it is the only bucket of males using
    // MODEL_RELEASE_2
    assertThat(result.population.value).isEqualTo(POPULATION_SIZE_2)
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
      PdpData(
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
          interval = interval {
            startTime = VALID_START_TIME_1
            endTime = VALID_END_TIME_2
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
