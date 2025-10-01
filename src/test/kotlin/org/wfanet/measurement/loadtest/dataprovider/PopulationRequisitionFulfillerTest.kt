// Copyright 2024 The Cross-Media Measurement Authors
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
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.stub
import org.mockito.kotlin.wheneverBlocking
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.FulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt.ModelReleasesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt.ModelReleasesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.PopulationsGrpcKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.RefuseRequisitionRequest
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
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Dummy
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionResponse
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getPopulationRequest
import org.wfanet.measurement.api.v2alpha.listModelRolloutsResponse
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.modelRelease
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.population
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.size
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
import org.wfanet.measurement.dataprovider.DataProviderData
import org.wfanet.measurement.populationdataprovider.PopulationRequisitionFulfiller

@RunWith(JUnit4::class)
class PopulationRequisitionFulfillerTest {
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
        getCertificate(eq(getCertificateRequest { name = DATA_PROVIDER_CERTIFICATE.name }))
      }
      .thenReturn(DATA_PROVIDER_CERTIFICATE)
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

  private val modelRolloutsServiceMock: ModelRolloutsCoroutineImplBase = mockService {
    onBlocking { listModelRollouts(any()) }
      .thenReturn(listModelRolloutsResponse { modelRollouts += listOf(MODEL_ROLLOUT) })
  }

  private val modelReleasesServiceMock: ModelReleasesCoroutineImplBase = mockService {
    onBlocking { getModelRelease(any()) }.thenReturn(MODEL_RELEASE_1)
  }

  private val populationsServiceMock: PopulationsGrpcKt.PopulationsCoroutineImplBase = mockService {
    onBlocking { getPopulation(getPopulationRequest { name = POPULATION_NAME_1 }) } doReturn
      POPULATION_1
    onBlocking { getPopulation(getPopulationRequest { name = POPULATION_NAME_2 }) } doReturn
      POPULATION_2
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(certificatesServiceMock)
    addService(dataProvidersServiceMock)
    addService(requisitionsServiceMock)
    addService(modelRolloutsServiceMock)
    addService(modelReleasesServiceMock)
    addService(populationsServiceMock)
  }

  private val certificatesStub: CertificatesCoroutineStub by lazy {
    CertificatesCoroutineStub(grpcTestServerRule.channel)
  }

  private val requisitionsStub: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }

  private val modelRolloutsStub: ModelRolloutsCoroutineStub by lazy {
    ModelRolloutsCoroutineStub(grpcTestServerRule.channel)
  }

  private val modelReleasesStub: ModelReleasesCoroutineStub by lazy {
    ModelReleasesCoroutineStub(grpcTestServerRule.channel)
  }

  private val populationsStub by lazy {
    PopulationsGrpcKt.PopulationsCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun `fulfills requisition for 18-34 age range`() {
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += REQUISITION })
    }

    val requisitionFulfiller =
      PopulationRequisitionFulfiller(
        PDP_DATA,
        certificatesStub,
        requisitionsStub,
        dummyThrottler,
        TRUSTED_CERTIFICATES,
        modelRolloutsStub,
        modelReleasesStub,
        populationsStub,
        EVENT_MESSAGE_DESCRIPTOR,
      )

    runBlocking { requisitionFulfiller.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()

    // Result should be the value of SUB_POPULATION_1
    assertThat(result.population.value).isEqualTo(VID_RANGE_1.size())
  }

  @Test
  fun `fulfills requisition for males`() {
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

    val requisitionFulfiller =
      PopulationRequisitionFulfiller(
        PDP_DATA,
        certificatesStub,
        requisitionsStub,
        dummyThrottler,
        TRUSTED_CERTIFICATES,
        modelRolloutsStub,
        modelReleasesStub,
        populationsStub,
        EVENT_MESSAGE_DESCRIPTOR,
      )

    runBlocking { requisitionFulfiller.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()

    // Result should be the sum of SUB_POPULATION_1 and SUB_POPULATION_2
    assertThat(result.population.value).isEqualTo(VID_RANGE_1.size() + VID_RANGE_2.size())
  }

  @Test
  fun `fulfills requisition for males 35-54`() {
    val requisitionSpec =
      REQUISITION_SPEC.copy {
        population =
          RequisitionSpecKt.population {
            filter = eventFilter {
              expression =
                "person.gender == ${Person.Gender.MALE_VALUE} && person.age_group == ${Person.AgeGroup.YEARS_35_TO_54_VALUE}"
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

    val requisitionFulfiller =
      PopulationRequisitionFulfiller(
        PDP_DATA,
        certificatesStub,
        requisitionsStub,
        dummyThrottler,
        TRUSTED_CERTIFICATES,
        modelRolloutsStub,
        modelReleasesStub,
        populationsStub,
        EVENT_MESSAGE_DESCRIPTOR,
      )

    runBlocking { requisitionFulfiller.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()

    // Result should be the value of SUB_POPULATION_2
    assertThat(result.population.value).isEqualTo(VID_RANGE_2.size())
  }

  @Test
  fun `fulfills requisition for females using different ModelRelease`() {
    modelReleasesServiceMock.stub {
      onBlocking { getModelRelease(any()) }.thenReturn(MODEL_RELEASE_2)
    }

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

    val requisitionFulfiller =
      PopulationRequisitionFulfiller(
        PDP_DATA,
        certificatesStub,
        requisitionsStub,
        dummyThrottler,
        TRUSTED_CERTIFICATES,
        modelRolloutsStub,
        modelReleasesStub,
        populationsStub,
        EVENT_MESSAGE_DESCRIPTOR,
      )

    runBlocking { requisitionFulfiller.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()

    // Result should be SUB_POPULATION_3
    assertThat(result.population.value).isEqualTo(VID_RANGE_3.size())
  }

  @Test
  fun `fulfills requisition for females using field not part of population spec`() {
    modelReleasesServiceMock.stub {
      onBlocking { getModelRelease(any()) }.thenReturn(MODEL_RELEASE_2)
    }

    val requisitionSpec =
      REQUISITION_SPEC.copy {
        population =
          RequisitionSpecKt.population {
            filter = eventFilter {
              expression =
                "person.gender == ${Person.Gender.FEMALE_VALUE} && banner_ad.viewable == true"
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

    val requisitionFulfiller =
      PopulationRequisitionFulfiller(
        PDP_DATA,
        certificatesStub,
        requisitionsStub,
        dummyThrottler,
        TRUSTED_CERTIFICATES,
        modelRolloutsStub,
        modelReleasesStub,
        populationsStub,
        EVENT_MESSAGE_DESCRIPTOR,
      )

    runBlocking { requisitionFulfiller.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()

    // Population value calculated with requisition spec that contains banner_ad field in the filter
    // expression should be equal to that which does not contain that field. banner_ad field should
    // just be ignored. The result of this test should be the same as test `fulfills requisition for
    // females using different ModelRelease`
    assertThat(result.population.value).isEqualTo(VID_RANGE_3.size())
  }

  @Test
  fun `refuses requisition when attribute is not part of event descriptor`() {
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
    wheneverBlocking {
      populationsServiceMock.getPopulation(getPopulationRequest { name = POPULATION_NAME_1 })
    } doReturn POPULATION_1.copy { populationSpec = INVALID_POPULATION_SPEC_1 }

    val requisitionFulfiller =
      PopulationRequisitionFulfiller(
        PDP_DATA,
        certificatesStub,
        requisitionsStub,
        dummyThrottler,
        TRUSTED_CERTIFICATES,
        modelRolloutsStub,
        modelReleasesStub,
        populationsStub,
        EVENT_MESSAGE_DESCRIPTOR,
      )
    runBlocking { requisitionFulfiller.executeRequisitionFulfillingWorkflow() }

    val refuseRequisitionRequest: RefuseRequisitionRequest =
      verifyAndCapture(requisitionsServiceMock, RequisitionsCoroutineImplBase::refuseRequisition)
    assertThat(refuseRequisitionRequest.refusal.justification)
      .isEqualTo(Requisition.Refusal.Justification.UNFULFILLABLE)
    assertThat(refuseRequisitionRequest.refusal.message).contains("PopulationSpec")
  }

  @Test
  fun `refuses requisition when filter expression is not in operative field in population info`() {
    val requisitionSpec =
      REQUISITION_SPEC.copy {
        population =
          RequisitionSpecKt.population {
            filter = eventFilter {
              expression =
                "person.gender == ${Person.Gender.FEMALE_VALUE} && other_field.value == other_value"
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

    val requisitionFulfiller =
      PopulationRequisitionFulfiller(
        PDP_DATA,
        certificatesStub,
        requisitionsStub,
        dummyThrottler,
        TRUSTED_CERTIFICATES,
        modelRolloutsStub,
        modelReleasesStub,
        populationsStub,
        EVENT_MESSAGE_DESCRIPTOR,
      )

    runBlocking { requisitionFulfiller.executeRequisitionFulfillingWorkflow() }

    val refuseRequisitionRequest: RefuseRequisitionRequest =
      verifyAndCapture(requisitionsServiceMock, RequisitionsCoroutineImplBase::refuseRequisition)
    assertThat(refuseRequisitionRequest.refusal.justification)
      .isEqualTo(Requisition.Refusal.Justification.SPEC_INVALID)
    assertThat(refuseRequisitionRequest.refusal.message).contains("filter")
  }

  companion object {
    private const val MC_ID = "mc"
    private const val MC_NAME = "measurementConsumers/$MC_ID"
    private const val PDP_DISPLAY_NAME = "pdp1"
    private val SECRET_FILES_PATH: Path =
      checkNotNull(
        getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )
      )
    private val EVENT_MESSAGE_DESCRIPTOR = TestEvent.getDescriptor()
    private const val PDP_ID = "somePopulationDataProvider"
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

    private val CREATE_TIME_1: Timestamp = Instant.ofEpochSecond(123).toProtoTime()
    private val CREATE_TIME_2: Timestamp = Instant.ofEpochSecond(456).toProtoTime()

    private const val POPULATION_NAME_1 = "$PDP_NAME/populations/AAAAAAAAAHs"
    private const val POPULATION_NAME_2 = "$PDP_NAME/populations/AAAAAAAAAJs"

    private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"
    private const val MODEL_SUITE_NAME = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAHs"
    private const val MODEL_RELEASE_NAME_1 = "$MODEL_SUITE_NAME/modelReleases/AAAAAAAAAHs"
    private const val MODEL_RELEASE_NAME_2 = "$MODEL_SUITE_NAME/modelReleases/AAAAAAAAAJs"

    private const val MODEL_LINE_NAME = "${MODEL_SUITE_NAME}/modelLines/AAAAAAAAAHs"
    private const val MODEL_ROLLOUT_NAME = "${MODEL_LINE_NAME}/modelRollouts/AAAAAAAAAHs"

    private val MODEL_RELEASE_1 = modelRelease {
      name = MODEL_RELEASE_NAME_1
      createTime = CREATE_TIME_1
      population = POPULATION_NAME_1
    }

    private val MODEL_RELEASE_2 = modelRelease {
      name = MODEL_RELEASE_NAME_2
      createTime = CREATE_TIME_2
      population = POPULATION_NAME_2
    }

    private val MODEL_ROLLOUT = modelRollout {
      name = MODEL_ROLLOUT_NAME
      modelRelease = MODEL_RELEASE_NAME_1
    }

    private val PERSON_1 = person {
      ageGroup = Person.AgeGroup.YEARS_18_TO_34
      gender = Person.Gender.MALE
      socialGradeGroup = Person.SocialGradeGroup.A_B_C1
    }

    private val PERSON_2 = person {
      ageGroup = Person.AgeGroup.YEARS_35_TO_54
      gender = Person.Gender.MALE
      socialGradeGroup = Person.SocialGradeGroup.A_B_C1
    }

    private val PERSON_3 = person {
      ageGroup = Person.AgeGroup.YEARS_18_TO_34
      gender = Person.Gender.FEMALE
      socialGradeGroup = Person.SocialGradeGroup.A_B_C1
    }

    private val ATTRIBUTE_1 = PERSON_1.pack()

    private val ATTRIBUTE_2 = PERSON_2.pack()

    private val ATTRIBUTE_3 = PERSON_3.pack()

    private val INVALID_ATTRIBUTE_1 = Dummy.getDefaultInstance().pack()

    private val VID_RANGE_1 = vidRange {
      startVid = 1
      endVidInclusive = 100
    }

    private val VID_RANGE_2 = vidRange {
      startVid = 101
      endVidInclusive = 300
    }

    private val VID_RANGE_3 = vidRange {
      startVid = 301
      endVidInclusive = 600
    }

    // Male 18-34
    private val SUB_POPULATION_1 = subPopulation {
      attributes += listOf(ATTRIBUTE_1)
      vidRanges += listOf(VID_RANGE_1)
    }

    // Male 35-54
    private val SUB_POPULATION_2 = subPopulation {
      attributes += listOf(ATTRIBUTE_2)
      vidRanges += listOf(VID_RANGE_2)
    }

    // Female 18-34
    private val SUB_POPULATION_3 = subPopulation {
      attributes += listOf(ATTRIBUTE_3)
      vidRanges += listOf(VID_RANGE_3)
    }

    private val POPULATION_SPEC_1 = populationSpec {
      subpopulations += SUB_POPULATION_1
      subpopulations += SUB_POPULATION_2
    }

    private val POPULATION_SPEC_2 = populationSpec { subpopulations += SUB_POPULATION_3 }

    private val INVALID_POPULATION_SPEC_1 = populationSpec {
      subpopulations +=
        listOf(
          subPopulation {
            attributes += listOf(INVALID_ATTRIBUTE_1)
            vidRanges += listOf(VID_RANGE_1)
          }
        )
    }

    private val POPULATION_1 = population {
      name = POPULATION_NAME_1
      populationSpec = POPULATION_SPEC_1
    }

    private val POPULATION_2 = population {
      name = POPULATION_NAME_2
      populationSpec = POPULATION_SPEC_2
    }

    private val MC_SIGNING_KEY: SigningKeyHandle =
      loadSigningKey("${MC_ID}_cs_cert.der", "${MC_ID}_cs_private.der")
    private val PDP_SIGNING_KEY: SigningKeyHandle =
      loadSigningKey("${PDP_DISPLAY_NAME}_cs_cert.der", "${PDP_DISPLAY_NAME}_cs_private.der")
    private val DATA_PROVIDER_CERTIFICATE_KEY: DataProviderCertificateKey =
      DataProviderCertificateKey(PDP_ID, externalIdToApiId(8L))

    private val DATA_PROVIDER_CERTIFICATE: Certificate = certificate {
      name = DATA_PROVIDER_CERTIFICATE_KEY.toName()
      x509Der = PDP_SIGNING_KEY.certificate.encoded.toByteString()
      subjectKeyIdentifier = PDP_SIGNING_KEY.certificate.subjectKeyIdentifier!!
    }

    private val PDP_DATA =
      DataProviderData(
        PDP_NAME,
        loadEncryptionPrivateKey("${PDP_DISPLAY_NAME}_enc_private.tink"),
        PDP_SIGNING_KEY,
        DATA_PROVIDER_CERTIFICATE_KEY,
      )

    private val MC_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val MC_PRIVATE_KEY: TinkPrivateKeyHandle =
      loadPrivateKey(SECRET_FILES_PATH.resolve("mc_enc_private.tink").toFile())
    private val DATA_PROVIDER_PUBLIC_KEY: EncryptionPublicKey =
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
      nonce = Random.nextLong()
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
