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
import com.google.common.truth.extensions.proto.FieldScopes
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.type.interval
import io.grpc.Status
import java.lang.UnsupportedOperationException
import java.nio.file.Path
import java.nio.file.Paths
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.anysketch.Sketch
import org.wfanet.anysketch.crypto.ElGamalPublicKey
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.CreateEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.CreateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.FulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionResponse
import org.wfanet.measurement.api.v2alpha.GetEventGroupRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.RefuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.ReplaceDataAvailabilityIntervalRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.liquidLegionsV2
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.value
import org.wfanet.measurement.api.v2alpha.RequisitionKt.duchyEntry
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.dataProvider
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.elGamalPublicKey
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessage
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.copy
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Banner
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Video
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionResponse
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.liquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.testing.MeasurementResultSubject.Companion.assertThat
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.HexString
import org.wfanet.measurement.common.OpenEndTimeRange
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
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.testing.verifyAndCapture
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.duchy.computeRequisitionFingerprint
import org.wfanet.measurement.consent.client.duchy.signElgamalPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.decryptMetadata
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AcdpCharge
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AgeGroup as PrivacyLandscapeAge
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.CompositionMechanism
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.DpCharge
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Gender as PrivacyLandscapeGender
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketFilter
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetBalanceEntry
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManager
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.TestInMemoryBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.TestPrivacyBucketMapper
import org.wfanet.measurement.integration.common.SyntheticGenerationSpecs
import org.wfanet.measurement.loadtest.config.EventGroupMetadata
import org.wfanet.measurement.loadtest.config.TestIdentifiers

private const val MC_ID = "mc"
private const val MC_NAME = "measurementConsumers/$MC_ID"
private const val EDP_DISPLAY_NAME = "edp1"
private val SECRET_FILES_PATH: Path =
  checkNotNull(
    getRuntimePath(
      Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
    )
  )
private const val EDP_ID = "someDataProvider"
private const val EDP_NAME = "dataProviders/$EDP_ID"

private const val LLV2_DECAY_RATE = 12.0
private const val LLV2_MAX_SIZE = 100_000L
private val NOISE_MECHANISM = ProtocolConfig.NoiseMechanism.DISCRETE_GAUSSIAN

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

private val CONSENT_SIGNALING_ELGAMAL_PUBLIC_KEY = elGamalPublicKey {
  ellipticCurveId = 415
  generator = HexString("036B17D1F2E12C4247F8BCE6E563A440F277037D812DEB33A0F4A13945D898C296").bytes
  element = HexString("0277BF406C5AA4376413E480E0AB8B0EFCA999D362204E6D1686E0BE567811604D").bytes
}

private val LAST_EVENT_DATE = LocalDate.now()
private val FIRST_EVENT_DATE = LAST_EVENT_DATE.minusDays(1)
private val TIME_RANGE = OpenEndTimeRange.fromClosedDateRange(FIRST_EVENT_DATE..LAST_EVENT_DATE)

private const val DUCHY_ID = "worker1"
private const val RANDOM_SEED: Long = 0
private val COMPOSITION_MECHANISM = CompositionMechanism.ACDP

// Resource ID for EventGroup that fails Requisitions with CONSENT_SIGNAL_INVALID if used.
private const val CONSENT_SIGNAL_INVALID_EVENT_GROUP_ID = "consent-signal-invalid"
// Resource ID for EventGroup that fails Requisitions with SPEC_INVALID if used.
private const val SPEC_INVALID_EVENT_GROUP_ID = "spec-invalid"
// Resource ID for EventGroup that fails Requisitions with INSUFFICIENT_PRIVACY_BUDGET if used.
private const val INSUFFICIENT_PRIVACY_BUDGET_EVENT_GROUP_ID = "insufficient-privacy-budget"
// Resource ID for EventGroup that fails Requisitions with UNFULFILLABLE if used.
private const val UNFULFILLABLE_EVENT_GROUP_ID = "unfulfillable"
// Resource ID for EventGroup that fails Requisitions with DECLINED if used.
private const val DECLINED_EVENT_GROUP_ID = "declined"

@RunWith(JUnit4::class)
class EdpSimulatorTest {
  private val certificatesServiceMock: CertificatesCoroutineImplBase = mockService {
    onBlocking {
        getCertificate(eq(getCertificateRequest { name = MEASUREMENT_CONSUMER_CERTIFICATE_NAME }))
      }
      .thenReturn(MEASUREMENT_CONSUMER_CERTIFICATE)
    onBlocking { getCertificate(eq(getCertificateRequest { name = DUCHY_CERTIFICATE.name })) }
      .thenReturn(DUCHY_CERTIFICATE)
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
  private val measurementConsumersServiceMock:
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
  private val eventGroupsServiceMock: EventGroupsCoroutineImplBase = mockService {
    onBlocking { getEventGroup(any()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<GetEventGroupRequest>(0)
        eventGroup {
          name = request.name
          eventGroupReferenceId = TestIdentifiers.SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX
        }
      }
  }
  private val eventGroupMetadataDescriptorsServiceMock:
    EventGroupMetadataDescriptorsCoroutineImplBase =
    mockService {
      onBlocking { createEventGroupMetadataDescriptor(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<CreateEventGroupMetadataDescriptorRequest>(0)
          request.eventGroupMetadataDescriptor.copy { name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME }
        }
    }
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { listRequisitions(any()) }
      .thenReturn(listRequisitionsResponse { requisitions += REQUISITION })
    onBlocking { fulfillDirectRequisition(any()) }.thenReturn(fulfillDirectRequisitionResponse {})
  }

  private val fakeRequisitionFulfillmentService = FakeRequisitionFulfillmentService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(measurementConsumersServiceMock)
    addService(certificatesServiceMock)
    addService(dataProvidersServiceMock)
    addService(eventGroupsServiceMock)
    addService(eventGroupMetadataDescriptorsServiceMock)
    addService(requisitionsServiceMock)
    addService(fakeRequisitionFulfillmentService)
  }

  private val measurementConsumersStub by lazy {
    MeasurementConsumersCoroutineStub(grpcTestServerRule.channel)
  }

  private val certificatesStub: CertificatesCoroutineStub by lazy {
    CertificatesCoroutineStub(grpcTestServerRule.channel)
  }

  private val dataProvidersStub: DataProvidersCoroutineStub by lazy {
    DataProvidersCoroutineStub(grpcTestServerRule.channel)
  }

  private val eventGroupsStub: EventGroupsCoroutineStub by lazy {
    EventGroupsCoroutineStub(grpcTestServerRule.channel)
  }

  private val eventGroupMetadataDescriptorsStub by lazy {
    EventGroupMetadataDescriptorsCoroutineStub(grpcTestServerRule.channel)
  }

  private val requisitionsStub: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }

  private val requisitionFulfillmentStub: RequisitionFulfillmentCoroutineStub by lazy {
    RequisitionFulfillmentCoroutineStub(grpcTestServerRule.channel)
  }

  private val backingStore = TestInMemoryBackingStore()
  private val privacyBudgetManager =
    PrivacyBudgetManager(PrivacyBucketFilter(TestPrivacyBucketMapper()), backingStore, 10.0f, 0.02f)

  private fun generateEvents(
    vidRange: LongRange,
    date: LocalDate,
    ageGroup: Person.AgeGroup,
    gender: Person.Gender,
  ): List<LabeledTestEvent> {
    val timestamp = date.atStartOfDay().toInstant(ZoneOffset.UTC)
    val message = testEvent {
      person = person {
        this.ageGroup = ageGroup
        this.gender = gender
      }
    }
    return vidRange.map { vid -> LabeledTestEvent(timestamp, vid, message) }
  }

  @Test
  fun `ensureEventGroup creates EventGroup and EventGroupMetadataDescriptor`() {
    val edpSimulator =
      EdpSimulator(
        EDP_DATA,
        MEASUREMENT_CONSUMER_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        InMemoryEventQuery(emptyList()),
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking { edpSimulator.ensureEventGroup(SYNTHETIC_DATA_SPEC) }

    // Verify metadata descriptor set contains synthetic data spec.
    val createDescriptorRequest: CreateEventGroupMetadataDescriptorRequest =
      verifyAndCapture(
        eventGroupMetadataDescriptorsServiceMock,
        EventGroupMetadataDescriptorsCoroutineImplBase::createEventGroupMetadataDescriptor,
      )
    val descriptors =
      ProtoReflection.buildDescriptors(
        listOf(createDescriptorRequest.eventGroupMetadataDescriptor.descriptorSet)
      )
    assertThat(descriptors.map { it.fullName })
      .contains(SyntheticEventGroupSpec.getDescriptor().fullName)

    // Verify EventGroup metadata.
    val createRequest: CreateEventGroupRequest =
      verifyAndCapture(eventGroupsServiceMock, EventGroupsCoroutineImplBase::createEventGroup)
    val metadata: EventGroup.Metadata =
      decryptMetadata(createRequest.eventGroup.encryptedMetadata, MC_PRIVATE_KEY)
    assertThat(metadata.eventGroupMetadataDescriptor)
      .isEqualTo(EVENT_GROUP_METADATA_DESCRIPTOR_NAME)
    assertThat(metadata.metadata.unpack(SyntheticEventGroupSpec::class.java))
      .isEqualTo(SYNTHETIC_DATA_SPEC)

    // Verify EventGroup has correct template types.
    assertThat(createRequest.eventGroup.eventTemplatesList.map { it.type })
      .containsAtLeast(
        Person.getDescriptor().fullName,
        Video.getDescriptor().fullName,
        Banner.getDescriptor().fullName,
      )
  }

  @Test
  fun `ensureEventGroup updates EventGroup`() {
    eventGroupsServiceMock.stub {
      onBlocking { listEventGroups(any()) }
        .thenReturn(
          listEventGroupsResponse {
            eventGroups += eventGroup {
              name = EVENT_GROUP_NAME
              measurementConsumer = MEASUREMENT_CONSUMER_NAME
              eventGroupReferenceId =
                "${TestIdentifiers.SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX}-${EDP_DATA.displayName}"
            }
          }
        )
    }
    val edpSimulator =
      EdpSimulator(
        EDP_DATA,
        MEASUREMENT_CONSUMER_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        InMemoryEventQuery(emptyList()),
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking { edpSimulator.ensureEventGroup(SYNTHETIC_DATA_SPEC) }

    // Verify EventGroup metadata has correct type.
    val updateRequest: UpdateEventGroupRequest =
      verifyAndCapture(eventGroupsServiceMock, EventGroupsCoroutineImplBase::updateEventGroup)
    val metadata: EventGroup.Metadata =
      decryptMetadata(updateRequest.eventGroup.encryptedMetadata, MC_PRIVATE_KEY)
    assertThat(metadata.eventGroupMetadataDescriptor)
      .isEqualTo(EVENT_GROUP_METADATA_DESCRIPTOR_NAME)
    assertThat(metadata.metadata.unpack(SyntheticEventGroupSpec::class.java))
      .isEqualTo(SYNTHETIC_DATA_SPEC)

    // Verify EventGroup has correct template types.
    assertThat(updateRequest.eventGroup.eventTemplatesList.map { it.type })
      .containsAtLeast(
        Person.getDescriptor().fullName,
        Video.getDescriptor().fullName,
        Banner.getDescriptor().fullName,
      )
  }

  @Test
  fun `ensureEventGroup updates EventGroupMetadataDescriptor`() {
    eventGroupMetadataDescriptorsServiceMock.stub {
      onBlocking { createEventGroupMetadataDescriptor(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<CreateEventGroupMetadataDescriptorRequest>(0)
          request.eventGroupMetadataDescriptor.copy {
            descriptorSet =
              ProtoReflection.buildFileDescriptorSet(TestMetadataMessage.getDescriptor())
          }
        }
    }
    val edpSimulator =
      EdpSimulator(
        EDP_DATA,
        MEASUREMENT_CONSUMER_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        InMemoryEventQuery(emptyList()),
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking { edpSimulator.ensureEventGroup(SYNTHETIC_DATA_SPEC) }

    val updateRequest: UpdateEventGroupMetadataDescriptorRequest =
      verifyAndCapture(
        eventGroupMetadataDescriptorsServiceMock,
        EventGroupMetadataDescriptorsCoroutineImplBase::updateEventGroupMetadataDescriptor,
      )
    assertThat(updateRequest.eventGroupMetadataDescriptor.descriptorSet)
      .isEqualTo(ProtoReflection.buildFileDescriptorSet(SyntheticEventGroupSpec.getDescriptor()))
  }

  @Test
  fun `ensureEventGroups creates multiple EventGroups`() {
    val edpSimulator =
      EdpSimulator(
        EDP_DATA,
        MEASUREMENT_CONSUMER_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        InMemoryEventQuery(emptyList()),
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        compositionMechanism = COMPOSITION_MECHANISM,
      )
    val metadataByReferenceIdSuffix =
      mapOf(
        "-foo" to SyntheticGenerationSpecs.SYNTHETIC_DATA_SPECS[0],
        "-bar" to SyntheticGenerationSpecs.SYNTHETIC_DATA_SPECS[1],
      )

    runBlocking { edpSimulator.ensureEventGroups(metadataByReferenceIdSuffix) }

    // Verify metadata descriptor set contains synthetic data spec.
    val createDescriptorRequest: CreateEventGroupMetadataDescriptorRequest =
      verifyAndCapture(
        eventGroupMetadataDescriptorsServiceMock,
        EventGroupMetadataDescriptorsCoroutineImplBase::createEventGroupMetadataDescriptor,
      )
    val descriptors =
      ProtoReflection.buildDescriptors(
        listOf(createDescriptorRequest.eventGroupMetadataDescriptor.descriptorSet)
      )
    assertThat(descriptors.map { it.fullName })
      .contains(SyntheticEventGroupSpec.getDescriptor().fullName)

    // Verify EventGroup metadata.
    val createRequests: List<CreateEventGroupRequest> =
      verifyAndCapture(
        eventGroupsServiceMock,
        EventGroupsCoroutineImplBase::createEventGroup,
        times(2),
      )
    for (createRequest in createRequests) {
      val metadata: EventGroup.Metadata =
        decryptMetadata(createRequest.eventGroup.encryptedMetadata, MC_PRIVATE_KEY)
      assertThat(metadata.eventGroupMetadataDescriptor)
        .isEqualTo(EVENT_GROUP_METADATA_DESCRIPTOR_NAME)
      assertThat(metadata.metadata.unpack(SyntheticEventGroupSpec::class.java))
        .isEqualTo(
          metadataByReferenceIdSuffix.getValue(
            EdpSimulator.getEventGroupReferenceIdSuffix(createRequest.eventGroup, EDP_DISPLAY_NAME)
          )
        )
    }
  }

  @Test
  fun `ensureEventGroups throws IllegalArgumentException when metadata message types mismatch`() {
    val edpSimulator =
      EdpSimulator(
        EDP_DATA,
        MEASUREMENT_CONSUMER_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        InMemoryEventQuery(emptyList()),
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        compositionMechanism = COMPOSITION_MECHANISM,
      )
    val metadataByReferenceIdSuffix = mapOf("-foo" to SYNTHETIC_DATA_SPEC, "-bar" to TEST_METADATA)

    val exception =
      assertFailsWith<IllegalArgumentException> {
        runBlocking { edpSimulator.ensureEventGroups(metadataByReferenceIdSuffix) }
      }

    assertThat(exception).hasMessageThat().contains("type")
  }

  @Test
  fun `ignores Requisitions for other MeasurementConsumers`() {
    val allEvents =
      generateEvents(
        1L..10L,
        FIRST_EVENT_DATE,
        Person.AgeGroup.YEARS_18_TO_34,
        Person.Gender.FEMALE,
      ) +
        generateEvents(
          11L..15L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_35_TO_54,
          Person.Gender.FEMALE,
        ) +
        generateEvents(
          16L..20L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_55_PLUS,
          Person.Gender.FEMALE,
        ) +
        generateEvents(
          21L..25L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_18_TO_34,
          Person.Gender.MALE,
        ) +
        generateEvents(
          26L..30L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_35_TO_54,
          Person.Gender.MALE,
        )

    val edpSimulator =
      EdpSimulator(
        EDP_DATA,
        "measurementConsumers/differentMcId",
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        InMemoryEventQuery(allEvents),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking {
      edpSimulator.ensureEventGroup(SYNTHETIC_DATA_SPEC)
      edpSimulator.executeRequisitionFulfillingWorkflow()
    }

    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
    verifyBlocking(requisitionsServiceMock, never()) { fulfillDirectRequisition(any()) }
  }

  @Test
  fun `charges privacy budget with Geometric noise and DP_ADVANCED composition mechanism for mpc reach and frequency Requisition`() {
    val measurementSpec =
      MEASUREMENT_SPEC.copy {
        vidSamplingInterval =
          vidSamplingInterval.copy {
            start = 0.0f
            width = PRIVACY_BUCKET_VID_SAMPLE_WIDTH
          }
      }
    val requisitionGeometric =
      REQUISITION.copy {
        this.measurementSpec = signMeasurementSpec(measurementSpec, MC_SIGNING_KEY)
        protocolConfig =
          protocolConfig.copy {
            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol {
                liquidLegionsV2 =
                  ProtocolConfigKt.liquidLegionsV2 {
                    noiseMechanism = ProtocolConfig.NoiseMechanism.GEOMETRIC
                    sketchParams = liquidLegionsSketchParams {
                      decayRate = LLV2_DECAY_RATE
                      maxSize = LLV2_MAX_SIZE
                      samplingIndicatorSize = 10_000_000
                    }
                    ellipticCurveId = 415
                  }
              }
          }
      }

    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisitionGeometric })
    }

    val matchingEvents =
      generateEvents(
        1L..10L,
        FIRST_EVENT_DATE,
        Person.AgeGroup.YEARS_18_TO_34,
        Person.Gender.FEMALE,
      )
    val nonMatchingEvents =
      generateEvents(
        11L..15L,
        FIRST_EVENT_DATE,
        Person.AgeGroup.YEARS_35_TO_54,
        Person.Gender.FEMALE,
      ) +
        generateEvents(
          16L..20L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_55_PLUS,
          Person.Gender.FEMALE,
        ) +
        generateEvents(
          21L..25L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_18_TO_34,
          Person.Gender.MALE,
        ) +
        generateEvents(
          26L..30L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_35_TO_54,
          Person.Gender.MALE,
        )

    val allEvents = matchingEvents + nonMatchingEvents

    val edpSimulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        InMemoryEventQuery(allEvents),
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        compositionMechanism = CompositionMechanism.DP_ADVANCED,
      )
    runBlocking {
      edpSimulator.ensureEventGroup(TEST_METADATA)
      edpSimulator.executeRequisitionFulfillingWorkflow()
    }

    val balanceLedger: Map<PrivacyBucketGroup, MutableMap<DpCharge, PrivacyBudgetBalanceEntry>> =
      backingStore.getDpBalancesMap()

    // Verify that each bucket is only charged once.
    for (bucketBalances in balanceLedger.values) {
      assertThat(bucketBalances).hasSize(1)
      for (balanceEntry in bucketBalances.values) {
        assertThat(balanceEntry.repetitionCount).isEqualTo(1)
      }
    }

    // The list of all the charged privacy bucket groups should be correct based on the filter.
    assertThat(balanceLedger.keys)
      .containsExactly(
        PrivacyBucketGroup(
          MC_NAME,
          FIRST_EVENT_DATE,
          FIRST_EVENT_DATE,
          PrivacyLandscapeAge.RANGE_18_34,
          PrivacyLandscapeGender.FEMALE,
          0.0f,
          PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
        ),
        PrivacyBucketGroup(
          MC_NAME,
          LAST_EVENT_DATE,
          LAST_EVENT_DATE,
          PrivacyLandscapeAge.RANGE_18_34,
          PrivacyLandscapeGender.FEMALE,
          0.0f,
          PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
        ),
        PrivacyBucketGroup(
          MC_NAME,
          FIRST_EVENT_DATE,
          FIRST_EVENT_DATE,
          PrivacyLandscapeAge.RANGE_18_34,
          PrivacyLandscapeGender.FEMALE,
          PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
          PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
        ),
        PrivacyBucketGroup(
          MC_NAME,
          LAST_EVENT_DATE,
          LAST_EVENT_DATE,
          PrivacyLandscapeAge.RANGE_18_34,
          PrivacyLandscapeGender.FEMALE,
          PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
          PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
        ),
      )
  }

  @Test
  fun `charges privacy budget with discrete Gaussian noise and ACDP composition mechanism for mpc reach and frequency Requisition`() {
    runBlocking {
      val measurementSpec =
        MEASUREMENT_SPEC.copy {
          vidSamplingInterval =
            vidSamplingInterval.copy {
              start = 0.0f
              width = PRIVACY_BUCKET_VID_SAMPLE_WIDTH
            }
        }
      val requisition =
        REQUISITION.copy {
          this.measurementSpec = signMeasurementSpec(measurementSpec, MC_SIGNING_KEY)
        }
      requisitionsServiceMock.stub {
        onBlocking { listRequisitions(any()) }
          .thenReturn(listRequisitionsResponse { requisitions += requisition })
      }

      val matchingEvents =
        generateEvents(
          1L..10L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_18_TO_34,
          Person.Gender.FEMALE,
        )
      val nonMatchingEvents =
        generateEvents(
          11L..15L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_35_TO_54,
          Person.Gender.FEMALE,
        ) +
          generateEvents(
            16L..20L,
            FIRST_EVENT_DATE,
            Person.AgeGroup.YEARS_55_PLUS,
            Person.Gender.FEMALE,
          ) +
          generateEvents(
            21L..25L,
            FIRST_EVENT_DATE,
            Person.AgeGroup.YEARS_18_TO_34,
            Person.Gender.MALE,
          ) +
          generateEvents(
            26L..30L,
            FIRST_EVENT_DATE,
            Person.AgeGroup.YEARS_35_TO_54,
            Person.Gender.MALE,
          )

      val allEvents = matchingEvents + nonMatchingEvents

      val edpSimulator =
        EdpSimulator(
          EDP_DATA,
          MC_NAME,
          measurementConsumersStub,
          certificatesStub,
          dataProvidersStub,
          eventGroupsStub,
          eventGroupMetadataDescriptorsStub,
          requisitionsStub,
          requisitionFulfillmentStub,
          InMemoryEventQuery(allEvents),
          dummyThrottler,
          privacyBudgetManager,
          TRUSTED_CERTIFICATES,
          compositionMechanism = CompositionMechanism.ACDP,
        )
      runBlocking {
        edpSimulator.ensureEventGroup(TEST_METADATA)
        edpSimulator.executeRequisitionFulfillingWorkflow()
      }

      val acdpBalancesMap: Map<PrivacyBucketGroup, AcdpCharge> = backingStore.getAcdpBalancesMap()

      // reach and frequency delta, epsilon, contributorCount: epsilon = 2.0, delta = 2E-12,
      // contributorCount = 1
      for (acdpCharge in acdpBalancesMap.values) {
        assertThat(acdpCharge.rho).isEqualTo(0.035901274080426)
        assertThat(acdpCharge.theta).isEqualTo(7.715411332048879E-14)
      }

      // The list of all the charged privacy bucket groups should be correct based on the filter.
      assertThat(acdpBalancesMap.keys)
        .containsExactly(
          PrivacyBucketGroup(
            MC_NAME,
            FIRST_EVENT_DATE,
            FIRST_EVENT_DATE,
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.FEMALE,
            0.0f,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LAST_EVENT_DATE,
            LAST_EVENT_DATE,
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.FEMALE,
            0.0f,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
          ),
          PrivacyBucketGroup(
            MC_NAME,
            FIRST_EVENT_DATE,
            FIRST_EVENT_DATE,
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.FEMALE,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LAST_EVENT_DATE,
            LAST_EVENT_DATE,
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.FEMALE,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
          ),
        )
    }
  }

  @Test
  fun `charges privacy budget with Gaussian noise and ACDP composition mechanism for direct reach and frequency Requisition`() {
    runBlocking {
      val measurementSpec =
        MEASUREMENT_SPEC.copy {
          vidSamplingInterval =
            vidSamplingInterval.copy {
              start = 0.0f
              width = PRIVACY_BUCKET_VID_SAMPLE_WIDTH
            }
        }
      val noiseMechanismOption = ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN

      val requisition =
        REQUISITION.copy {
          this.measurementSpec = signMeasurementSpec(measurementSpec, MC_SIGNING_KEY)
          protocolConfig =
            protocolConfig.copy {
              protocols.clear()
              protocols +=
                ProtocolConfigKt.protocol {
                  direct =
                    ProtocolConfigKt.direct {
                      noiseMechanisms += noiseMechanismOption
                      customDirectMethodology =
                        ProtocolConfig.Direct.CustomDirectMethodology.getDefaultInstance()
                      deterministicCountDistinct =
                        ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
                      deterministicDistribution =
                        ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
                    }
                }
            }
        }

      requisitionsServiceMock.stub {
        onBlocking { listRequisitions(any()) }
          .thenReturn(listRequisitionsResponse { requisitions += requisition })
      }

      val matchingEvents =
        generateEvents(
          1L..10L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_18_TO_34,
          Person.Gender.FEMALE,
        )
      val nonMatchingEvents =
        generateEvents(
          11L..15L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_35_TO_54,
          Person.Gender.FEMALE,
        ) +
          generateEvents(
            16L..20L,
            FIRST_EVENT_DATE,
            Person.AgeGroup.YEARS_55_PLUS,
            Person.Gender.FEMALE,
          ) +
          generateEvents(
            21L..25L,
            FIRST_EVENT_DATE,
            Person.AgeGroup.YEARS_18_TO_34,
            Person.Gender.MALE,
          ) +
          generateEvents(
            26L..30L,
            FIRST_EVENT_DATE,
            Person.AgeGroup.YEARS_35_TO_54,
            Person.Gender.MALE,
          )

      val allEvents = matchingEvents + nonMatchingEvents

      val edpSimulator =
        EdpSimulator(
          EDP_DATA,
          MC_NAME,
          measurementConsumersStub,
          certificatesStub,
          dataProvidersStub,
          eventGroupsStub,
          eventGroupMetadataDescriptorsStub,
          requisitionsStub,
          requisitionFulfillmentStub,
          InMemoryEventQuery(allEvents),
          dummyThrottler,
          privacyBudgetManager,
          TRUSTED_CERTIFICATES,
          compositionMechanism = CompositionMechanism.ACDP,
        )
      runBlocking {
        edpSimulator.ensureEventGroup(TEST_METADATA)
        edpSimulator.executeRequisitionFulfillingWorkflow()
      }

      val acdpBalancesMap: Map<PrivacyBucketGroup, AcdpCharge> = backingStore.getAcdpBalancesMap()

      // reach and frequency delta, epsilon: epsilon = 2.0, delta = 2E-12,
      for (acdpCharge in acdpBalancesMap.values) {
        assertThat(acdpCharge.rho).isEqualTo(0.04552935394838453)
        assertThat(acdpCharge.theta).isEqualTo(0.0)
      }

      // The list of all the charged privacy bucket groups should be correct based on the filter.
      assertThat(acdpBalancesMap.keys)
        .containsExactly(
          PrivacyBucketGroup(
            MC_NAME,
            FIRST_EVENT_DATE,
            FIRST_EVENT_DATE,
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.FEMALE,
            0.0f,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LAST_EVENT_DATE,
            LAST_EVENT_DATE,
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.FEMALE,
            0.0f,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
          ),
          PrivacyBucketGroup(
            MC_NAME,
            FIRST_EVENT_DATE,
            FIRST_EVENT_DATE,
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.FEMALE,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
          ),
          PrivacyBucketGroup(
            MC_NAME,
            LAST_EVENT_DATE,
            LAST_EVENT_DATE,
            PrivacyLandscapeAge.RANGE_18_34,
            PrivacyLandscapeGender.FEMALE,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
            PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
          ),
        )
    }
  }

  @Test
  fun `fulfills reach and frequency Requisition`() {
    val events =
      generateEvents(
        1L..10L,
        FIRST_EVENT_DATE,
        Person.AgeGroup.YEARS_18_TO_34,
        Person.Gender.FEMALE,
      ) +
        generateEvents(
          11L..15L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_35_TO_54,
          Person.Gender.FEMALE,
        ) +
        generateEvents(
          16L..20L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_55_PLUS,
          Person.Gender.FEMALE,
        ) +
        generateEvents(
          21L..25L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_18_TO_34,
          Person.Gender.MALE,
        ) +
        generateEvents(
          26L..30L,
          FIRST_EVENT_DATE,
          Person.AgeGroup.YEARS_35_TO_54,
          Person.Gender.MALE,
        )
    val eventQuery = InMemoryEventQuery(events)
    val edpSimulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        eventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        sketchEncrypter = fakeSketchEncrypter,
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking { edpSimulator.executeRequisitionFulfillingWorkflow() }

    val requests: List<FulfillRequisitionRequest> =
      fakeRequisitionFulfillmentService.fullfillRequisitionInvocations.single().requests
    val header: FulfillRequisitionRequest.Header = requests.first().header
    assertThat(header)
      .isEqualTo(
        FulfillRequisitionRequestKt.header {
          name = REQUISITION.name
          requisitionFingerprint =
            computeRequisitionFingerprint(
              REQUISITION.measurementSpec.message.value,
              Hashing.hashSha256(REQUISITION.encryptedRequisitionSpec.ciphertext),
            )
          nonce = REQUISITION_SPEC.nonce
        }
      )

    // Injection of fake encrypter means this is should just be the serialized Sketch.
    // TODO(world-federation-of-advertisers/any-sketch-java#16): Consider verifying the decrypted
    // sketch instead.
    val encryptedSketch: ByteString = requests.drop(1).map { it.bodyChunk.data }.flatten()
    val expectedSketch =
      SketchGenerator(
          eventQuery,
          LIQUID_LEGIONS_SKETCH_PARAMS.toSketchConfig(),
          MEASUREMENT_SPEC.vidSamplingInterval,
        )
        .generate(
          REQUISITION_SPEC.events.eventGroupsList.map {
            EventQuery.EventGroupSpec(eventGroup { name = it.key }, it.value)
          }
        )
    assertThat(Sketch.parseFrom(encryptedSketch)).isEqualTo(expectedSketch)
  }

  @Test
  fun `refuses requisition when DuchyEntry verification fails`() {
    val eventQueryMock = mock<EventQuery<TestEvent>>()
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        eventQueryMock,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        compositionMechanism = COMPOSITION_MECHANISM,
      )
    val requisition =
      REQUISITION.copy {
        duchies[0] =
          duchies[0].copy {
            value =
              value.copy {
                liquidLegionsV2 =
                  liquidLegionsV2.copy {
                    elGamalPublicKey =
                      elGamalPublicKey.copy { signature = "garbage".toByteStringUtf8() }
                  }
              }
          }
      }
    requisitionsServiceMock.stub {
      onBlocking { requisitionsServiceMock.listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val refuseRequest: RefuseRequisitionRequest =
      verifyAndCapture(requisitionsServiceMock, RequisitionsCoroutineImplBase::refuseRequisition)
    assertThat(refuseRequest)
      .ignoringFieldScope(
        FieldScopes.allowingFieldDescriptors(
          Refusal.getDescriptor().findFieldByNumber(Refusal.MESSAGE_FIELD_NUMBER)
        )
      )
      .isEqualTo(
        refuseRequisitionRequest {
          name = requisition.name
          refusal = refusal { justification = Refusal.Justification.CONSENT_SIGNAL_INVALID }
        }
      )
    assertThat(refuseRequest.refusal.message).contains(DUCHY_NAME)
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
    verifyBlocking(requisitionsServiceMock, never()) { fulfillDirectRequisition(any()) }
  }

  @Test
  fun `refuses Requisition when EventGroup not found`() {
    val eventQueryMock = mock<EventQuery<TestEvent>>()
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        eventQueryMock,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        compositionMechanism = COMPOSITION_MECHANISM,
      )
    eventGroupsServiceMock.stub {
      onBlocking { getEventGroup(any()) }.thenThrow(Status.NOT_FOUND.asRuntimeException())
    }

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val refuseRequest: RefuseRequisitionRequest =
      verifyAndCapture(requisitionsServiceMock, RequisitionsCoroutineImplBase::refuseRequisition)
    assertThat(refuseRequest)
      .ignoringFieldScope(
        FieldScopes.allowingFieldDescriptors(
          Refusal.getDescriptor().findFieldByNumber(Refusal.MESSAGE_FIELD_NUMBER)
        )
      )
      .isEqualTo(
        refuseRequisitionRequest {
          name = REQUISITION.name
          refusal = refusal { justification = Refusal.Justification.SPEC_INVALID }
        }
      )
    assertThat(refuseRequest.refusal.message)
      .contains(REQUISITION_SPEC.events.eventGroupsList.first().key)
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
    verifyBlocking(requisitionsServiceMock, never()) { fulfillDirectRequisition(any()) }
  }

  @Test
  fun `refuses Requisition when noiseMechanism is GEOMETRIC and compositionMechanism is ACDP`() {
    val requisitionGeometric =
      REQUISITION.copy {
        protocolConfig =
          protocolConfig.copy {
            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol {
                liquidLegionsV2 =
                  ProtocolConfigKt.liquidLegionsV2 {
                    noiseMechanism = ProtocolConfig.NoiseMechanism.GEOMETRIC
                    sketchParams = liquidLegionsSketchParams {
                      decayRate = LLV2_DECAY_RATE
                      maxSize = LLV2_MAX_SIZE
                      samplingIndicatorSize = 10_000_000
                    }
                    ellipticCurveId = 415
                  }
              }
          }
      }

    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisitionGeometric })
    }
    val eventQueryMock = mock<EventQuery<TestEvent>>()
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        eventQueryMock,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        compositionMechanism = CompositionMechanism.ACDP,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val refuseRequest: RefuseRequisitionRequest =
      verifyAndCapture(requisitionsServiceMock, RequisitionsCoroutineImplBase::refuseRequisition)
    assertThat(refuseRequest)
      .ignoringFieldScope(
        FieldScopes.allowingFieldDescriptors(
          Refusal.getDescriptor().findFieldByNumber(Refusal.MESSAGE_FIELD_NUMBER)
        )
      )
      .isEqualTo(
        refuseRequisitionRequest {
          name = REQUISITION.name
          refusal = refusal { justification = Refusal.Justification.SPEC_INVALID }
        }
      )
    assertThat(refuseRequest.refusal.message)
      .contains(ProtocolConfig.NoiseMechanism.GEOMETRIC.toString())
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
    verifyBlocking(requisitionsServiceMock, never()) { fulfillDirectRequisition(any()) }
  }

  @Test
  fun `refuses Requisition when directNoiseMechanism option provided by Kingdom is not CONTINUOUS_GAUSSIAN and compositionMechanism is ACDP`() {
    val noiseMechanismOption = ProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE
    val requisition =
      REQUISITION.copy {
        protocolConfig =
          protocolConfig.copy {
            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol {
                direct =
                  ProtocolConfigKt.direct {
                    noiseMechanisms += noiseMechanismOption
                    customDirectMethodology =
                      ProtocolConfig.Direct.CustomDirectMethodology.getDefaultInstance()
                    deterministicCountDistinct =
                      ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
                    deterministicDistribution =
                      ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
                  }
              }
          }
      }
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }
    val eventQueryMock = mock<EventQuery<TestEvent>>()
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        eventQueryMock,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        random = Random(RANDOM_SEED),
        compositionMechanism = CompositionMechanism.ACDP,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val refuseRequest: RefuseRequisitionRequest =
      verifyAndCapture(requisitionsServiceMock, RequisitionsCoroutineImplBase::refuseRequisition)
    assertThat(refuseRequest)
      .ignoringFieldScope(
        FieldScopes.allowingFieldDescriptors(
          Refusal.getDescriptor().findFieldByNumber(Refusal.MESSAGE_FIELD_NUMBER)
        )
      )
      .isEqualTo(
        refuseRequisitionRequest {
          name = REQUISITION.name
          refusal = refusal { justification = Refusal.Justification.SPEC_INVALID }
        }
      )
    assertThat(refuseRequest.refusal.message).contains("No valid noise mechanism option")
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
    verifyBlocking(requisitionsServiceMock, never()) { fulfillDirectRequisition(any()) }
  }

  @Test
  fun `refuses Requisition with CONSENT_SIGNAL_INVALID when EventGroup ID matches refusal`() {
    val eventGroupName = EventGroupKey(EDP_ID, CONSENT_SIGNAL_INVALID_EVENT_GROUP_ID).toName()
    val requisitionSpec =
      REQUISITION_SPEC.copy {
        clearEvents()
        events =
          RequisitionSpecKt.events {
            eventGroups += eventGroupEntry {
              key = eventGroupName
              value = RequisitionSpecKt.EventGroupEntryKt.value {}
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
    val eventQueryMock = mock<EventQuery<TestEvent>>()
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        eventQueryMock,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val refuseRequest: RefuseRequisitionRequest =
      verifyAndCapture(requisitionsServiceMock, RequisitionsCoroutineImplBase::refuseRequisition)
    assertThat(refuseRequest)
      .ignoringFieldScope(
        FieldScopes.allowingFieldDescriptors(
          Refusal.getDescriptor().findFieldByNumber(Refusal.MESSAGE_FIELD_NUMBER)
        )
      )
      .isEqualTo(
        refuseRequisitionRequest {
          name = REQUISITION.name
          refusal = refusal { justification = Refusal.Justification.CONSENT_SIGNAL_INVALID }
        }
      )
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
    verifyBlocking(requisitionsServiceMock, never()) { fulfillDirectRequisition(any()) }
  }

  @Test
  fun `refuses Requisition with SPEC_INVALID when EventGroup ID matches refusal justification`() {
    val eventGroupName = EventGroupKey(EDP_ID, SPEC_INVALID_EVENT_GROUP_ID).toName()
    val requisitionSpec =
      REQUISITION_SPEC.copy {
        clearEvents()
        events =
          RequisitionSpecKt.events {
            eventGroups += eventGroupEntry {
              key = eventGroupName
              value = RequisitionSpecKt.EventGroupEntryKt.value {}
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
    val eventQueryMock = mock<EventQuery<TestEvent>>()
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        eventQueryMock,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val refuseRequest: RefuseRequisitionRequest =
      verifyAndCapture(requisitionsServiceMock, RequisitionsCoroutineImplBase::refuseRequisition)
    assertThat(refuseRequest)
      .ignoringFieldScope(
        FieldScopes.allowingFieldDescriptors(
          Refusal.getDescriptor().findFieldByNumber(Refusal.MESSAGE_FIELD_NUMBER)
        )
      )
      .isEqualTo(
        refuseRequisitionRequest {
          name = REQUISITION.name
          refusal = refusal { justification = Refusal.Justification.SPEC_INVALID }
        }
      )
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
    verifyBlocking(requisitionsServiceMock, never()) { fulfillDirectRequisition(any()) }
  }

  @Test
  fun `refuses Requisition with INSUFFICIENT_PRIVACY_BUDGET when EventGroup ID matches refusal`() {
    val eventGroupName = EventGroupKey(EDP_ID, INSUFFICIENT_PRIVACY_BUDGET_EVENT_GROUP_ID).toName()
    val requisitionSpec =
      REQUISITION_SPEC.copy {
        clearEvents()
        events =
          RequisitionSpecKt.events {
            eventGroups += eventGroupEntry {
              key = eventGroupName
              value = RequisitionSpecKt.EventGroupEntryKt.value {}
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
    val eventQueryMock = mock<EventQuery<TestEvent>>()
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        eventQueryMock,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val refuseRequest: RefuseRequisitionRequest =
      verifyAndCapture(requisitionsServiceMock, RequisitionsCoroutineImplBase::refuseRequisition)
    assertThat(refuseRequest)
      .ignoringFieldScope(
        FieldScopes.allowingFieldDescriptors(
          Refusal.getDescriptor().findFieldByNumber(Refusal.MESSAGE_FIELD_NUMBER)
        )
      )
      .isEqualTo(
        refuseRequisitionRequest {
          name = REQUISITION.name
          refusal = refusal { justification = Refusal.Justification.INSUFFICIENT_PRIVACY_BUDGET }
        }
      )
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
    verifyBlocking(requisitionsServiceMock, never()) { fulfillDirectRequisition(any()) }
  }

  @Test
  fun `refuses Requisition with UNFULFILLABLE when EventGroup ID matches refusal justification`() {
    val eventGroupName = EventGroupKey(EDP_ID, UNFULFILLABLE_EVENT_GROUP_ID).toName()
    val requisitionSpec =
      REQUISITION_SPEC.copy {
        clearEvents()
        events =
          RequisitionSpecKt.events {
            eventGroups += eventGroupEntry {
              key = eventGroupName
              value = RequisitionSpecKt.EventGroupEntryKt.value {}
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
    val eventQueryMock = mock<EventQuery<TestEvent>>()
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        eventQueryMock,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val refuseRequest: RefuseRequisitionRequest =
      verifyAndCapture(requisitionsServiceMock, RequisitionsCoroutineImplBase::refuseRequisition)
    assertThat(refuseRequest)
      .ignoringFieldScope(
        FieldScopes.allowingFieldDescriptors(
          Refusal.getDescriptor().findFieldByNumber(Refusal.MESSAGE_FIELD_NUMBER)
        )
      )
      .isEqualTo(
        refuseRequisitionRequest {
          name = REQUISITION.name
          refusal = refusal { justification = Refusal.Justification.UNFULFILLABLE }
        }
      )
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
    verifyBlocking(requisitionsServiceMock, never()) { fulfillDirectRequisition(any()) }
  }

  @Test
  fun `refuses Requisition with DECLINED when EventGroup ID matches refusal justification`() {
    val eventGroupName = EventGroupKey(EDP_ID, DECLINED_EVENT_GROUP_ID).toName()
    val requisitionSpec =
      REQUISITION_SPEC.copy {
        clearEvents()
        events =
          RequisitionSpecKt.events {
            eventGroups += eventGroupEntry {
              key = eventGroupName
              value = RequisitionSpecKt.EventGroupEntryKt.value {}
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
    val eventQueryMock = mock<EventQuery<TestEvent>>()
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        eventQueryMock,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val refuseRequest: RefuseRequisitionRequest =
      verifyAndCapture(requisitionsServiceMock, RequisitionsCoroutineImplBase::refuseRequisition)
    assertThat(refuseRequest)
      .ignoringFieldScope(
        FieldScopes.allowingFieldDescriptors(
          Refusal.getDescriptor().findFieldByNumber(Refusal.MESSAGE_FIELD_NUMBER)
        )
      )
      .isEqualTo(
        refuseRequisitionRequest {
          name = REQUISITION.name
          refusal = refusal { justification = Refusal.Justification.DECLINED }
        }
      )
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
    verifyBlocking(requisitionsServiceMock, never()) { fulfillDirectRequisition(any()) }
  }

  @Test
  fun `refuses Requisition with UNFULFILLABLE when certificate doesn't match private key`() {
    val requisition =
      REQUISITION.copy {
        dataProviderCertificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
        protocolConfig =
          protocolConfig.copy {
            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol {
                direct =
                  ProtocolConfigKt.direct {
                    noiseMechanisms += ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
                    deterministicCountDistinct =
                      ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
                    deterministicDistribution =
                      ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
                  }
              }
          }
      }
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        random = Random(RANDOM_SEED),
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val refuseRequest: RefuseRequisitionRequest =
      verifyAndCapture(requisitionsServiceMock, RequisitionsCoroutineImplBase::refuseRequisition)
    assertThat(refuseRequest)
      .ignoringFields(RefuseRequisitionRequest.REFUSAL_FIELD_NUMBER)
      .isEqualTo(refuseRequisitionRequest { name = REQUISITION.name })
    assertThat(refuseRequest.refusal)
      .ignoringFields(Refusal.MESSAGE_FIELD_NUMBER)
      .isEqualTo(refusal { justification = Refusal.Justification.UNFULFILLABLE })
    assertThat(refuseRequest.refusal.message).ignoringCase().contains("certificate")
  }

  @Test
  fun `fulfills direct reach and frequency Requisition`() {
    val noiseMechanismOption = ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
    val requisition =
      REQUISITION.copy {
        protocolConfig =
          protocolConfig.copy {
            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol {
                direct =
                  ProtocolConfigKt.direct {
                    noiseMechanisms += noiseMechanismOption
                    deterministicCountDistinct =
                      ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
                    deterministicDistribution =
                      ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
                  }
              }
          }
      }
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        random = Random(RANDOM_SEED),
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()
    assertThat(result.reach.noiseMechanism == noiseMechanismOption)
    assertThat(result.reach.hasDeterministicCountDistinct())
    assertThat(result.frequency.noiseMechanism == noiseMechanismOption)
    assertThat(result.frequency.hasDeterministicDistribution())
    assertThat(result).reachValue().isWithin(2.0).of(2000L)
    assertThat(result).frequencyDistribution().isWithin(0.01).of(mapOf(2L to 0.5, 4L to 0.5))
  }

  @Test
  fun `fulfills direct reach and frequency Requisition when true reach is 0`() {
    val noiseMechanismOption = ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
    val requisitionSpec =
      REQUISITION_SPEC.copy {
        clearEvents()
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
                    // An null set expression
                    expression =
                      "person.gender == ${Person.Gender.MALE_VALUE} && " +
                        "person.gender == ${Person.Gender.FEMALE_VALUE}"
                  }
                }
            }
          }
      }

    val encryptedRequisitionSpec =
      encryptRequisitionSpec(
        signRequisitionSpec(requisitionSpec, MC_SIGNING_KEY),
        DATA_PROVIDER_PUBLIC_KEY,
      )

    val requisition =
      REQUISITION.copy {
        this.encryptedRequisitionSpec = encryptedRequisitionSpec
        protocolConfig =
          protocolConfig.copy {
            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol {
                direct =
                  ProtocolConfigKt.direct {
                    noiseMechanisms += noiseMechanismOption
                    deterministicCountDistinct =
                      ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
                    deterministicDistribution =
                      ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
                  }
              }
          }
      }
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        random = Random(RANDOM_SEED),
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()
    assertThat(result.reach.noiseMechanism == noiseMechanismOption)
    assertThat(result.reach.hasDeterministicCountDistinct())
    assertThat(result.frequency.noiseMechanism == noiseMechanismOption)
    assertThat(result.frequency.hasDeterministicDistribution())
    assertThat(result).reachValue().isWithin(2.0).of(0L)
    // TODO(world-federation-of-advertisers/cross-media-measurement#1388): Remove this check after
    // the issue is resolved.
    assertThat(result.frequency.relativeFrequencyDistributionMap.values.all { !it.isNaN() })
      .isTrue()
    assertThat(result).frequencyDistribution().isWithin(0.01)
  }

  @Test
  fun `fulfills direct reach and frequency Requisition with sampling rate less than 1`() {
    val measurementSpec =
      MEASUREMENT_SPEC.copy { vidSamplingInterval = vidSamplingInterval.copy { width = 0.1f } }
    val noiseMechanismOption = ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
    val requisition =
      REQUISITION.copy {
        this.measurementSpec = signMeasurementSpec(measurementSpec, MC_SIGNING_KEY)
        protocolConfig =
          protocolConfig.copy {
            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol {
                direct =
                  ProtocolConfigKt.direct {
                    noiseMechanisms += noiseMechanismOption
                    deterministicCountDistinct =
                      ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
                    deterministicDistribution =
                      ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
                  }
              }
          }
      }
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        random = Random(RANDOM_SEED),
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()

    assertThat(result.reach.noiseMechanism == noiseMechanismOption)
    assertThat(result.reach.hasDeterministicCountDistinct())
    assertThat(result.frequency.noiseMechanism == noiseMechanismOption)
    assertThat(result.frequency.hasDeterministicDistribution())
    assertThat(result).reachValue().isWithin(10.0).of(1920)
    assertThat(result)
      .frequencyDistribution()
      .isWithin(0.07)
      .of(mapOf(2L to 0.49479664833057146, 4L to 0.5052080336866532))
  }

  @Test
  fun `fails to fulfill direct reach and frequency Requisition when no direct noise mechanism is picked by EDP`() {
    val noiseMechanismOption = ProtocolConfig.NoiseMechanism.NONE
    val requisition =
      REQUISITION.copy {
        protocolConfig =
          protocolConfig.copy {
            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol {
                direct =
                  ProtocolConfigKt.direct {
                    noiseMechanisms += noiseMechanismOption
                    deterministicCountDistinct =
                      ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
                    deterministicDistribution =
                      ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
                  }
              }
          }
      }
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        random = Random(RANDOM_SEED),
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val refuseRequest: RefuseRequisitionRequest =
      verifyAndCapture(requisitionsServiceMock, RequisitionsCoroutineImplBase::refuseRequisition)
    assertThat(refuseRequest)
      .ignoringFieldScope(
        FieldScopes.allowingFieldDescriptors(
          Refusal.getDescriptor().findFieldByNumber(Refusal.MESSAGE_FIELD_NUMBER)
        )
      )
      .isEqualTo(
        refuseRequisitionRequest {
          name = REQUISITION.name
          refusal = refusal { justification = Refusal.Justification.SPEC_INVALID }
        }
      )
    assertThat(refuseRequest.refusal.message).contains("No valid noise mechanism option")
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
    verifyBlocking(requisitionsServiceMock, never()) { fulfillDirectRequisition(any()) }
  }

  @Test
  fun `fails to fulfill direct reach and frequency Requisition when no direct methodology is picked by EDP`() {
    val noiseMechanismOption = ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
    val requisition =
      REQUISITION.copy {
        protocolConfig =
          protocolConfig.copy {
            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol {
                direct = ProtocolConfigKt.direct { noiseMechanisms += noiseMechanismOption }
              }
          }
      }
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        random = Random(RANDOM_SEED),
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val refuseRequest: RefuseRequisitionRequest =
      verifyAndCapture(requisitionsServiceMock, RequisitionsCoroutineImplBase::refuseRequisition)
    assertThat(refuseRequest)
      .ignoringFieldScope(
        FieldScopes.allowingFieldDescriptors(
          Refusal.getDescriptor().findFieldByNumber(Refusal.MESSAGE_FIELD_NUMBER)
        )
      )
      .isEqualTo(
        refuseRequisitionRequest {
          name = REQUISITION.name
          refusal = refusal { justification = Refusal.Justification.DECLINED }
        }
      )
    assertThat(refuseRequest.refusal.message).contains("No valid methodologies")
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
    verifyBlocking(requisitionsServiceMock, never()) { fulfillDirectRequisition(any()) }
  }

  @Test
  fun `fulfills direct reach-only Requisition`() {
    val measurementSpec = REACH_ONLY_MEASUREMENT_SPEC
    val noiseMechanismOption = ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
    val requisition =
      REQUISITION.copy {
        this.measurementSpec = signMeasurementSpec(measurementSpec, MC_SIGNING_KEY)
        protocolConfig =
          protocolConfig.copy {
            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol {
                direct =
                  ProtocolConfigKt.direct {
                    noiseMechanisms += noiseMechanismOption
                    deterministicCountDistinct =
                      ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
                  }
              }
          }
      }
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        random = Random(RANDOM_SEED),
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()

    assertThat(result.reach.noiseMechanism == noiseMechanismOption)
    assertThat(result.reach.hasDeterministicCountDistinct())
    assertThat(result).reachValue().isWithin(2.0).of(2000L)
    assertThat(result.hasFrequency()).isFalse()
  }

  @Test
  fun `fulfills direct reach-only Requisition with sampling rate less than 1`() {
    val measurementSpec =
      REACH_ONLY_MEASUREMENT_SPEC.copy {
        vidSamplingInterval = vidSamplingInterval.copy { width = 0.1f }
      }
    val noiseMechanismOption = ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
    val requisition =
      REQUISITION.copy {
        this.measurementSpec = signMeasurementSpec(measurementSpec, MC_SIGNING_KEY)
        protocolConfig =
          protocolConfig.copy {
            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol {
                direct =
                  ProtocolConfigKt.direct {
                    noiseMechanisms += noiseMechanismOption
                    deterministicCountDistinct =
                      ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
                  }
              }
          }
      }
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        random = Random(RANDOM_SEED),
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()

    assertThat(result.reach.noiseMechanism == noiseMechanismOption)
    assertThat(result.reach.hasDeterministicCountDistinct())
    assertThat(result).reachValue().isWithin(10.0).of(1920L)
    assertThat(result.hasFrequency()).isFalse()
  }

  @Test
  fun `fails to fulfill direct reach-only Requisition when no direct noise mechanism is picked by EDP`() {
    val measurementSpec =
      REACH_ONLY_MEASUREMENT_SPEC.copy {
        vidSamplingInterval = vidSamplingInterval.copy { width = 0.1f }
      }
    val noiseMechanismOption = ProtocolConfig.NoiseMechanism.NONE
    val requisition =
      REQUISITION.copy {
        this.measurementSpec = signMeasurementSpec(measurementSpec, MC_SIGNING_KEY)
        protocolConfig =
          protocolConfig.copy {
            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol {
                direct =
                  ProtocolConfigKt.direct {
                    noiseMechanisms += noiseMechanismOption
                    deterministicCountDistinct =
                      ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
                  }
              }
          }
      }
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        random = Random(RANDOM_SEED),
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val refuseRequest: RefuseRequisitionRequest =
      verifyAndCapture(requisitionsServiceMock, RequisitionsCoroutineImplBase::refuseRequisition)
    assertThat(refuseRequest)
      .ignoringFieldScope(
        FieldScopes.allowingFieldDescriptors(
          Refusal.getDescriptor().findFieldByNumber(Refusal.MESSAGE_FIELD_NUMBER)
        )
      )
      .isEqualTo(
        refuseRequisitionRequest {
          name = REQUISITION.name
          refusal = refusal { justification = Refusal.Justification.SPEC_INVALID }
        }
      )
    assertThat(refuseRequest.refusal.message).contains("No valid noise mechanism option")
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
    verifyBlocking(requisitionsServiceMock, never()) { fulfillDirectRequisition(any()) }
  }

  @Test
  fun `fails to fulfill direct reach-only Requisition when no direct methodology is picked by EDP`() {
    val measurementSpec =
      REACH_ONLY_MEASUREMENT_SPEC.copy {
        vidSamplingInterval = vidSamplingInterval.copy { width = 0.1f }
      }
    val noiseMechanismOption = ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
    val requisition =
      REQUISITION.copy {
        this.measurementSpec = signMeasurementSpec(measurementSpec, MC_SIGNING_KEY)
        protocolConfig =
          protocolConfig.copy {
            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol {
                direct = ProtocolConfigKt.direct { noiseMechanisms += noiseMechanismOption }
              }
          }
      }
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }
    val simulator =
      EdpSimulator(
        EDP_DATA,
        MC_NAME,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        random = Random(RANDOM_SEED),
        compositionMechanism = COMPOSITION_MECHANISM,
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val refuseRequest: RefuseRequisitionRequest =
      verifyAndCapture(requisitionsServiceMock, RequisitionsCoroutineImplBase::refuseRequisition)
    assertThat(refuseRequest)
      .ignoringFieldScope(
        FieldScopes.allowingFieldDescriptors(
          Refusal.getDescriptor().findFieldByNumber(Refusal.MESSAGE_FIELD_NUMBER)
        )
      )
      .isEqualTo(
        refuseRequisitionRequest {
          name = REQUISITION.name
          refusal = refusal { justification = Refusal.Justification.DECLINED }
        }
      )
    assertThat(refuseRequest.refusal.message).contains("No valid methodologies")
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
    verifyBlocking(requisitionsServiceMock, never()) { fulfillDirectRequisition(any()) }
  }

  private class FakeRequisitionFulfillmentService : RequisitionFulfillmentCoroutineImplBase() {
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
    private const val EVENT_GROUP_METADATA_DESCRIPTOR_NAME =
      "dataProviders/foo/eventGroupMetadataDescriptors/bar"

    private val MC_SIGNING_KEY = loadSigningKey("${MC_ID}_cs_cert.der", "${MC_ID}_cs_private.der")
    private val DUCHY_SIGNING_KEY =
      loadSigningKey("${DUCHY_ID}_cs_cert.der", "${DUCHY_ID}_cs_private.der")

    private val DUCHY_NAME = DuchyKey(DUCHY_ID).toName()
    private val DUCHY_CERTIFICATE = certificate {
      name = DuchyCertificateKey(DUCHY_ID, externalIdToApiId(6L)).toName()
      x509Der = DUCHY_SIGNING_KEY.certificate.encoded.toByteString()
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

    private val DATA_PROVIDER_CERTIFICATE = certificate {
      name = DATA_PROVIDER_CERTIFICATE_KEY.toName()
      x509Der = EDP_SIGNING_KEY.certificate.encoded.toByteString()
      subjectKeyIdentifier = EDP_SIGNING_KEY.certificate.subjectKeyIdentifier!!
    }
    private val DATA_PROVIDER_RESULT_CERTIFICATE = certificate {
      name = DATA_PROVIDER_RESULT_CERTIFICATE_KEY.toName()
      x509Der = EDP_RESULT_SIGNING_KEY.certificate.encoded.toByteString()
      subjectKeyIdentifier = EDP_RESULT_SIGNING_KEY.certificate.subjectKeyIdentifier!!
    }
    private val EDP_DATA =
      EdpData(
        EDP_NAME,
        EDP_DISPLAY_NAME,
        loadEncryptionPrivateKey("${EDP_DISPLAY_NAME}_enc_private.tink"),
        EDP_RESULT_SIGNING_KEY,
        DATA_PROVIDER_RESULT_CERTIFICATE_KEY,
      )

    private val MC_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val MC_PRIVATE_KEY =
      loadPrivateKey(SECRET_FILES_PATH.resolve("mc_enc_private.tink").toFile())
    private val DATA_PROVIDER_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("${EDP_DISPLAY_NAME}_enc_public.tink").toFile())
        .toEncryptionPublicKey()

    private const val EVENT_GROUP_NAME = "$EDP_NAME/eventGroups/name"
    private val REQUISITION_SPEC = requisitionSpec {
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
    private val REACH_ONLY_MEASUREMENT_SPEC =
      MEASUREMENT_SPEC.copy {
        clearReachAndFrequency()
        reach = reach { privacyParams = OUTPUT_DP_PARAMS }
      }

    private val LIQUID_LEGIONS_SKETCH_PARAMS = liquidLegionsSketchParams {
      decayRate = LLV2_DECAY_RATE
      maxSize = LLV2_MAX_SIZE
      samplingIndicatorSize = 10_000_000
    }

    private val REQUISITION = requisition {
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
        key = DUCHY_NAME
        value = value {
          duchyCertificate = DUCHY_CERTIFICATE.name
          liquidLegionsV2 = liquidLegionsV2 {
            elGamalPublicKey =
              signElgamalPublicKey(CONSENT_SIGNALING_ELGAMAL_PUBLIC_KEY, DUCHY_SIGNING_KEY)
          }
        }
      }
    }

    private val TRUSTED_CERTIFICATES: Map<ByteString, X509Certificate> =
      readCertificateCollection(SECRET_FILES_PATH.resolve("edp_trusted_certs.pem").toFile())
        .associateBy { requireNotNull(it.authorityKeyIdentifier) }

    private val TEST_METADATA = EventGroupMetadata.testMetadata(1)

    private val SYNTHETIC_DATA_SPEC =
      SyntheticGenerationSpecs.SYNTHETIC_DATA_SPECS.first().copy {
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
          SyntheticGenerationSpecs.POPULATION_SPEC,
          TestEvent.getDescriptor(),
        ) {
        override fun getSyntheticDataSpec(eventGroup: EventGroup): SyntheticEventGroupSpec {
          return SYNTHETIC_DATA_SPEC
        }
      }

    /** Dummy [Throttler] for satisfying signatures without being used. */
    private val dummyThrottler =
      object : Throttler {
        override suspend fun <T> onReady(block: suspend () -> T): T {
          throw UnsupportedOperationException("Should not be called")
        }
      }

    /** [SketchEncrypter] that does not encrypt, just returning the plaintext. */
    private val fakeSketchEncrypter =
      object : SketchEncrypter {
        override fun encrypt(
          sketch: Sketch,
          ellipticCurveId: Int,
          encryptionKey: ElGamalPublicKey,
          maximumValue: Int,
        ): ByteString {
          return sketch.toByteString()
        }

        override fun encrypt(
          sketch: Sketch,
          ellipticCurveId: Int,
          encryptionKey: ElGamalPublicKey,
        ): ByteString {
          return sketch.toByteString()
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
