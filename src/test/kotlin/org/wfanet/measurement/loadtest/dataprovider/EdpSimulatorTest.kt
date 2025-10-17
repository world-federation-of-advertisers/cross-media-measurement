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
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.type.interval
import io.grpc.Status
import java.time.Clock
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.random.Random
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.stub
import org.mockito.kotlin.verifyBlocking
import org.wfanet.anysketch.Sketch
import org.wfanet.anysketch.crypto.ElGamalPublicKey
import org.wfanet.frequencycount.FrequencyVector
import org.wfanet.measurement.api.v2alpha.CreateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.EventGroupMetadata
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.FulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MediaType
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.RefuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.honestMajorityShareShuffle
import org.wfanet.measurement.api.v2alpha.RequisitionKt.DuchyEntryKt.value
import org.wfanet.measurement.api.v2alpha.RequisitionKt.duchyEntry
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createEventGroupRequest
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.liquidLegionsSketchParams
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.replaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.testing.MeasurementResultSubject.Companion.assertThat
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.api.v2alpha.updateEventGroupRequest
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.testing.verifyAndCapture
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.duchy.computeRequisitionFingerprint
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.dataprovider.MeasurementResults
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AcdpCharge
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AgeGroup as PrivacyLandscapeAge
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Gender as PrivacyLandscapeGender
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.trustee.FulfillRequisitionRequestBuilder as TrusTeeFulfillRequisitionRequestBuilder
import org.wfanet.measurement.integration.common.SyntheticGenerationSpecs
import org.wfanet.measurement.internal.kingdom.ReplaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.loadtest.common.sampleVids
import org.wfanet.measurement.loadtest.config.PrivacyBudgets
import org.wfanet.measurement.loadtest.config.TestIdentifiers

private const val RANDOM_SEED: Long = 0

private const val RING_MODULUS = 127

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

class EdpSimulatorTest : AbstractEdpSimulatorTest() {
  private data class EventGroupOptions(
    override val referenceIdSuffix: String,
    override val syntheticDataSpec: SyntheticEventGroupSpec,
    override val mediaTypes: Set<MediaType>,
    override val metadata: EventGroupMetadata?,
  ) : EdpSimulator.EventGroupOptions

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
  fun `ensureEventGroups creates EventGroup`() {
    val referenceIdSuffix = "-001"
    val edpSimulator =
      EdpSimulator(
        EDP_DATA,
        EDP_DISPLAY_NAME,
        MEASUREMENT_CONSUMER_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(
          EventGroupOptions(
            referenceIdSuffix,
            SYNTHETIC_DATA_SPEC,
            MEDIA_TYPES,
            EVENT_GROUP_METADATA,
          )
        ),
        InMemoryEventQuery(emptyList()),
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
      )

    runBlocking { edpSimulator.ensureEventGroups() }

    val request: CreateEventGroupRequest =
      verifyAndCapture(eventGroupsServiceMock, EventGroupsCoroutineImplBase::createEventGroup)
    assertThat(request)
      .isEqualTo(
        createEventGroupRequest {
          parent = EDP_NAME
          eventGroup = eventGroup {
            measurementConsumer = MEASUREMENT_CONSUMER_NAME
            eventGroupReferenceId =
              "${TestIdentifiers.SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX}-${EDP_DISPLAY_NAME}${referenceIdSuffix}"
            mediaTypes += MEDIA_TYPES
            eventGroupMetadata = EVENT_GROUP_METADATA
            dataAvailabilityInterval = DATA_AVAILABILITY_INTERVAL
          }
        }
      )
  }

  @Test
  fun `ensureEventGroup updates EventGroup`() {
    val referenceIdSuffix = "-001"
    val eventGroup = eventGroup {
      name = EVENT_GROUP_NAME
      measurementConsumer = MEASUREMENT_CONSUMER_NAME
      eventGroupReferenceId =
        "${TestIdentifiers.SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX}-${EDP_DISPLAY_NAME}${referenceIdSuffix}"
    }
    eventGroupsServiceMock.stub {
      onBlocking { listEventGroups(any()) }
        .thenReturn(listEventGroupsResponse { eventGroups += eventGroup })
    }
    val edpSimulator =
      EdpSimulator(
        EDP_DATA,
        EDP_DISPLAY_NAME,
        MEASUREMENT_CONSUMER_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(
          EventGroupOptions(
            referenceIdSuffix,
            SYNTHETIC_DATA_SPEC,
            MEDIA_TYPES,
            EVENT_GROUP_METADATA,
          )
        ),
        InMemoryEventQuery(emptyList()),
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
      )

    runBlocking { edpSimulator.ensureEventGroups() }

    // Verify EventGroup metadata has correct type.
    val updateRequest: UpdateEventGroupRequest =
      verifyAndCapture(eventGroupsServiceMock, EventGroupsCoroutineImplBase::updateEventGroup)
    assertThat(updateRequest)
      .isEqualTo(
        updateEventGroupRequest {
          this.eventGroup =
            eventGroup.copy {
              mediaTypes += MEDIA_TYPES
              eventGroupMetadata = EVENT_GROUP_METADATA
              dataAvailabilityInterval = DATA_AVAILABILITY_INTERVAL
            }
        }
      )
  }

  @Test
  fun `run updates DataProvider`() {
    val throttlerMock = mock<Throttler> {} // Prevent `run` from suspending forever.
    val simulator =
      EdpSimulator(
        EDP_DATA,
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        syntheticGeneratorEventQuery,
        throttlerMock,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
        random = Random(RANDOM_SEED),
      )

    runBlocking { simulator.run() }

    verifyProtoArgument(
        dataProvidersServiceMock,
        DataProvidersGrpcKt.DataProvidersCoroutineImplBase::replaceDataAvailabilityIntervals,
      )
      .ignoringRepeatedFieldOrderOfFields(
        ReplaceDataAvailabilityIntervalsRequest.DATA_AVAILABILITY_INTERVALS_FIELD_NUMBER
      )
      .isEqualTo(
        replaceDataAvailabilityIntervalsRequest {
          name = EDP_DATA.name
          dataAvailabilityIntervals +=
            DataProviderKt.dataAvailabilityMapEntry {
              key = MODEL_LINE_NAME
              value = DATA_AVAILABILITY_INTERVAL
            }
          dataAvailabilityIntervals +=
            DataProviderKt.dataAvailabilityMapEntry {
              key = MODEL_LINE_2_NAME
              value = DATA_AVAILABILITY_INTERVAL
            }
        }
      )
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
        EDP_DISPLAY_NAME,
        "measurementConsumers/differentMcId",
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        InMemoryEventQuery(allEvents),
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(1000)),
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
      )

    runBlocking {
      edpSimulator.ensureEventGroups()
      edpSimulator.executeRequisitionFulfillingWorkflow()
    }

    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
    verifyBlocking(requisitionsServiceMock, never()) { fulfillDirectRequisition(any()) }
  }

  @Test
  fun `refuses HMSS requisition due to invalid number of duchy entries`() {
    val requisition =
      HMSS_REQUISITION.copy {
        duchies.clear()
        duchies += DUCHY_ENTRY_ONE
      }
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }

    val edpSimulator =
      EdpSimulator(
        EDP_DATA,
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        InMemoryEventQuery(emptyList()),
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
      )
    runBlocking { edpSimulator.executeRequisitionFulfillingWorkflow() }

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
    assertThat(refuseRequest.refusal.message).contains("Two duchy entries are expected")
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
  }

  @Test
  fun `refuses HMSS requisition due to invalid number of encryption public key`() {
    val requisition =
      HMSS_REQUISITION.copy {
        duchies.clear()
        duchies +=
          DUCHY_ENTRY_ONE.copy {
            key = DUCHY_ONE_NAME
            value = value { duchyCertificate = DUCHY_ONE_CERTIFICATE.name }
          }
        duchies += DUCHY_ENTRY_TWO
      }
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }

    val edpSimulator =
      EdpSimulator(
        EDP_DATA,
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        InMemoryEventQuery(emptyList()),
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
      )
    runBlocking { edpSimulator.executeRequisitionFulfillingWorkflow() }

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
    assertThat(refuseRequest.refusal.message).contains("encryption public key")
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
  }

  @Test
  fun `refuses HMSS requisition when event filter is invalid`() {
    val requisitionSpec =
      REQUISITION_SPEC.copy {
        events =
          events.copy {
            eventGroups[0] =
              eventGroups[0].copy {
                value =
                  value.copy {
                    filter =
                      filter.copy {
                        // Expression that isn't valid for TestEvent.
                        expression = "video.viewable_100_percent == true"
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
      HMSS_REQUISITION.copy { this.encryptedRequisitionSpec = encryptedRequisitionSpec }
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }

    val edpSimulator =
      EdpSimulator(
        EDP_DATA,
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        syntheticGeneratorEventQuery,
        dummyThrottler,
        PrivacyBudgets.createNoOpPrivacyBudgetManager(),
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
      )

    runBlocking { edpSimulator.executeRequisitionFulfillingWorkflow() }

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
    assertThat(refuseRequest.refusal.message).contains("filter")
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
  }

  @Test
  fun `fulfills HMSS reach and frequency Requisition`() {
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += HMSS_REQUISITION })
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        InMemoryEventQuery(allEvents),
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
      )

    runBlocking { edpSimulator.executeRequisitionFulfillingWorkflow() }

    val requests: List<FulfillRequisitionRequest> =
      fakeRequisitionFulfillmentService.fullfillRequisitionInvocations.single().requests
    val header: FulfillRequisitionRequest.Header = requests.first().header
    val shareVector =
      FrequencyVector.parseFrom(requests.drop(1).map { it.bodyChunk.data }.flatten())
    assert(shareVector.dataList.all { it in 0 until RING_MODULUS })
    assertThat(header)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        FulfillRequisitionRequestKt.header {
          name = HMSS_REQUISITION.name
          requisitionFingerprint =
            computeRequisitionFingerprint(
              HMSS_REQUISITION.measurementSpec.message.value,
              Hashing.hashSha256(HMSS_REQUISITION.encryptedRequisitionSpec.ciphertext),
            )
          nonce = REQUISITION_SPEC.nonce
          honestMajorityShareShuffle =
            FulfillRequisitionRequestKt.HeaderKt.honestMajorityShareShuffle {
              registerCount = shareVector.dataList.size.toLong()
              dataProviderCertificate = EDP_DATA.certificateKey.toName()
            }
        }
      )
    // TODO(@ple13): Verify the reach and the frequency distribution.
    assert(header.honestMajorityShareShuffle.hasSecretSeed())
  }

  @Test
  fun `refuses HMSS requisition due to empty vidIndexMap`() {
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += HMSS_REQUISITION })
    }

    val edpSimulator =
      EdpSimulator(
        EDP_DATA,
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        InMemoryEventQuery(emptyList()),
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
      )
    runBlocking { edpSimulator.executeRequisitionFulfillingWorkflow() }

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
    assertThat(refuseRequest.refusal.message).contains("Protocol not set or not supported.")
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
  }

  @Test
  fun `refuses TrusTee requisition due to invalid number of duchy entries`() {
    val requisition = TRUSTEE_REQUISITION.copy { duchies += DUCHY_ENTRY_TWO }
    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += requisition })
    }

    val edpSimulator =
      EdpSimulator(
        EDP_DATA,
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        syntheticGeneratorEventQuery,
        AbstractEdpSimulatorTest.dummyThrottler,
        PrivacyBudgets.createNoOpPrivacyBudgetManager(),
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
      )
    runBlocking { edpSimulator.executeRequisitionFulfillingWorkflow() }

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
    assertThat(refuseRequest.refusal.message).contains("One duchy entry is expected")
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
  }

  @Test
  fun `fulfills TrusTee requisition`() {
    val trusTeeEncryptionParams =
      TrusTeeFulfillRequisitionRequestBuilder.EncryptionParams(
        kmsClient = KMS_CLIENT,
        kmsKekUri = KEK_URI,
        workloadIdentityProvider = "workload-identity-provider",
        impersonatedServiceAccount = "impersonated-service-account",
      )

    requisitionsServiceMock.stub {
      onBlocking { listRequisitions(any()) }
        .thenReturn(listRequisitionsResponse { requisitions += TRUSTEE_REQUISITION })
    }

    val edpSimulator =
      EdpSimulator(
        EDP_DATA,
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
        trusTeeEncryptionParams = trusTeeEncryptionParams,
      )

    runBlocking { edpSimulator.executeRequisitionFulfillingWorkflow() }

    val requests: List<FulfillRequisitionRequest> =
      fakeRequisitionFulfillmentService.fullfillRequisitionInvocations.single().requests
    val header: FulfillRequisitionRequest.Header = requests.first().header

    assertThat(header)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        FulfillRequisitionRequestKt.header {
          name = TRUSTEE_REQUISITION.name
          requisitionFingerprint =
            computeRequisitionFingerprint(
              TRUSTEE_REQUISITION.measurementSpec.message.value,
              Hashing.hashSha256(TRUSTEE_REQUISITION.encryptedRequisitionSpec.ciphertext),
            )
          nonce = REQUISITION_SPEC.nonce
          trusTee =
            FulfillRequisitionRequestKt.HeaderKt.trusTee {
              dataFormat =
                FulfillRequisitionRequest.Header.TrusTee.DataFormat.ENCRYPTED_FREQUENCY_VECTOR
            }
        }
      )
  }

  @Test
  fun `charges privacy budget with discrete Gaussian noise and ACDP composition for mpc HMSS reach and frequency Requisition`() {
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
        HMSS_REQUISITION.copy {
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
          EDP_DISPLAY_NAME,
          MC_NAME,
          certificatesStub,
          modelLinesStub,
          dataProvidersStub,
          eventGroupsStub,
          requisitionsStub,
          requisitionFulfillmentStubMap,
          SYNTHETIC_DATA_TIME_ZONE,
          listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
          InMemoryEventQuery(allEvents),
          dummyThrottler,
          privacyBudgetManager,
          TRUSTED_CERTIFICATES,
          VID_INDEX_MAP,
        )
      runBlocking {
        edpSimulator.ensureEventGroups()
        edpSimulator.executeRequisitionFulfillingWorkflow()
      }

      val acdpBalancesMap: Map<PrivacyBucketGroup, AcdpCharge> = backingStore.getAcdpBalancesMap()

      // reach and frequency delta, epsilon, contributorCount: epsilon = 2.0, delta = 2E-12,
      // contributorCount = 1
      for (acdpCharge in acdpBalancesMap.values) {
        assertThat(acdpCharge.rho).isEqualTo(0.017514783972154814)
        assertThat(acdpCharge.theta).isEqualTo(1.152890060534907E-13)
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
  fun `charges privacy budget with discrete Gaussian noise and ACDP composition for mpc LLV2 reach and frequency Requisition`() {
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
          EDP_DISPLAY_NAME,
          MC_NAME,
          certificatesStub,
          modelLinesStub,
          dataProvidersStub,
          eventGroupsStub,
          requisitionsStub,
          requisitionFulfillmentStubMap,
          SYNTHETIC_DATA_TIME_ZONE,
          listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
          InMemoryEventQuery(allEvents),
          dummyThrottler,
          privacyBudgetManager,
          TRUSTED_CERTIFICATES,
          VID_INDEX_MAP,
        )
      runBlocking {
        edpSimulator.ensureEventGroups()
        edpSimulator.executeRequisitionFulfillingWorkflow()
      }

      val acdpBalancesMap: Map<PrivacyBucketGroup, AcdpCharge> = backingStore.getAcdpBalancesMap()

      // reach and frequency delta, epsilon, contributorCount: epsilon = 2.0, delta = 2E-12,
      // contributorCount = 1
      for (acdpCharge in acdpBalancesMap.values) {
        assertThat(acdpCharge.rho).isEqualTo(0.017514783972154814)
        assertThat(acdpCharge.theta).isEqualTo(1.152890060534907E-13)
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
  fun `charges privacy budget with continuous Gaussian noise and ACDP composition for direct reach and frequency Requisition`() {
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
          EDP_DISPLAY_NAME,
          MC_NAME,
          certificatesStub,
          modelLinesStub,
          dataProvidersStub,
          eventGroupsStub,
          requisitionsStub,
          requisitionFulfillmentStubMap,
          SYNTHETIC_DATA_TIME_ZONE,
          listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
          InMemoryEventQuery(allEvents),
          dummyThrottler,
          privacyBudgetManager,
          TRUSTED_CERTIFICATES,
          VID_INDEX_MAP,
        )
      runBlocking {
        edpSimulator.ensureEventGroups()
        edpSimulator.executeRequisitionFulfillingWorkflow()
      }

      val acdpBalancesMap: Map<PrivacyBucketGroup, AcdpCharge> = backingStore.getAcdpBalancesMap()

      // reach and frequency delta, epsilon: epsilon = 2.0, delta = 2E-12,
      for (acdpCharge in acdpBalancesMap.values) {
        assertThat(acdpCharge.rho).isEqualTo(0.023253185826311272)
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        eventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
        sketchEncrypter = fakeSketchEncrypter,
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        eventQueryMock,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
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
    assertThat(refuseRequest.refusal.message).contains(DUCHY_ONE_NAME)
    assertThat(fakeRequisitionFulfillmentService.fullfillRequisitionInvocations).isEmpty()
    verifyBlocking(requisitionsServiceMock, never()) { fulfillDirectRequisition(any()) }
  }

  @Test
  fun `refuses Requisition when EventGroup not found`() {
    val eventQueryMock = mock<EventQuery<TestEvent>>()
    val simulator =
      EdpSimulator(
        EDP_DATA,
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        eventQueryMock,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
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
  fun `refuses Requisition when noiseMechanism is GEOMETRIC and PBM uses ACDP composition`() {
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        eventQueryMock,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
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
  fun `refuses Requisition when directNoiseMechanism option provided by Kingdom is not CONTINUOUS_GAUSSIAN and PBM uses ACDP composition`() {
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        eventQueryMock,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
        random = Random(RANDOM_SEED),
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        eventQueryMock,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        eventQueryMock,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        eventQueryMock,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        eventQueryMock,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        eventQueryMock,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
        random = Random(RANDOM_SEED),
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
        random = Random(RANDOM_SEED),
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

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
    assertThat(result.reach.noiseMechanism == noiseMechanismOption)
    assertThat(result.reach.hasDeterministicCountDistinct())
    assertThat(result.frequency.noiseMechanism == noiseMechanismOption)
    assertThat(result.frequency.hasDeterministicDistribution())
    assertThat(result)
      .reachValue()
      .isWithin(REACH_TOLERANCE / MEASUREMENT_SPEC.vidSamplingInterval.width)
      .of(expectedReach)
    assertThat(result)
      .frequencyDistribution()
      .isWithin(FREQUENCY_DISTRIBUTION_TOLERANCE)
      .of(expectedFrequencyDistribution)
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
        random = Random(RANDOM_SEED),
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
    assertThat(result).reachValue().isWithin(REACH_TOLERANCE).of(0L)
    // TODO(world-federation-of-advertisers/cross-media-measurement#1388): Remove this check after
    //  the issue is resolved.
    assertThat(result.frequency.relativeFrequencyDistributionMap.values.all { !it.isNaN() })
      .isTrue()
    assertThat(result).frequencyDistribution().isWithin(FREQUENCY_DISTRIBUTION_TOLERANCE)
  }

  @Test
  fun `fulfills direct reach and frequency Requisition with sampling rate less than 1`() {
    val vidSamplingIntervalWidth = 0.1
    val measurementSpec =
      MEASUREMENT_SPEC.copy {
        vidSamplingInterval =
          vidSamplingInterval.copy { width = vidSamplingIntervalWidth.toFloat() }
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
        random = Random(RANDOM_SEED),
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()
    val expectedReach: Long =
      computeExpectedReach(REQUISITION_SPEC.events.eventGroupsList, measurementSpec)
    val expectedFrequencyDistribution: Map<Long, Double> =
      computeExpectedFrequencyDistribution(REQUISITION_SPEC.events.eventGroupsList, measurementSpec)

    assertThat(result.reach.noiseMechanism == noiseMechanismOption)
    assertThat(result.reach.hasDeterministicCountDistinct())
    assertThat(result.frequency.noiseMechanism == noiseMechanismOption)
    assertThat(result.frequency.hasDeterministicDistribution())
    assertThat(result)
      .reachValue()
      .isWithin(REACH_TOLERANCE / vidSamplingIntervalWidth)
      .of(expectedReach)
    assertThat(result)
      .frequencyDistribution()
      .isWithin(FREQUENCY_DISTRIBUTION_TOLERANCE / vidSamplingIntervalWidth)
      .of(expectedFrequencyDistribution)
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
        random = Random(RANDOM_SEED),
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
        random = Random(RANDOM_SEED),
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
        random = Random(RANDOM_SEED),
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()
    val expectedReach: Long =
      computeExpectedReach(REQUISITION_SPEC.events.eventGroupsList, measurementSpec)
    assertThat(result.reach.noiseMechanism == noiseMechanismOption)
    assertThat(result.reach.hasDeterministicCountDistinct())
    assertThat(result).reachValue().isWithin(REACH_TOLERANCE).of(expectedReach)
    assertThat(result.hasFrequency()).isFalse()
  }

  @Test
  fun `fulfills direct reach-only Requisition with sampling rate less than 1`() {
    val vidSamplingIntervalWidth = 0.1
    val measurementSpec =
      REACH_ONLY_MEASUREMENT_SPEC.copy {
        vidSamplingInterval =
          vidSamplingInterval.copy { width = vidSamplingIntervalWidth.toFloat() }
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
        random = Random(RANDOM_SEED),
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()
    val expectedReach: Long =
      computeExpectedReach(REQUISITION_SPEC.events.eventGroupsList, measurementSpec)
    assertThat(result.reach.noiseMechanism == noiseMechanismOption)
    assertThat(result.reach.hasDeterministicCountDistinct())
    assertThat(result)
      .reachValue()
      .isWithin(REACH_TOLERANCE / vidSamplingIntervalWidth)
      .of(expectedReach)
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
        random = Random(RANDOM_SEED),
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
        random = Random(RANDOM_SEED),
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
  fun `fulfills impression Requisition`() {
    val noiseMechanismOption = ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
    val requisition =
      REQUISITION.copy {
        measurementSpec = signMeasurementSpec(IMPRESSION_MEASUREMENT_SPEC, MC_SIGNING_KEY)
        protocolConfig =
          protocolConfig.copy {
            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol {
                direct =
                  ProtocolConfigKt.direct {
                    noiseMechanisms += noiseMechanismOption
                    deterministicCount =
                      ProtocolConfig.Direct.DeterministicCount.getDefaultInstance()
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
        random = Random(RANDOM_SEED),
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()
    val expectedImpression =
      computeExpectedImpression(
        REQUISITION_SPEC.events.eventGroupsList,
        IMPRESSION_MEASUREMENT_SPEC,
      )

    assertThat(result.impression.noiseMechanism == noiseMechanismOption)
    assertThat(result.impression.hasDeterministicCount())
    assertThat(result).impressionValue().isWithin(IMPRESSION_TOLERANCE).of(expectedImpression)
  }

  @Test
  fun `fulfills impression requisition with sampling rate less than 1`() {
    val noiseMechanismOption = ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
    val vidSamplingWidth = 0.1
    val measurementSpec =
      IMPRESSION_MEASUREMENT_SPEC.copy {
        vidSamplingInterval = vidSamplingInterval.copy { width = vidSamplingWidth.toFloat() }
      }
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
                    deterministicCount =
                      ProtocolConfig.Direct.DeterministicCount.getDefaultInstance()
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
        random = Random(RANDOM_SEED),
      )

    runBlocking { simulator.executeRequisitionFulfillingWorkflow() }

    val request: FulfillDirectRequisitionRequest =
      verifyAndCapture(
        requisitionsServiceMock,
        RequisitionsCoroutineImplBase::fulfillDirectRequisition,
      )
    val result: Measurement.Result = decryptResult(request.encryptedResult, MC_PRIVATE_KEY).unpack()
    val expectedImpression =
      computeExpectedImpression(REQUISITION_SPEC.events.eventGroupsList, measurementSpec)

    assertThat(result.impression.noiseMechanism == noiseMechanismOption)
    assertThat(result.impression.hasDeterministicCount())
    assertThat(result)
      .impressionValue()
      .isWithin(IMPRESSION_TOLERANCE / vidSamplingWidth)
      .of(expectedImpression)
  }

  @Test
  fun `fails to fulfill impression Requisition when no direct noise mechanism options are provided by Kingdom`() {
    val noiseMechanismOption = ProtocolConfig.NoiseMechanism.NONE
    val requisition =
      REQUISITION.copy {
        measurementSpec = signMeasurementSpec(IMPRESSION_MEASUREMENT_SPEC, MC_SIGNING_KEY)
        protocolConfig =
          protocolConfig.copy {
            protocols.clear()
            protocols +=
              ProtocolConfigKt.protocol {
                direct =
                  ProtocolConfigKt.direct {
                    noiseMechanisms += noiseMechanismOption
                    deterministicCount =
                      ProtocolConfig.Direct.DeterministicCount.getDefaultInstance()
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
        random = Random(RANDOM_SEED),
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
  fun `fails to fulfill impression Requisition when no direct methodologies are provided by Kingdom`() {
    val noiseMechanismOption = ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
    val requisition =
      REQUISITION.copy {
        measurementSpec = signMeasurementSpec(IMPRESSION_MEASUREMENT_SPEC, MC_SIGNING_KEY)
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
        EDP_DISPLAY_NAME,
        MC_NAME,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        SYNTHETIC_DATA_TIME_ZONE,
        listOf(EventGroupOptions("", SYNTHETIC_DATA_SPEC, MEDIA_TYPES, EVENT_GROUP_METADATA)),
        syntheticGeneratorEventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        VID_INDEX_MAP,
        random = Random(RANDOM_SEED),
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

  companion object {
    private val KMS_CLIENT = FakeKmsClient()
    private const val KEK_URI = FakeKmsClient.KEY_URI_PREFIX + "kek"

    init {
      AeadConfig.register()
      StreamingAeadConfig.register()
      val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES256_GCM"))
      KMS_CLIENT.setAead(KEK_URI, kmsKeyHandle.getPrimitive(Aead::class.java))
    }

    private val MEDIA_TYPES = setOf(MediaType.VIDEO, MediaType.DISPLAY)
    private val DATA_AVAILABILITY_INTERVAL = interval {
      startTime = FIRST_EVENT_DATE.atStartOfDay(SYNTHETIC_DATA_TIME_ZONE).toInstant().toProtoTime()
      endTime =
        LAST_EVENT_DATE.plusDays(1).atStartOfDay(SYNTHETIC_DATA_TIME_ZONE).toInstant().toProtoTime()
    }
    private val EVENT_GROUP_METADATA = eventGroupMetadata {
      adMetadata =
        EventGroupMetadataKt.adMetadata {
          campaignMetadata =
            EventGroupMetadataKt.AdMetadataKt.campaignMetadata {
              brandName = "Log by Blammo!"
              campaignName = "Better Than Bad"
            }
        }
    }

    private val DUCHY1_ENCRYPTION_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()

    private val REACH_ONLY_MEASUREMENT_SPEC =
      MEASUREMENT_SPEC.copy {
        clearReachAndFrequency()
        reach = reach { privacyParams = OUTPUT_DP_PARAMS }
      }
    private val IMPRESSION_MEASUREMENT_SPEC = measurementSpec {
      measurementPublicKey = MC_PUBLIC_KEY.pack()
      impression = impression {
        privacyParams = differentialPrivacyParams {
          epsilon = 10.0
          delta = 1E-12
        }
        maximumFrequencyPerUser = 10
      }
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }
      nonceHashes += Hashing.hashSha256(REQUISITION_SPEC.nonce)
    }

    private val DUCHY_ENTRY_ONE = duchyEntry {
      key = DUCHY_ONE_NAME
      value = value {
        duchyCertificate = DUCHY_ONE_CERTIFICATE.name
        honestMajorityShareShuffle = honestMajorityShareShuffle {
          publicKey = signEncryptionPublicKey(DUCHY1_ENCRYPTION_PUBLIC_KEY, DUCHY_ONE_SIGNING_KEY)
        }
      }
    }

    private val DUCHY_ENTRY_TWO = duchyEntry {
      key = DUCHY_TWO_NAME
      value = value { duchyCertificate = DUCHY_TWO_CERTIFICATE.name }
    }

    /** TODO(@kungfucraig): Replace this with the object in HmmsRequisitions.kt */
    private val HMSS_REQUISITION = requisition {
      name = "${EDP_NAME}/requisitions/foo"
      measurement = MEASUREMENT_NAME
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      measurementSpec = signMeasurementSpec(MEASUREMENT_SPEC, MC_SIGNING_KEY)
      encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
      protocolConfig = protocolConfig {
        protocols +=
          ProtocolConfigKt.protocol {
            honestMajorityShareShuffle =
              ProtocolConfigKt.honestMajorityShareShuffle {
                ringModulus = RING_MODULUS
                noiseMechanism = NOISE_MECHANISM
              }
          }
      }
      dataProviderCertificate = DATA_PROVIDER_CERTIFICATE.name
      dataProviderPublicKey = DATA_PROVIDER_PUBLIC_KEY.pack()
      duchies += DUCHY_ENTRY_ONE
      duchies += DUCHY_ENTRY_TWO
    }

    private val TRUSTEE_REQUISITION = requisition {
      name = "${EDP_NAME}/requisitions/foo"
      measurement = MEASUREMENT_NAME
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = MEASUREMENT_CONSUMER_CERTIFICATE_NAME
      measurementSpec = signMeasurementSpec(MEASUREMENT_SPEC, MC_SIGNING_KEY)
      encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
      protocolConfig = protocolConfig {
        protocols += ProtocolConfigKt.protocol { trusTee = ProtocolConfigKt.trusTee {} }
      }
      dataProviderCertificate = DATA_PROVIDER_CERTIFICATE.name
      dataProviderPublicKey = DATA_PROVIDER_PUBLIC_KEY.pack()
      duchies += DUCHY_ENTRY_ONE
    }

    private val syntheticGeneratorEventQuery =
      object :
        SyntheticGeneratorEventQuery(
          SyntheticGenerationSpecs.SYNTHETIC_POPULATION_SPEC_SMALL,
          TestEvent.getDescriptor(),
          SYNTHETIC_DATA_TIME_ZONE,
        ) {
        override fun getSyntheticDataSpec(eventGroup: EventGroup): SyntheticEventGroupSpec {
          return SYNTHETIC_DATA_SPEC
        }
      }

    private val POPULATION_SPEC = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1L
          endVidInclusive = 1000L
        }
      }
    }
    private val VID_INDEX_MAP = InMemoryVidIndexMap.build(POPULATION_SPEC)

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

    private const val REACH_TOLERANCE = 1.0
    private const val FREQUENCY_DISTRIBUTION_TOLERANCE = 1.0
    private const val IMPRESSION_TOLERANCE = 1.0

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

    private fun computeExpectedImpression(
      eventGroupsList: List<RequisitionSpec.EventGroupEntry>,
      measurementSpec: MeasurementSpec,
    ): Long {
      val sampledVids = sampleVids(eventGroupsList, measurementSpec)
      val sampledImpression =
        MeasurementResults.computeImpression(
          sampledVids,
          measurementSpec.impression.maximumFrequencyPerUser,
        )
      return (sampledImpression / measurementSpec.vidSamplingInterval.width).toLong()
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
      return sampleVids(
        syntheticGeneratorEventQuery,
        eventGroupSpecs,
        measurementSpec.vidSamplingInterval.start,
        measurementSpec.vidSamplingInterval.width,
      )
    }
  }
}
