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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Message
import io.grpc.BindableService
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.CreateEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.CreateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupMetadataDescriptorRequest
import org.wfanet.measurement.api.v2alpha.UpdateEventGroupRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessage
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Banner
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Video
import org.wfanet.measurement.api.v2alpha.listEventGroupsResponse
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyAndCapture
import org.wfanet.measurement.consent.client.measurementconsumer.decryptMetadata
import org.wfanet.measurement.integration.common.SyntheticGenerationSpecs
import org.wfanet.measurement.loadtest.config.EventGroupMetadata
import org.wfanet.measurement.loadtest.config.TestIdentifiers

class LegacyMetadataEdpSimulatorTest : AbstractEdpSimulatorTest() {
  private data class EventGroupOptions(
    override val referenceIdSuffix: String,
    override val syntheticDataSpec: SyntheticEventGroupSpec,
    override val eventTemplates: List<EventGroup.EventTemplate>,
    override val legacyMetadata: Message,
  ) : LegacyMetadataEdpSimulator.EventGroupOptions

  private val eventGroupMetadataDescriptorsServiceMock:
    EventGroupMetadataDescriptorsCoroutineImplBase =
    mockService {
      onBlocking { createEventGroupMetadataDescriptor(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<CreateEventGroupMetadataDescriptorRequest>(0)
          request.eventGroupMetadataDescriptor.copy { name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME }
        }
    }

  override val services: Iterable<BindableService>
    get() = super.services + eventGroupMetadataDescriptorsServiceMock

  private val measurementConsumersStub by lazy {
    MeasurementConsumersCoroutineStub(grpcTestServerRule.channel)
  }

  private val eventGroupMetadataDescriptorsStub by lazy {
    EventGroupMetadataDescriptorsCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun `ensureEventGroups creates EventGroup and EventGroupMetadataDescriptor`() {
    val knownEventGroupMetadataTypes = listOf(SyntheticEventGroupSpec.getDescriptor().file)
    val edpSimulator =
      LegacyMetadataEdpSimulator(
        EDP_DATA,
        EDP_DISPLAY_NAME,
        MEASUREMENT_CONSUMER_NAME,
        measurementConsumersStub,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        listOf(
          EventGroupOptions("", SYNTHETIC_DATA_SPEC, TEST_EVENT_TEMPLATES, SYNTHETIC_DATA_SPEC)
        ),
        eventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        knownEventGroupMetadataTypes,
        vidIndexMap = VID_INDEX_MAP,
      )

    runBlocking { edpSimulator.ensureEventGroups() }

    // Verify metadata descriptor is created.
    verifyBlocking(eventGroupMetadataDescriptorsServiceMock) {
      createEventGroupMetadataDescriptor(any())
    }

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
  fun `ensureEventGroups updates EventGroup`() {
    eventGroupsServiceMock.stub {
      onBlocking { listEventGroups(any()) }
        .thenReturn(
          listEventGroupsResponse {
            eventGroups += eventGroup {
              name = EVENT_GROUP_NAME
              measurementConsumer = MEASUREMENT_CONSUMER_NAME
              eventGroupReferenceId =
                "${TestIdentifiers.SIMULATOR_EVENT_GROUP_REFERENCE_ID_PREFIX}-${EDP_DISPLAY_NAME}"
            }
          }
        )
    }
    val edpSimulator =
      LegacyMetadataEdpSimulator(
        EDP_DATA,
        EDP_DISPLAY_NAME,
        MEASUREMENT_CONSUMER_NAME,
        measurementConsumersStub,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        listOf(
          EventGroupOptions("", SYNTHETIC_DATA_SPEC, TEST_EVENT_TEMPLATES, SYNTHETIC_DATA_SPEC)
        ),
        eventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        knownEventGroupMetadataTypes = emptyList(),
        vidIndexMap = VID_INDEX_MAP,
      )

    runBlocking { edpSimulator.ensureEventGroups() }

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
  fun `ensureEventGroups updates EventGroupMetadataDescriptor`() {
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
      LegacyMetadataEdpSimulator(
        EDP_DATA,
        EDP_DISPLAY_NAME,
        MEASUREMENT_CONSUMER_NAME,
        measurementConsumersStub,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        listOf(
          EventGroupOptions("", SYNTHETIC_DATA_SPEC, TEST_EVENT_TEMPLATES, SYNTHETIC_DATA_SPEC)
        ),
        eventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        knownEventGroupMetadataTypes = emptyList(),
        vidIndexMap = VID_INDEX_MAP,
      )

    runBlocking { edpSimulator.ensureEventGroups() }

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
    val eventGroupsOptions =
      listOf(
        EventGroupOptions(
          "-foo",
          SyntheticGenerationSpecs.SYNTHETIC_DATA_SPECS_SMALL[0],
          TEST_EVENT_TEMPLATES,
          SyntheticGenerationSpecs.SYNTHETIC_DATA_SPECS_SMALL[0],
        ),
        EventGroupOptions(
          "-bar",
          SyntheticGenerationSpecs.SYNTHETIC_DATA_SPECS_SMALL[1],
          TEST_EVENT_TEMPLATES,
          SyntheticGenerationSpecs.SYNTHETIC_DATA_SPECS_SMALL[1],
        ),
      )
    val edpSimulator =
      LegacyMetadataEdpSimulator(
        EDP_DATA,
        EDP_DISPLAY_NAME,
        MEASUREMENT_CONSUMER_NAME,
        measurementConsumersStub,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        eventGroupsOptions,
        eventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        knownEventGroupMetadataTypes = emptyList(),
        vidIndexMap = VID_INDEX_MAP,
      )

    runBlocking { edpSimulator.ensureEventGroups() }

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
          eventGroupsOptions
            .find {
              it.referenceIdSuffix ==
                AbstractEdpSimulator.getEventGroupReferenceIdSuffix(
                  createRequest.eventGroup,
                  EDP_DISPLAY_NAME,
                )
            }
            ?.syntheticDataSpec
        )
    }
  }

  @Test
  fun `ensureEventGroups throws IllegalArgumentException when metadata message types mismatch`() {
    val edpSimulator =
      LegacyMetadataEdpSimulator(
        EDP_DATA,
        EDP_DISPLAY_NAME,
        MEASUREMENT_CONSUMER_NAME,
        measurementConsumersStub,
        certificatesStub,
        modelLinesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStubMap,
        listOf(
          EventGroupOptions("-foo", SYNTHETIC_DATA_SPEC, TEST_EVENT_TEMPLATES, SYNTHETIC_DATA_SPEC),
          EventGroupOptions("-bar", SYNTHETIC_DATA_SPEC, TEST_EVENT_TEMPLATES, TEST_METADATA),
        ),
        eventQuery,
        dummyThrottler,
        privacyBudgetManager,
        TRUSTED_CERTIFICATES,
        knownEventGroupMetadataTypes = emptyList(),
        vidIndexMap = VID_INDEX_MAP,
      )

    val exception =
      assertFailsWith<IllegalArgumentException> { runBlocking { edpSimulator.ensureEventGroups() } }

    assertThat(exception).hasMessageThat().contains("type")
  }

  companion object {
    private const val EVENT_GROUP_METADATA_DESCRIPTOR_NAME =
      "dataProviders/foo/eventGroupMetadataDescriptors/bar"

    private val TEST_EVENT_TEMPLATES =
      LegacyMetadataEdpSimulator.buildEventTemplates(TestEvent.getDescriptor())
    private val TEST_METADATA = EventGroupMetadata.testMetadata(1)

    private val eventQuery =
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
  }
}
