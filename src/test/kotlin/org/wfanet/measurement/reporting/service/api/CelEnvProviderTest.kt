/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api

import com.google.protobuf.Any.pack
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.DynamicMessage
import io.grpc.Status
import java.time.Clock
import java.time.Duration
import kotlin.test.assertFailsWith
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.KArgumentCaptor
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.atLeast
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.projectnessie.cel.common.types.Err
import org.projectnessie.cel.common.types.ref.Val
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt
import org.wfanet.measurement.api.v2alpha.ListEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessageKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.copy
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testMetadataMessage
import org.wfanet.measurement.api.v2alpha.listEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.reporting.v1alpha.EventGroup
import org.wfanet.measurement.reporting.v1alpha.EventGroupKt
import org.wfanet.measurement.reporting.v1alpha.eventGroup

private const val METADATA_FIELD = "metadata.metadata"
private const val MAX_PAGE_SIZE = 1000
private val TEST_MESSAGE = testMetadataMessage {
  name = TestMetadataMessageKt.name { value = "Bob" }
  age = TestMetadataMessageKt.age { value = 15 }
  duration = TestMetadataMessageKt.duration { value = 20 }
}
private const val DATA_PROVIDER_NAME = "dataProviders/123"
private const val EVENT_GROUP_METADATA_DESCRIPTOR_NAME =
  "$DATA_PROVIDER_NAME/eventGroupMetadataDescriptors/abc"
private val EVENT_GROUP_METADATA_DESCRIPTOR = eventGroupMetadataDescriptor {
  name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
  descriptorSet = ProtoReflection.buildFileDescriptorSet(TEST_MESSAGE.descriptorForType)
}

@RunWith(JUnit4::class)
class CelEnvProviderTest {
  private val cmmsEventGroupMetadataDescriptorsServiceMock:
    EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase =
      mockService {}

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(cmmsEventGroupMetadataDescriptorsServiceMock)
  }

  @Test
  @OptIn(ExperimentalCoroutinesApi::class) // For `runTest`
  fun `cache provider retries initial cache sync if exception occurs`() =
    runTest(UnconfinedTestDispatcher()) {
      whenever(cmmsEventGroupMetadataDescriptorsServiceMock.listEventGroupMetadataDescriptors(any()))
        .thenThrow(Status.DEADLINE_EXCEEDED.asRuntimeException())
        .thenReturn(
          listEventGroupMetadataDescriptorsResponse {
            eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR
          }
        )

      CelEnvCacheProvider(
          EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub(
            grpcTestServerRule.channel
          ),
          Duration.ofMinutes(5),
          coroutineContext,
          Clock.systemUTC(),
          1
        )
        .use {
          val typeRegistryAndEnv = it.getTypeRegistryAndEnv()
          val eventGroup = eventGroup {
            metadata = EventGroupKt.metadata {
              eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
              metadata = pack(TEST_MESSAGE.copy {
                age = TestMetadataMessageKt.age {
                  value = 15
                }
              })
            }
          }
          val eventGroup2 = eventGroup {
            metadata = EventGroupKt.metadata {
              eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
              metadata = pack(TEST_MESSAGE.copy {
                age = TestMetadataMessageKt.age {
                  value = 9
                }
              })
            }
          }
          val filter = "metadata.metadata.age.value > 10"

          assertThat(filterEventGroups(listOf(eventGroup, eventGroup2), filter, typeRegistryAndEnv))
            .containsExactly(eventGroup)
        }

      val eventGroupMetadataDescriptorsCaptor:
        KArgumentCaptor<ListEventGroupMetadataDescriptorsRequest> =
        argumentCaptor()

      verify(cmmsEventGroupMetadataDescriptorsServiceMock, atLeast(2))
        .listEventGroupMetadataDescriptors(eventGroupMetadataDescriptorsCaptor.capture())

      eventGroupMetadataDescriptorsCaptor.allValues.forEach {
        assertThat(it)
          .isEqualTo(listEventGroupMetadataDescriptorsRequest {
            parent = "dataProviders/-"
            pageSize = MAX_PAGE_SIZE
          })
      }
    }

  @Test
  @OptIn(ExperimentalCoroutinesApi::class) // For `runTest`
  fun `cache provider throws EXCEPTION when initial sync fails`() {
    assertFailsWith<Exception> {
      runTest(UnconfinedTestDispatcher()) {
        whenever(
          cmmsEventGroupMetadataDescriptorsServiceMock.listEventGroupMetadataDescriptors(any())
        )
          .thenThrow(Status.DEADLINE_EXCEEDED.asRuntimeException())

        val celEnvCacheProvider = CelEnvCacheProvider(
          EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub(
            grpcTestServerRule.channel
          ),
          Duration.ofMinutes(5),
          coroutineContext,
          Clock.systemUTC(),
          0
        )

        celEnvCacheProvider.getTypeRegistryAndEnv()
      }
    }
  }

  companion object {
    private fun filterEventGroups(
      eventGroups: Iterable<EventGroup>,
      filter: String,
      typeRegistryAndEnv: CelEnvProvider.TypeRegistryAndEnv
    ): List<EventGroup> {
      val env = typeRegistryAndEnv.env
      val typeRegistry = typeRegistryAndEnv.typeRegistry

      val astAndIssues = env.compile(filter)
      val program = env.program(astAndIssues.ast)

      eventGroups
        .distinctBy { it.metadata.metadata.typeUrl }
        .forEach {
          val typeUrl = it.metadata.metadata.typeUrl
          typeRegistry.getDescriptorForTypeUrl(typeUrl)
            ?: throw IllegalStateException(
              "${it.metadata.eventGroupMetadataDescriptor} does not contain descriptor for $typeUrl"
            )
        }

      return eventGroups.filter { eventGroup ->
        val variables: Map<String, Any> =
          mutableMapOf<String, Any>().apply {
            for (fieldDescriptor in eventGroup.descriptorForType.fields) {
              put(fieldDescriptor.name, eventGroup.getField(fieldDescriptor))
            }
            // TODO(projectnessie/cel-java#295): Remove when fixed.
            if (eventGroup.hasMetadata()) {
              val metadata: com.google.protobuf.Any = eventGroup.metadata.metadata
              put(
                METADATA_FIELD,
                DynamicMessage.parseFrom(
                  typeRegistry.getDescriptorForTypeUrl(metadata.typeUrl),
                  metadata.value
                )
              )
            }
          }
        val result: Val = program.eval(variables).`val`
        if (result is Err) {
          throw result.toRuntimeException()
        }
        result.booleanValue()
      }
    }
  }
}
