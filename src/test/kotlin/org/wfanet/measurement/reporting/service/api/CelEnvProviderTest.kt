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

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any.pack
import com.google.protobuf.DynamicMessage
import io.grpc.Status
import io.grpc.StatusException
import java.time.Duration
import kotlin.test.assertFailsWith
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.KArgumentCaptor
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyBlocking
import org.projectnessie.cel.common.types.Err
import org.projectnessie.cel.common.types.ref.Val
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ListEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.copy
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testMetadataMessage
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testParentMetadataMessage
import org.wfanet.measurement.api.v2alpha.listEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.listEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupKt
import org.wfanet.measurement.reporting.v2alpha.eventGroup

private const val METADATA_FIELD = "metadata.metadata"
private const val MAX_PAGE_SIZE = 1000
private val TEST_MESSAGE = testMetadataMessage { publisherId = 15 }
private const val DATA_PROVIDER_NAME = "dataProviders/123"
private const val EVENT_GROUP_METADATA_DESCRIPTOR_NAME =
  "$DATA_PROVIDER_NAME/eventGroupMetadataDescriptors/abc"
private val EVENT_GROUP_METADATA_DESCRIPTOR = eventGroupMetadataDescriptor {
  name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
  descriptorSet = ProtoReflection.buildFileDescriptorSet(TEST_MESSAGE.descriptorForType)
}

@RunWith(JUnit4::class)
@OptIn(ExperimentalCoroutinesApi::class) // For `advanceTimeBy`
class CelEnvProviderTest {
  private val cmmsEventGroupMetadataDescriptorsServiceMock:
    EventGroupMetadataDescriptorsCoroutineImplBase =
    mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(cmmsEventGroupMetadataDescriptorsServiceMock)
  }

  @Test
  fun `getTypeRegistryAndEnv returns cached value`() {
    cmmsEventGroupMetadataDescriptorsServiceMock.stub {
      onBlocking { listEventGroupMetadataDescriptors(any()) }
        .thenReturn(
          listEventGroupMetadataDescriptorsResponse {
            eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR
          }
        )
    }

    val typeRegistryAndEnv: CelEnvProvider.TypeRegistryAndEnv = runBlocking {
      CelEnvCacheProvider(
          EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub(
            grpcTestServerRule.channel
          ),
          REPORTING_EVENT_GROUP_DESCRIPTOR,
          Duration.ofMinutes(5),
          emptyList(),
          coroutineContext,
        )
        .use { it.getTypeRegistryAndEnv() }
    }

    verifyTypeRegistryAndEnv(typeRegistryAndEnv)
    verifyProtoArgument(
        cmmsEventGroupMetadataDescriptorsServiceMock,
        EventGroupMetadataDescriptorsCoroutineImplBase::listEventGroupMetadataDescriptors,
      )
      .isEqualTo(
        listEventGroupMetadataDescriptorsRequest {
          parent = "dataProviders/-"
          pageSize = MAX_PAGE_SIZE
        }
      )
  }

  @Test
  fun `cache provider retries initial cache sync when status is retryable`() {
    cmmsEventGroupMetadataDescriptorsServiceMock.stub {
      onBlocking { listEventGroupMetadataDescriptors(any()) }
        .thenThrow(Status.DEADLINE_EXCEEDED.asRuntimeException())
        .thenReturn(
          listEventGroupMetadataDescriptorsResponse {
            eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR
          }
        )
    }

    runTest {
      CelEnvCacheProvider(
          EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub(
            grpcTestServerRule.channel
          ),
          REPORTING_EVENT_GROUP_DESCRIPTOR,
          Duration.ofMinutes(5),
          emptyList(),
          coroutineContext,
          numRetriesInitialSync = 1,
        )
        .use {
          advanceTimeBy(CelEnvCacheProvider.RETRY_DELAY.toMillis())
          val typeRegistryAndEnv = it.getTypeRegistryAndEnv()
          verifyTypeRegistryAndEnv(typeRegistryAndEnv)
        }
    }

    val expectedRequest = listEventGroupMetadataDescriptorsRequest {
      parent = "dataProviders/-"
      pageSize = MAX_PAGE_SIZE
    }
    val listDescriptorsRequestCaptor: KArgumentCaptor<ListEventGroupMetadataDescriptorsRequest> =
      argumentCaptor()
    verifyBlocking(cmmsEventGroupMetadataDescriptorsServiceMock, times(2)) {
      listEventGroupMetadataDescriptors(listDescriptorsRequestCaptor.capture())
    }
    assertThat(listDescriptorsRequestCaptor.allValues)
      .containsExactly(expectedRequest, expectedRequest)
  }

  @Test
  fun `cache provider throws exception when status is not retryable`() {
    cmmsEventGroupMetadataDescriptorsServiceMock.stub {
      onBlocking { listEventGroupMetadataDescriptors(any()) }
        .thenThrow(Status.UNKNOWN.asRuntimeException())
    }
    val numRetries = 3

    val exception =
      assertFailsWith<Exception> {
        runTest {
          CelEnvCacheProvider(
              EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub(
                grpcTestServerRule.channel
              ),
              REPORTING_EVENT_GROUP_DESCRIPTOR,
              Duration.ofMinutes(5),
              emptyList(),
              coroutineContext,
              numRetriesInitialSync = numRetries,
            )
            .use { it.getTypeRegistryAndEnv() }
        }
      }

    verifyBlocking(cmmsEventGroupMetadataDescriptorsServiceMock) {
      listEventGroupMetadataDescriptors(any())
    }
    assertThat(exception).hasCauseThat().isInstanceOf(StatusException::class.java)
    assertThat((exception.cause as StatusException).status.code).isEqualTo(Status.Code.UNKNOWN)
  }

  @Test
  fun `cache provider throws exception when retry limit exceeded`() {
    cmmsEventGroupMetadataDescriptorsServiceMock.stub {
      onBlocking { listEventGroupMetadataDescriptors(any()) }
        .thenThrow(Status.DEADLINE_EXCEEDED.asRuntimeException())
    }
    val numRetries = 3

    val exception =
      assertFailsWith<Exception> {
        runTest {
          CelEnvCacheProvider(
              EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub(
                grpcTestServerRule.channel
              ),
              REPORTING_EVENT_GROUP_DESCRIPTOR,
              Duration.ofMinutes(5),
              emptyList(),
              coroutineContext,
              numRetriesInitialSync = numRetries,
            )
            .use { it.getTypeRegistryAndEnv() }
        }
      }

    verifyBlocking(cmmsEventGroupMetadataDescriptorsServiceMock, times(numRetries + 1)) {
      listEventGroupMetadataDescriptors(any())
    }
    assertThat(exception).hasCauseThat().isInstanceOf(StatusException::class.java)
    assertThat((exception.cause as StatusException).status.code)
      .isEqualTo(Status.Code.DEADLINE_EXCEEDED)
  }

  @Test
  fun `cache provider syncs cache again after interval`() {
    val testMessage = testParentMetadataMessage { name = "test" }
    val eventGroupMetadataDescriptor = eventGroupMetadataDescriptor {
      name = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
      descriptorSet = ProtoReflection.buildFileDescriptorSet(testMessage.descriptorForType)
    }
    cmmsEventGroupMetadataDescriptorsServiceMock.stub {
      onBlocking { listEventGroupMetadataDescriptors(any()) }
        .thenReturn(
          listEventGroupMetadataDescriptorsResponse {
            eventGroupMetadataDescriptors += eventGroupMetadataDescriptor
          }
        )
        .thenReturn(
          listEventGroupMetadataDescriptorsResponse {
            eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR
          }
        )
    }
    val cacheRefreshInterval = Duration.ofMillis(500)

    runTest {
      val typeRegistryAndEnv: CelEnvProvider.TypeRegistryAndEnv =
        CelEnvCacheProvider(
            EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub(
              grpcTestServerRule.channel
            ),
            REPORTING_EVENT_GROUP_DESCRIPTOR,
            cacheRefreshInterval,
            emptyList(),
            coroutineContext,
          )
          .use {
            it.getTypeRegistryAndEnv()

            // Verify only called once for initial sync.
            verify(cmmsEventGroupMetadataDescriptorsServiceMock)
              .listEventGroupMetadataDescriptors(any())

            advanceTimeBy(cacheRefreshInterval.toMillis() + 1)
            it.waitForSync()
            it.getTypeRegistryAndEnv()
          }

      // Verify called again.
      verify(cmmsEventGroupMetadataDescriptorsServiceMock, times(2))
        .listEventGroupMetadataDescriptors(any())

      // Verify against latest response.
      verifyTypeRegistryAndEnv(typeRegistryAndEnv)
    }
  }

  companion object {
    private val REPORTING_EVENT_GROUP_DESCRIPTOR = EventGroup.getDescriptor()

    private fun verifyTypeRegistryAndEnv(typeRegistryAndEnv: CelEnvProvider.TypeRegistryAndEnv) {
      val eventGroup = eventGroup {
        metadata =
          EventGroupKt.metadata {
            eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
            metadata = pack(TEST_MESSAGE.copy { publisherId = 15 })
          }
      }
      val eventGroup2 = eventGroup {
        metadata =
          EventGroupKt.metadata {
            eventGroupMetadataDescriptor = EVENT_GROUP_METADATA_DESCRIPTOR_NAME
            metadata = pack(TEST_MESSAGE.copy { publisherId = 9 })
          }
      }
      val filter = "metadata.metadata.publisher_id > 10"

      assertThat(filterEventGroups(listOf(eventGroup, eventGroup2), filter, typeRegistryAndEnv))
        .containsExactly(eventGroup)
    }

    private fun filterEventGroups(
      eventGroups: Iterable<EventGroup>,
      filter: String,
      typeRegistryAndEnv: CelEnvProvider.TypeRegistryAndEnv,
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
                  metadata.value,
                ),
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
