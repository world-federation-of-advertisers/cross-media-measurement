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

import io.grpc.Status
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.KArgumentCaptor
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.timeout
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt
import org.wfanet.measurement.api.v2alpha.ListEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.eventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.TestMetadataMessageKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testMetadataMessage
import org.wfanet.measurement.api.v2alpha.listEventGroupMetadataDescriptorsResponse
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService

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
    mockService {
      onBlocking { listEventGroupMetadataDescriptors(any()) }
        .thenReturn(
          listEventGroupMetadataDescriptorsResponse {
            eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR
          }
        )
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(cmmsEventGroupMetadataDescriptorsServiceMock)
  }

  @Test
  fun `cache provider updates its cache only once if 2 update attempts around same time`() =
    runBlocking {
      val coroutineScope = CoroutineScope(coroutineContext + SupervisorJob())
      val cacheProvider =
        CelEnvCacheProvider(
          EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub(
            grpcTestServerRule.channel
          ),
          Duration.ofMinutes(5),
          coroutineScope,
          Clock.systemUTC(),
          3
        )
      cacheProvider.getTypeRegistryAndEnv()

      val eventGroupMetadataDescriptorsCaptor:
        KArgumentCaptor<ListEventGroupMetadataDescriptorsRequest> =
        argumentCaptor()

      verifyBlocking(cmmsEventGroupMetadataDescriptorsServiceMock, times(1)) {
        listEventGroupMetadataDescriptors(eventGroupMetadataDescriptorsCaptor.capture())
      }
      coroutineScope.cancel()
    }

  @Test
  @kotlinx.coroutines.ExperimentalCoroutinesApi
  fun `cache provider retries cache update if exception occurs`() =
    runTest(UnconfinedTestDispatcher()) {
      whenever(
          cmmsEventGroupMetadataDescriptorsServiceMock.listEventGroupMetadataDescriptors(any())
        )
        .thenThrow(Status.DEADLINE_EXCEEDED.asRuntimeException())
        .thenReturn(
          listEventGroupMetadataDescriptorsResponse {
            eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR
          }
        )

      val coroutineScope = CoroutineScope(coroutineContext + SupervisorJob())
      CelEnvCacheProvider(
        EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub(
          grpcTestServerRule.channel
        ),
        Duration.ofMinutes(5),
        coroutineScope,
        Clock.systemUTC(),
        1
      )

      val eventGroupMetadataDescriptorsCaptor:
        KArgumentCaptor<ListEventGroupMetadataDescriptorsRequest> =
        argumentCaptor()

      verifyBlocking(cmmsEventGroupMetadataDescriptorsServiceMock, timeout(10).times(1)) {
        listEventGroupMetadataDescriptors(eventGroupMetadataDescriptorsCaptor.capture())
      }
      coroutineScope.cancel()
    }

  @Test
  @kotlinx.coroutines.ExperimentalCoroutinesApi
  fun `cache provider is not stopped by exceptions`() =
    runTest(UnconfinedTestDispatcher()) {
      whenever(
          cmmsEventGroupMetadataDescriptorsServiceMock.listEventGroupMetadataDescriptors(any())
        )
        .thenThrow(Status.DEADLINE_EXCEEDED.asRuntimeException())
        .thenReturn(
          listEventGroupMetadataDescriptorsResponse {
            eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR
          }
        )

      val clock = Clock.systemUTC()
      val fakeClock: Clock = mock()
      whenever(fakeClock.instant()).thenReturn(clock.instant())

      val coroutineScope = CoroutineScope(coroutineContext + SupervisorJob())
      CelEnvCacheProvider(
        EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub(
          grpcTestServerRule.channel
        ),
        Duration.ofMillis(100),
        coroutineScope,
        fakeClock,
        1
      )

      advanceTimeBy(150)
      whenever(fakeClock.instant()).thenReturn(clock.instant().plusMillis(150))

      val eventGroupMetadataDescriptorsCaptor:
        KArgumentCaptor<ListEventGroupMetadataDescriptorsRequest> =
        argumentCaptor()

      verifyBlocking(cmmsEventGroupMetadataDescriptorsServiceMock, timeout(50).atLeast(2)) {
        listEventGroupMetadataDescriptors(eventGroupMetadataDescriptorsCaptor.capture())
      }
      coroutineScope.cancel()
    }
}
