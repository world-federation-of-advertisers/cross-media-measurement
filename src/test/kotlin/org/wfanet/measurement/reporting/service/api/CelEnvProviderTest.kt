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
import io.grpc.Status
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.invocation.InvocationOnMock
import org.mockito.kotlin.KArgumentCaptor
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.mockito.stubbing.Answer
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt
import org.wfanet.measurement.api.v2alpha.ListEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.ListEventGroupMetadataDescriptorsResponse
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
private const val EVENT_GROUP_METADATA_DESCRIPTOR_NAME = "$DATA_PROVIDER_NAME/eventGroupMetadataDescriptors/abc"
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

  private lateinit var cacheProvider: CelEnvCacheProvider

  private fun initCacheProvider() {
    cacheProvider =
      CelEnvCacheProvider(
        EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub(
          grpcTestServerRule.channel
        ),
        Duration.ofMinutes(5),
        Dispatchers.Default,
        Clock.systemUTC(),
        3
      )
  }

  @Test
  fun `cache provider updates its cache only once if 2 update attempts around same time`() =
    runBlocking {
      initCacheProvider()
      cacheProvider.getTypeRegistryAndEnv()

      val eventGroupMetadataDescriptorsCaptor: KArgumentCaptor<ListEventGroupMetadataDescriptorsRequest> = argumentCaptor()
      verifyBlocking(cmmsEventGroupMetadataDescriptorsServiceMock, times(1)) { listEventGroupMetadataDescriptors(eventGroupMetadataDescriptorsCaptor.capture()) }
    }

  @Test
  fun `cache provider retries cache update if exception occurs`() = runBlocking {
    whenever(cmmsEventGroupMetadataDescriptorsServiceMock)
      .thenAnswer(object : Answer<ListEventGroupMetadataDescriptorsResponse> {
        private var count = 0

        override fun answer(p0: InvocationOnMock?): ListEventGroupMetadataDescriptorsResponse {
          if (count <= 0) {
            count++
            throw Status.DEADLINE_EXCEEDED.asRuntimeException()
          } else {
            return listEventGroupMetadataDescriptorsResponse {
              eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR
            }
          }
        }
      })

    initCacheProvider()

    cacheProvider.getTypeRegistryAndEnv()

    val eventGroupMetadataDescriptorsCaptor: KArgumentCaptor<ListEventGroupMetadataDescriptorsRequest> = argumentCaptor()
    verifyBlocking(cmmsEventGroupMetadataDescriptorsServiceMock, times(1)) { listEventGroupMetadataDescriptors(eventGroupMetadataDescriptorsCaptor.capture()) }
  }

  @Test
  fun `cache provider is not stopped by exceptions`() = runBlocking {
    whenever(cmmsEventGroupMetadataDescriptorsServiceMock)
      .thenAnswer(object : Answer<ListEventGroupMetadataDescriptorsResponse> {
        private var count = 0

        override fun answer(p0: InvocationOnMock?): ListEventGroupMetadataDescriptorsResponse {
          if (count <= 0) {
            count++
            throw Status.DEADLINE_EXCEEDED.asRuntimeException()
          } else {
            return listEventGroupMetadataDescriptorsResponse {
              eventGroupMetadataDescriptors += EVENT_GROUP_METADATA_DESCRIPTOR
            }
          }
        }
      })

    cacheProvider =
      CelEnvCacheProvider(
        EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub(
          grpcTestServerRule.channel
        ),
        Duration.ofMillis(200),
        Dispatchers.Default,
        Clock.systemUTC(),
        1
      )

    delay(350)

    val eventGroupMetadataDescriptorsCaptor: KArgumentCaptor<ListEventGroupMetadataDescriptorsRequest> = argumentCaptor()
    verifyBlocking(cmmsEventGroupMetadataDescriptorsServiceMock, times(2)) { listEventGroupMetadataDescriptors(eventGroupMetadataDescriptorsCaptor.capture()) }
  }
}
