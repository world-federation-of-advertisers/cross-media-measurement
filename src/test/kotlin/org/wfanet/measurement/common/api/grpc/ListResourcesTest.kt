/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common.api.grpc

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.StringValue
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.kotlin.AbstractCoroutineStub
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertThrows
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class ListResourcesTest {

  private class FakeStub(
    channel: Channel = NoopChannel,
    callOptions: CallOptions = CallOptions.DEFAULT,
  ) : AbstractCoroutineStub<FakeStub>(channel, callOptions) {
    override fun build(channel: Channel, callOptions: CallOptions): FakeStub =
      FakeStub(channel, callOptions)
  }

  private object NoopChannel : Channel() {
    override fun <RequestT : Any, ResponseT : Any> newCall(
      methodDescriptor: io.grpc.MethodDescriptor<RequestT, ResponseT>,
      callOptions: CallOptions,
    ): io.grpc.ClientCall<RequestT, ResponseT> = throw UnsupportedOperationException()

    override fun authority(): String = "fake"
  }

  @Test
  fun `listResourcesWithAdaptivePageSize halves on RESOURCE_EXHAUSTED and persists size`(): Unit = runBlocking {
      val stub = FakeStub()
      val pageSizesSeen = mutableListOf<Int>()
      val reductions = mutableListOf<Pair<Int, Int>>()
      val callCount = AtomicInteger(0)

      val pages =
        stub
          .listResourcesWithAdaptivePageSize<StringValue, String, FakeStub>(
            startingPageSize = 8,
            minPageSize = 1,
            onPageSizeReduced = { from, to -> reductions += from to to },
          ) { pageToken, pageSize ->
            pageSizesSeen += pageSize
            val attempt = callCount.incrementAndGet()
            when {
              attempt == 1 ->
                throw StatusException(Status.RESOURCE_EXHAUSTED.withDescription("too big"))
              attempt == 2 ->
                throw StatusException(Status.RESOURCE_EXHAUSTED.withDescription("still too big"))
              attempt == 3 ->
                ResourceList(
                  resources = listOf(StringValue.of("a"), StringValue.of("b")),
                  nextPageToken = "next",
                )
              else -> ResourceList(resources = listOf(StringValue.of("c")), nextPageToken = "")
            }
          }
          .toList()

      assertThat(pageSizesSeen).containsExactly(8, 4, 2, 2).inOrder()
      assertThat(reductions).containsExactly(8 to 4, 4 to 2).inOrder()
      assertThat(pages).hasSize(2)
      assertThat(pages.flatMap { it.resources }.map { it.value }).containsExactly("a", "b", "c")
    }

  @Test
  fun `listResourcesWithAdaptivePageSize rethrows when minimum page size still fails`(): Unit = runBlocking {
      val stub = FakeStub()
      val callCount = AtomicInteger(0)

      val e =
        assertThrows(StatusException::class.java) {
          runBlocking {
            stub
              .listResourcesWithAdaptivePageSize<StringValue, String, FakeStub>(
                startingPageSize = 2,
                minPageSize = 1,
              ) { _, _ ->
                callCount.incrementAndGet()
                throw StatusException(Status.RESOURCE_EXHAUSTED.withDescription("nope"))
              }
              .toList()
          }
        }
      assertThat(e.status.code).isEqualTo(Status.Code.RESOURCE_EXHAUSTED)
      assertThat(callCount.get()).isEqualTo(2)
    }

  @Test
  fun `listResourcesWithAdaptivePageSize does not retry non-RESOURCE_EXHAUSTED errors`(): Unit = runBlocking {
      val stub = FakeStub()
      val callCount = AtomicInteger(0)

      val e =
        assertThrows(StatusException::class.java) {
          runBlocking {
            stub
              .listResourcesWithAdaptivePageSize<StringValue, String, FakeStub>(
                startingPageSize = 8
              ) { _, _ ->
                callCount.incrementAndGet()
                throw StatusException(Status.UNAVAILABLE.withDescription("down"))
              }
              .toList()
          }
        }
      assertThat(e.status.code).isEqualTo(Status.Code.UNAVAILABLE)
      assertThat(callCount.get()).isEqualTo(1)
    }

  @Test
  fun `listResourcesWithAdaptivePageSize stops at empty nextPageToken`(): Unit = runBlocking {
    val stub = FakeStub()
    val pages =
      stub
        .listResourcesWithAdaptivePageSize<StringValue, String, FakeStub>(startingPageSize = 4) {
          _,
          _ ->
          ResourceList(listOf(StringValue.of("only")), nextPageToken = "")
        }
        .toList()
    assertThat(pages).hasSize(1)
  }
}
