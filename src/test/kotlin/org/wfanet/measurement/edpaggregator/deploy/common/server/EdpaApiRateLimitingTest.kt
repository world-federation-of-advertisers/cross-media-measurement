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

package org.wfanet.measurement.edpaggregator.deploy.common.server

import com.google.common.truth.Truth.assertThat
import io.grpc.Context
import io.grpc.Status
import io.grpc.StatusException
import kotlin.test.assertFailsWith
import kotlin.time.TestTimeSource
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.wfanet.measurement.common.grpc.RateLimiterProvider
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.withInterceptor
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest

@RunWith(JUnit4::class)
class EdpaApiRateLimitingTest {
  private val impressionMetadataServiceMock =
    mockService<ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineImplBase> {
      onBlocking { listImpressionMetadata(any()) } doReturn
        ListImpressionMetadataResponse.getDefaultInstance()
    }

  // Server wires the real interceptor + default config (bounding ListImpressionMetadata to a burst
  // of 2) exactly as the API servers do, so an over-limit call is exercised end-to-end.
  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(
      impressionMetadataServiceMock.withInterceptor(
        EdpaApiRateLimiting.interceptor(
          EdpaApiRateLimiting.defaultConfig(
            EdpaApiRateLimiting.PUBLIC_API_EXPENSIVE_METHODS,
            maximumRequestCount = 2,
            averageRequestRate = 1.0,
          )
        )
      )
    )
  }

  @Test
  fun `defaultConfig bounds each expensive public method`() {
    val config =
      EdpaApiRateLimiting.defaultConfig(
        EdpaApiRateLimiting.PUBLIC_API_EXPENSIVE_METHODS,
        maximumRequestCount = 3,
        averageRequestRate = 1.0,
      )
    // Frozen time source: no token refill, so the burst is exactly maximumRequestCount.
    val provider = RateLimiterProvider(config, TestTimeSource()) { null }

    for (method in EdpaApiRateLimiting.PUBLIC_API_EXPENSIVE_METHODS) {
      val rateLimiter = provider.getRateLimiter(Context.current(), method)
      assertThat(rateLimiter.tryAcquire()).isTrue()
      assertThat(rateLimiter.tryAcquire()).isTrue()
      assertThat(rateLimiter.tryAcquire()).isTrue()
      assertThat(rateLimiter.tryAcquire()).isFalse()
    }
  }

  @Test
  fun `defaultConfig bounds each expensive internal method`() {
    val config =
      EdpaApiRateLimiting.defaultConfig(
        EdpaApiRateLimiting.INTERNAL_API_EXPENSIVE_METHODS,
        maximumRequestCount = 1,
        averageRequestRate = 1.0,
      )
    val provider = RateLimiterProvider(config, TestTimeSource()) { null }

    for (method in EdpaApiRateLimiting.INTERNAL_API_EXPENSIVE_METHODS) {
      val rateLimiter = provider.getRateLimiter(Context.current(), method)
      assertThat(rateLimiter.tryAcquire()).isTrue()
      assertThat(rateLimiter.tryAcquire()).isFalse()
    }
  }

  @Test
  fun `defaultConfig leaves non-expensive methods unlimited`() {
    val config = EdpaApiRateLimiting.defaultConfig(EdpaApiRateLimiting.PUBLIC_API_EXPENSIVE_METHODS)
    val provider = RateLimiterProvider(config, TestTimeSource()) { null }

    val rateLimiter =
      provider.getRateLimiter(
        Context.current(),
        "wfa.measurement.edpaggregator.v1alpha.ImpressionMetadataService/CreateImpressionMetadata",
      )
    repeat(1_000) { assertThat(rateLimiter.tryAcquire()).isTrue() }
  }

  @Test
  fun `interceptor rejects an expensive method over the limit with UNAVAILABLE`(): Unit =
    runBlocking {
      val stub =
        ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub(
          grpcTestServerRule.channel
        )
      val request = listImpressionMetadataRequest { parent = "dataProviders/123" }

      // A burst of 2 is allowed; the third call trips the global per-method limit.
      stub.listImpressionMetadata(request)
      stub.listImpressionMetadata(request)
      val exception = assertFailsWith<StatusException> { stub.listImpressionMetadata(request) }

      assertThat(exception.status.code).isEqualTo(Status.Code.UNAVAILABLE)
    }
}
