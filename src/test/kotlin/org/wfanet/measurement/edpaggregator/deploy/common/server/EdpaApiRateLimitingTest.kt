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
import kotlin.time.TestTimeSource
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.RateLimiterProvider

@RunWith(JUnit4::class)
class EdpaApiRateLimitingTest {
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
  fun `interceptor is built from config`() {
    val interceptor =
      EdpaApiRateLimiting.interceptor(
        EdpaApiRateLimiting.defaultConfig(EdpaApiRateLimiting.PUBLIC_API_EXPENSIVE_METHODS)
      )
    assertThat(interceptor).isNotNull()
  }
}
