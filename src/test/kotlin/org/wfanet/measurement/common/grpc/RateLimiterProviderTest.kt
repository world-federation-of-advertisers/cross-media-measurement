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

package org.wfanet.measurement.common.grpc

import com.google.common.truth.Truth.assertThat
import io.grpc.Context
import kotlin.time.TestTimeSource
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ratelimit.RateLimiter
import org.wfanet.measurement.config.RateLimitConfigKt
import org.wfanet.measurement.config.rateLimitConfig

@RunWith(JUnit4::class)
class RateLimiterProviderTest {
  @Test
  fun `returns limiter using default rate limit`() {
    val rateLimiter: RateLimiter =
      RateLimiterProvider(RATE_LIMIT_CONFIG) { null }.getRateLimiter(Context.current(), "bar")

    assertThat(rateLimiter.tryAcquire()).isFalse()
  }

  @Test
  fun `returns limiter using per-method limit`() {
    val rateLimiter: RateLimiter =
      RateLimiterProvider(RATE_LIMIT_CONFIG) { null }.getRateLimiter(Context.current(), "foo")

    assertThat(rateLimiter.tryAcquire()).isTrue()
  }

  @Test
  fun `returns limiter using default rate limit from principal override`() {
    val rateLimiter: RateLimiter =
      RateLimiterProvider(RATE_LIMIT_CONFIG) { "alice" }.getRateLimiter(Context.current(), "bar")

    assertThat(rateLimiter.tryAcquire()).isTrue()
  }

  @Test
  fun `returns limiter using per-method rate limit from principal override`() {
    val rateLimiter: RateLimiter =
      RateLimiterProvider(RATE_LIMIT_CONFIG) { "alice" }.getRateLimiter(Context.current(), "foo")

    assertThat(rateLimiter.tryAcquire()).isFalse()
  }

  @Test
  fun `returns same rate limiter for multiple calls`() {
    val timeSource = TestTimeSource()
    val provider = RateLimiterProvider(RATE_LIMIT_CONFIG, timeSource) { null }
    val rateLimiter: RateLimiter = provider.getRateLimiter(Context.current(), "foo")

    assertThat(provider.getRateLimiter(Context.current(), "foo")).isSameInstanceAs(rateLimiter)
    assertThat(rateLimiter.tryAcquire()).isTrue()
    assertThat(rateLimiter.tryAcquire()).isFalse()
  }

  companion object {
    private val UNLIMITED = RateLimitConfigKt.methodRateLimit { maximumRequestCount = -1 }
    private val BLOCKED = RateLimitConfigKt.methodRateLimit { maximumRequestCount = 0 }

    private val RATE_LIMIT_CONFIG = rateLimitConfig {
      rateLimit =
        RateLimitConfigKt.rateLimit {
          defaultRateLimit = BLOCKED
          perMethodRateLimit["foo"] =
            RateLimitConfigKt.methodRateLimit {
              maximumRequestCount = 1
              averageRequestRate = 1.0
            }
        }
      principalRateLimitOverride["alice"] =
        RateLimitConfigKt.rateLimit {
          defaultRateLimit = UNLIMITED
          perMethodRateLimit["foo"] = BLOCKED
        }
    }
  }
}
