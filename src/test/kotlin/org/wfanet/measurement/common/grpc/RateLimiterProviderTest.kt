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
import org.wfanet.measurement.config.copy
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

  @Test
  fun `method-only principal override exempts only that principal from the method limit`() {
    val provider =
      RateLimiterProvider(METHOD_ONLY_RATE_LIMIT_CONFIG, TestTimeSource()) { PRINCIPAL_KEY.get(it) }
    val edpaContext = Context.current().withValue(PRINCIPAL_KEY, "edpa")
    val otherContext = Context.current().withValue(PRINCIPAL_KEY, "other")

    assertThat(provider.getRateLimiter(edpaContext, LIST_REQUISITIONS).tryAcquire()).isTrue()
    assertThat(provider.getRateLimiter(otherContext, LIST_REQUISITIONS).tryAcquire()).isTrue()
    assertThat(provider.getRateLimiter(otherContext, LIST_REQUISITIONS).tryAcquire()).isFalse()
    assertThat(provider.getRateLimiter(edpaContext, LIST_REQUISITIONS).tryAcquire()).isTrue()
    assertThat(provider.getRateLimiter(Context.current(), LIST_REQUISITIONS).tryAcquire()).isFalse()
  }

  @Test
  fun `method-only principal override shares regular per-method limit for other methods`() {
    val provider =
      RateLimiterProvider(METHOD_ONLY_RATE_LIMIT_CONFIG, TestTimeSource()) { PRINCIPAL_KEY.get(it) }
    val edpaContext = Context.current().withValue(PRINCIPAL_KEY, "edpa")

    assertThat(provider.getRateLimiter(edpaContext, GET_REQUISITION).tryAcquire()).isTrue()
    assertThat(provider.getRateLimiter(Context.current(), GET_REQUISITION).tryAcquire()).isFalse()
    assertThat(provider.getRateLimiter(edpaContext, GET_REQUISITION).tryAcquire()).isFalse()
  }

  @Test
  fun `method-only principal override shares regular default limit across other methods`() {
    val provider =
      RateLimiterProvider(METHOD_ONLY_RATE_LIMIT_CONFIG, TestTimeSource()) { PRINCIPAL_KEY.get(it) }
    val edpaContext = Context.current().withValue(PRINCIPAL_KEY, "edpa")

    assertThat(provider.getRateLimiter(edpaContext, "bar").tryAcquire()).isTrue()
    assertThat(provider.getRateLimiter(Context.current(), "baz").tryAcquire()).isFalse()
    assertThat(provider.getRateLimiter(edpaContext, "bar").tryAcquire()).isFalse()
  }

  @Test
  fun `explicit blocked principal default overrides regular per-method limit`() {
    val config =
      METHOD_ONLY_RATE_LIMIT_CONFIG.copy {
        principalRateLimitOverride["edpa"] =
          RateLimitConfigKt.rateLimit {
            defaultRateLimit = BLOCKED
            perMethodRateLimit[LIST_REQUISITIONS] = UNLIMITED
          }
      }
    val provider = RateLimiterProvider(config, TestTimeSource()) { "edpa" }

    assertThat(provider.getRateLimiter(Context.current(), GET_REQUISITION).tryAcquire()).isFalse()
    assertThat(provider.getRateLimiter(Context.current(), LIST_REQUISITIONS).tryAcquire()).isTrue()
  }

  companion object {
    private const val LIST_REQUISITIONS =
      "wfa.measurement.api.v2alpha.Requisitions/ListRequisitions"
    private const val GET_REQUISITION = "wfa.measurement.api.v2alpha.Requisitions/GetRequisition"
    private val PRINCIPAL_KEY = Context.key<String>("principal")
    private val LIMITED =
      RateLimitConfigKt.methodRateLimit {
        maximumRequestCount = 1
        averageRequestRate = 1.0
      }
    private val UNLIMITED = RateLimitConfigKt.methodRateLimit { maximumRequestCount = -1 }
    private val BLOCKED = RateLimitConfigKt.methodRateLimit { maximumRequestCount = 0 }

    private val METHOD_ONLY_RATE_LIMIT_CONFIG = rateLimitConfig {
      rateLimit =
        RateLimitConfigKt.rateLimit {
          defaultRateLimit = LIMITED
          perMethodRateLimit[LIST_REQUISITIONS] = LIMITED
          perMethodRateLimit[GET_REQUISITION] = LIMITED
        }
      principalRateLimitOverride["edpa"] =
        RateLimitConfigKt.rateLimit { perMethodRateLimit[LIST_REQUISITIONS] = UNLIMITED }
    }

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
