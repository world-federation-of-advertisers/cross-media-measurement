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

import io.grpc.Context
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.TimeSource
import org.wfanet.measurement.common.ratelimit.RateLimiter
import org.wfanet.measurement.common.ratelimit.TokenBucket
import org.wfanet.measurement.config.RateLimitConfig

/** Config-based provider of [RateLimiter] instances for gRPC calls. */
class RateLimiterProvider(
  private val config: RateLimitConfig,
  private val timeSource: TimeSource.WithComparableMarks,
  /**
   * Function which returns the principal identifier from the specified gRPC [Context], or `null` if
   * the principal was not found.
   */
  private val getPrincipalIdentifier: (context: Context) -> String?,
) {
  private val rateLimit = RateLimit(config.rateLimit, timeSource)
  private val rateLimitOverrides = ConcurrentHashMap<String, RateLimit>()

  constructor(
    config: RateLimitConfig,
    getPrincipalIdentifier: (context: Context) -> String?,
  ) : this(config, TimeSource.Monotonic, getPrincipalIdentifier)

  /** Returns a [RateLimiter] instance for the specified gRPC call. */
  fun getRateLimiter(context: Context, fullMethodName: String): RateLimiter {
    return getRateLimiter(getPrincipalIdentifier(context), fullMethodName)
  }

  private fun getRateLimiter(principalIdentifier: String?, methodName: String): RateLimiter {
    val rateLimit =
      if (principalIdentifier == null) {
        this.rateLimit
      } else {
        val rateLimitOverride = config.principalRateLimitOverrideMap[principalIdentifier]
        if (rateLimitOverride == null) {
          rateLimit
        } else {
          rateLimitOverrides.getOrPut(principalIdentifier) {
            RateLimit(rateLimitOverride, timeSource)
          }
        }
      }

    return rateLimit.getRateLimiter(methodName)
  }

  private class RateLimit(
    private val config: RateLimitConfig.RateLimit,
    private val timeSource: TimeSource.WithComparableMarks,
  ) {
    private val defaultRateLimiter = createRateLimiter(config.defaultRateLimit)
    private val rateLimiterByMethod = ConcurrentHashMap<String, RateLimiter>()

    fun getRateLimiter(methodName: String): RateLimiter {
      val methodRateLimit: RateLimitConfig.MethodRateLimit =
        config.perMethodRateLimitMap[methodName] ?: return defaultRateLimiter

      return rateLimiterByMethod.getOrPut(methodName) { createRateLimiter(methodRateLimit) }
    }

    private fun createRateLimiter(methodRateLimit: RateLimitConfig.MethodRateLimit): RateLimiter {
      return when {
        methodRateLimit.maximumRequestCount == 0 -> RateLimiter.Blocked
        methodRateLimit.maximumRequestCount < 0 -> RateLimiter.Unlimited
        else ->
          TokenBucket(
            methodRateLimit.maximumRequestCount,
            methodRateLimit.averageRequestRate,
            timeSource,
          )
      }
    }
  }
}
