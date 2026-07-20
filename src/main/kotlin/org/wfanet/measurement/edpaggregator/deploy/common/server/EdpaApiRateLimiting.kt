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

import org.wfanet.measurement.common.grpc.RateLimiterProvider
import org.wfanet.measurement.common.grpc.RateLimitingServerInterceptor
import org.wfanet.measurement.config.RateLimitConfig
import org.wfanet.measurement.config.RateLimitConfigKt
import org.wfanet.measurement.config.rateLimitConfig

/**
 * Server-side rate limiting for the EDP Aggregator metadata API servers.
 *
 * The public (system) and internal metadata services stayed responsive for cheap RPCs but were
 * overwhelmed (OOM, bare `INTERNAL`) by large `List*` calls. This bounds those expensive methods so
 * the servers shed load cleanly (rejecting with `UNAVAILABLE`, which the fulfiller's per-op retry
 * absorbs) instead of falling over.
 *
 * CAVEAT: [org.wfanet.measurement.common.ratelimit.TokenBucket] is a *rate* limiter (requests/sec +
 * burst), not a *concurrency* limiter. For OOM specifically — too many simultaneous large-response
 * serializations — an in-flight cap would be more directly protective; a tightly-tuned token bucket
 * on the `List*` methods is a pragmatic "good enough". Operators can override everything via
 * `--rate-limit-config-file`.
 */
object EdpaApiRateLimiting {
  /**
   * Default burst (max in-flight token count) allowed for an expensive `List*` method before it
   * starts rejecting. Deliberately conservative; tune via `--rate-limit-config-file`.
   */
  const val DEFAULT_EXPENSIVE_METHOD_MAX_REQUEST_COUNT = 20

  /** Default sustained request rate (requests/second) for an expensive `List*` method. */
  const val DEFAULT_EXPENSIVE_METHOD_AVERAGE_REQUEST_RATE = 10.0

  /**
   * gRPC full method names (`<package>.<Service>/<Method>`) of the expensive public v1alpha
   * methods.
   */
  val PUBLIC_API_EXPENSIVE_METHODS: List<String> =
    listOf(
      "wfa.measurement.edpaggregator.v1alpha.ImpressionMetadataService/ListImpressionMetadata",
      "wfa.measurement.edpaggregator.v1alpha.RequisitionMetadataService/ListRequisitionMetadata",
    )

  /** gRPC full method names of the expensive internal methods. */
  val INTERNAL_API_EXPENSIVE_METHODS: List<String> =
    listOf(
      "wfa.measurement.internal.edpaggregator.ImpressionMetadataService/ListImpressionMetadata",
      "wfa.measurement.internal.edpaggregator.RequisitionMetadataService/ListRequisitionMetadata",
    )

  /**
   * Builds a [RateLimitConfig] that leaves every method unlimited except [expensiveMethods], which
   * are each bounded to a [maximumRequestCount] burst refilling at [averageRequestRate] per second.
   */
  fun defaultConfig(
    expensiveMethods: Iterable<String>,
    maximumRequestCount: Int = DEFAULT_EXPENSIVE_METHOD_MAX_REQUEST_COUNT,
    averageRequestRate: Double = DEFAULT_EXPENSIVE_METHOD_AVERAGE_REQUEST_RATE,
  ): RateLimitConfig = rateLimitConfig {
    rateLimit =
      RateLimitConfigKt.rateLimit {
        defaultRateLimit =
          RateLimitConfigKt.methodRateLimit {
            this.maximumRequestCount = -1 // Unlimited: cheap RPCs are not throttled.
            this.averageRequestRate = 1.0
          }
        val boundedRateLimit =
          RateLimitConfigKt.methodRateLimit {
            this.maximumRequestCount = maximumRequestCount
            this.averageRequestRate = averageRequestRate
          }
        for (method in expensiveMethods) {
          perMethodRateLimit[method] = boundedRateLimit
        }
      }
  }

  /**
   * Builds a [RateLimitingServerInterceptor] from [config].
   *
   * The principal extractor returns `null`, so limits are global per method rather than per client,
   * which is what overload protection needs here.
   */
  fun interceptor(config: RateLimitConfig): RateLimitingServerInterceptor =
    RateLimitingServerInterceptor(RateLimiterProvider(config) { null }::getRateLimiter)
}
