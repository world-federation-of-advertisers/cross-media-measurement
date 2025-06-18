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
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.Status
import java.util.concurrent.ConcurrentHashMap
import org.wfanet.measurement.common.ratelimit.RateLimiter
import org.wfanet.measurement.common.ratelimit.TokenBucket
import org.wfanet.measurement.config.RateLimitConfig

/**
 * Server interceptor that applies a per-principal, per-method rate limit.
 *
 * The principal can be any value from the current gRPC [Context] which has a unique string
 * identifier.
 */
class PrincipalRateLimitingServerInterceptor(
  /** Map of full method name to cost for that method. */
  private val methodCost: Map<String, Int>,
  /**
   * Function which returns the principal identifier from the specified gRPC [Context], or `null` if
   * the principal was not found.
   */
  private val getPrincipalIdentifier: (context: Context) -> String?,
  /** Function which returns a new [RateLimiter] for the specified principal identifier. */
  private val createRateLimiter: (principalIdentifier: String?) -> RateLimiter,
) : ServerInterceptor {
  private val rateLimiterByPrincipal = ConcurrentHashMap<String, RateLimiter>()

  override fun <ReqT : Any, RespT : Any> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    val principalIdentifier = getPrincipalIdentifier(Context.current())
    val rateLimiter: RateLimiter =
      if (principalIdentifier == null) {
        createRateLimiter(null)
      } else {
        rateLimiterByPrincipal.getOrPut(principalIdentifier) {
          createRateLimiter(principalIdentifier)
        }
      }
    val cost = methodCost.getOrDefault(call.methodDescriptor.fullMethodName, DEFAULT_METHOD_COST)

    return if (rateLimiter.tryAcquire(cost)) {
      next.startCall(call, headers)
    } else {
      call.close(RATE_LIMIT_EXCEEDED_STATUS, Metadata())
      object : ServerCall.Listener<ReqT>() {}
    }
  }

  companion object {
    private const val DEFAULT_METHOD_COST = 1
    private val RATE_LIMIT_EXCEEDED_STATUS =
      Status.UNAVAILABLE.withDescription("Rate limit exceeded")

    fun fromConfig(
      config: RateLimitConfig,
      getPrincipalIdentifier: (context: Context) -> String?,
    ): PrincipalRateLimitingServerInterceptor {
      return PrincipalRateLimitingServerInterceptor(config.methodCostMap, getPrincipalIdentifier) {
        principalIdentifier ->
        val rateLimit =
          if (principalIdentifier == null) {
            config.defaultRateLimit
          } else {
            config.perPrincipalRateLimitMap.getOrDefault(
              principalIdentifier,
              config.defaultRateLimit,
            )
          }
        when {
          rateLimit.maximumRequestCount == 0 -> RateLimiter.Blocked
          rateLimit.maximumRequestCount < 0 -> RateLimiter.Unlimited
          else -> TokenBucket(rateLimit.maximumRequestCount, rateLimit.averageRequestRate)
        }
      }
    }
  }
}
