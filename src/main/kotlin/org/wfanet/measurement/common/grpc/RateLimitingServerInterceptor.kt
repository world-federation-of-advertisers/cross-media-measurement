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
import org.wfanet.measurement.common.ratelimit.RateLimiter

/** Server interceptor that applies rate limiting. */
class RateLimitingServerInterceptor(
  /** Function which returns the [RateLimiter] for the specified gRPC context and method. */
  private val getRateLimiter: (context: Context, fullMethodName: String) -> RateLimiter
) : ServerInterceptor {
  override fun <ReqT : Any, RespT : Any> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    val rateLimiter: RateLimiter =
      getRateLimiter(Context.current(), call.methodDescriptor.fullMethodName)
    return if (rateLimiter.tryAcquire()) {
      next.startCall(call, headers)
    } else {
      call.close(RATE_LIMIT_EXCEEDED_STATUS, Metadata())
      object : ServerCall.Listener<ReqT>() {}
    }
  }

  companion object {
    private val RATE_LIMIT_EXCEEDED_STATUS =
      Status.UNAVAILABLE.withDescription("Rate limit exceeded")
  }
}
