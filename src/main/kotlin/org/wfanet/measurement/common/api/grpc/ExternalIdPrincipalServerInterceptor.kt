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

package org.wfanet.measurement.common.api.grpc

import io.grpc.Context
import io.grpc.Contexts
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.api.Principal
import org.wfanet.measurement.common.api.PrincipalLookup
import org.wfanet.measurement.common.grpc.SuspendableServerInterceptor

class ExternalIdPrincipalServerInterceptor<T : Principal>(
  private val principalContextKey: Context.Key<T>,
  private val externalId: Long,
  private val externalIdPrincipalLookup: PrincipalLookup<T, Long>,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : SuspendableServerInterceptor(coroutineContext) {
  override suspend fun <ReqT : Any, RespT : Any> interceptCallSuspending(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    var rpcContext = Context.current()
    if (principalContextKey.get() != null) {
      return Contexts.interceptCall(rpcContext, call, headers, next)
    }

    val principal: T? = externalIdPrincipalLookup.getPrincipal(externalId)
    if (principal == null) {
      call.close(Status.UNAUTHENTICATED.withDescription("No single principal found"), headers)
    } else {
      rpcContext = rpcContext.withValue(principalContextKey, principal)
    }

    return Contexts.interceptCall(rpcContext, call, headers, next)
  }
}
