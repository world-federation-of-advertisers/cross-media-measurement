/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.common.api.grpc

import com.google.protobuf.ByteString
import io.grpc.Context
import io.grpc.Contexts
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.api.v2alpha.AkidPrincipalLookup
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.common.api.Principal
import org.wfanet.measurement.common.api.PrincipalLookup
import org.wfanet.measurement.common.grpc.SuspendableServerInterceptor

/**
 * [ServerInterceptor] which maps the authority key identifiers (AKIDs) from [akidsContextKey] to a
 * single [Principal].
 *
 * @param principalContextKey [Context.Key] for the [Principal]. If the current RPC context already
 *   has a value for this key, then this interceptor does nothing.
 * @param akidsContextKey [Context.Key] containing AKIDs
 * @param akidPrincipalLookup [PrincipalLookup] where the lookup key is an AKID
 */
class AkidPrincipalServerInterceptor<T : Principal>(
  private val principalContextKey: Context.Key<T>,
  private val akidsContextKey: Context.Key<List<ByteString>>,
  private val akidPrincipalLookup: PrincipalLookup<T, ByteString>,
  coroutineContext: CoroutineContext = EmptyCoroutineContext
) : SuspendableServerInterceptor(coroutineContext) {
  override suspend fun <ReqT : Any, RespT : Any> interceptCallSuspending(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>
  ): ServerCall.Listener<ReqT> {
    var rpcContext = Context.current()
    if (principalContextKey.get() != null) {
      return Contexts.interceptCall(rpcContext, call, headers, next)
    }

    val principal =
      akidsContextKey
        .get(rpcContext)
        .asFlow()
        .map { akidPrincipalLookup.getPrincipal(it) }
        .filterNotNull()
        .singleOrNull()
    if (principal == null) {
      val akidMap = if (akidPrincipalLookup is AkidPrincipalLookup) {
        akidPrincipalLookup.config
      } else {
        null
      }
      val message = "No single principal found: ${akidsContextKey.get(rpcContext).joinToString(",")} with $akidMap."
      call.close(Status.UNAUTHENTICATED.withDescription(message), headers)
    } else {
      rpcContext = rpcContext.withValue(principalContextKey, principal)
    }

    return Contexts.interceptCall(rpcContext, call, headers, next)
  }

}
