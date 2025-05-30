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
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.api.Principal
import org.wfanet.measurement.common.api.PrincipalLookup
import org.wfanet.measurement.common.grpc.SuspendableServerInterceptor

/**
 * [ServerInterceptor] which maps the authority key identifier (AKID) from [akidContextKey] to a
 * [Principal].
 *
 * @param principalContextKey [Context.Key] for the [Principal]. If the current RPC context already
 *   has a value for this key, then this interceptor does nothing.
 * @param akidContextKey [Context.Key] containing AKID from client certificate
 * @param akidPrincipalLookup [PrincipalLookup] where the lookup key is an AKID
 */
class AkidPrincipalServerInterceptor<T : Principal>(
  private val principalContextKey: Context.Key<T>,
  private val akidContextKey: Context.Key<ByteString>,
  private val akidPrincipalLookup: PrincipalLookup<T, ByteString>,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : SuspendableServerInterceptor(coroutineContext) {
  override suspend fun <ReqT : Any, RespT : Any> interceptCallSuspending(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    if (principalContextKey.get() != null) {
      return next.startCall(call, headers)
    }
    val clientAkid: ByteString = akidContextKey.get() ?: return next.startCall(call, headers)
    val principal: T =
      akidPrincipalLookup.getPrincipal(clientAkid) ?: return next.startCall(call, headers)

    return Contexts.interceptCall(
      Context.current().withValue(principalContextKey, principal),
      call,
      headers,
      next,
    )
  }
}
