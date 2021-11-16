// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.kingdom.deploy.common.service

import io.grpc.BindableService
import io.grpc.Context
import io.grpc.Contexts
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.ServerInterceptors
import io.grpc.ServerServiceDefinition
import org.wfanet.measurement.api.AccountConstants
import org.wfanet.measurement.internal.kingdom.Account

/** gRPC [ServerInterceptor] to get [Account] credentials coming in from a request. */
class AccountsServerInterceptor() : ServerInterceptor {
  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>
  ): ServerCall.Listener<ReqT> {
    val idToken = headers.get(AccountConstants.ID_TOKEN_METADATA_KEY)

    var context = Context.current()
    if (idToken != null) {
      context = Context.current().withValue(AccountConstants.CONTEXT_ID_TOKEN_KEY, idToken)
    }

    return Contexts.interceptCall(context, call, headers, next)
  }
}

fun BindableService.withAccountsServerInterceptor(): ServerServiceDefinition =
  ServerInterceptors.interceptForward(this, AccountsServerInterceptor())

/** Executes [block] with id token installed in a new [Context]. */
fun <T> withIdToken(idToken: String, block: () -> T): T {
  return Context.current().withIdToken(idToken).call(block)
}

fun Context.withIdToken(idToken: String): Context {
  return withValue(AccountConstants.CONTEXT_ID_TOKEN_KEY, idToken)
}

fun getIdToken(): String? = AccountConstants.CONTEXT_ID_TOKEN_KEY.get()
