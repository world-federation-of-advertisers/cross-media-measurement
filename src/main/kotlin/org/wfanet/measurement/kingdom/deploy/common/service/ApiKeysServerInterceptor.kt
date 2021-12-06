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
import org.wfanet.measurement.api.ApiKeyConstants
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.kingdom.MeasurementConsumer

/** gRPC [ServerInterceptor] to get api authentication key for a [MeasurementConsumer] principal. */
class ApiKeysServerInterceptor : ServerInterceptor {
  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>
  ): ServerCall.Listener<ReqT> {
    val apiAuthKey = headers.get(ApiKeyConstants.API_AUTHENTICATION_KEY_METADATA_KEY)

    var context = Context.current()
    if (apiAuthKey != null) {
      context =
        Context.current()
          .withValue(
            ApiKeyConstants.CONTEXT_API_AUTHENTICATION_KEY_KEY,
            apiIdToExternalId(apiAuthKey)
          )
    }

    return Contexts.interceptCall(context, call, headers, next)
  }
}

fun BindableService.withApiKeysServerInterceptor(): ServerServiceDefinition =
  ServerInterceptors.interceptForward(this, ApiKeysServerInterceptor())

/** Executes [block] with api authentication key installed in a new [Context]. */
fun <T> withApiAuthenticationKey(apiAuthenticationKey: Long, block: () -> T): T {
  return Context.current().withApiAuthenticationKey(apiAuthenticationKey).call(block)
}

fun Context.withApiAuthenticationKey(apiAuthenticationKey: Long): Context {
  return withValue(ApiKeyConstants.CONTEXT_API_AUTHENTICATION_KEY_KEY, apiAuthenticationKey)
}

fun getApiAuthenticationKey(): Long? = ApiKeyConstants.CONTEXT_API_AUTHENTICATION_KEY_KEY.get()
