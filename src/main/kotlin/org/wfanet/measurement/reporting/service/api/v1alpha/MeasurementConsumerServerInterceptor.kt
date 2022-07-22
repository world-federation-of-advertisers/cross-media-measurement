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

package org.wfanet.measurement.reporting.service.api.v1alpha

import io.grpc.Context
import io.grpc.Contexts
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.ServerInterceptors
import io.grpc.ServerServiceDefinition
import io.grpc.Status
import org.wfanet.measurement.api.PrincipalConstants
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.grpc.failGrpc

/**
 * Returns an API key in the current gRPC context. Requires [MeasurementConsumerServerInterceptor]
 * to be installed.
 */
val apiKeyFromCurrentContext: String
  get() =
    MeasurementConsumerConstants.API_KEY_CONTEXT_KEY.get()
      ?: failGrpc(Status.UNAUTHENTICATED) { "No API key found" }

/** Executes [block] with API key installed in a new [Context]. */
fun <T> withApiKey(apiKey: String, block: () -> T): T {
  return Context.current().withApiKey(apiKey).call(block)
}

/** Adds API key to the receiver and returns the new [Context]. */
fun Context.withApiKey(apiKey: String): Context {
  return withValue(MeasurementConsumerConstants.API_KEY_CONTEXT_KEY, apiKey)
}

/** gRPC [ServerInterceptor] to get the API key for a MeasurementConsumer */
class MeasurementConsumerServerInterceptor(private val apiKeyLookup: ApiKeyLookup) :
  ServerInterceptor {

  interface ApiKeyLookup {
    fun get(measurementConsumerName: String): String?
  }

  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>
  ): ServerCall.Listener<ReqT> {
    val principal = PrincipalConstants.PRINCIPAL_CONTEXT_KEY.get()
    if (principal == null || principal.resourceKey !is MeasurementConsumerKey) {
      return unauthenticatedError(call, "MeasurementConsumer not found")
    }

    val apiKey = apiKeyLookup.get(principal.resourceKey.toName())
    val context =
      Context.current().withValue(MeasurementConsumerConstants.API_KEY_CONTEXT_KEY, apiKey)
    return Contexts.interceptCall(context, call, headers, next)
  }

  private fun <ReqT> unauthenticatedError(
    call: ServerCall<*, *>,
    message: String
  ): ServerCall.Listener<ReqT> {
    call.close(Status.UNAUTHENTICATED.withDescription(message), Metadata())
    return object : ServerCall.Listener<ReqT>() {}
  }
}

/** Convenience helper for [MeasurementConsumerServerInterceptor]. */
fun ServerServiceDefinition.withMeasurementConsumerServerInterceptor(
  apiKeyLookup: MeasurementConsumerServerInterceptor.ApiKeyLookup
): ServerServiceDefinition =
  ServerInterceptors.interceptForward(this, MeasurementConsumerServerInterceptor(apiKeyLookup))
