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

package org.wfanet.measurement.api.v2alpha.testing

import io.grpc.BindableService
import io.grpc.Context
import io.grpc.Contexts
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.ServerInterceptors
import io.grpc.ServerServiceDefinition
import io.grpc.Status
import org.wfanet.measurement.api.v2alpha.ContextKeys
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.api.v2alpha.withPrincipal
import org.wfanet.measurement.common.identity.TrustedPrincipalCallCredentials

/**
 * Extracts a [MeasurementPrincipal] from the gRPC [headers][Metadata] and adds it to the gRPC
 * [Context].
 *
 * This is only for testing with in-process transport.
 *
 * To install, wrap a service with:
 * ```
 *   yourService.withMetadataPrincipalIdentities()
 * ```
 *
 * The principal can be accessed within gRPC services via [principalFromCurrentContext].
 *
 * See [TrustedPrincipalCallCredentials].
 */
class MetadataPrincipalServerInterceptor : ServerInterceptor {
  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    if (ContextKeys.PRINCIPAL_CONTEXT_KEY.get() != null) {
      return next.startCall(call, headers)
    }

    val credentials = TrustedPrincipalCallCredentials.fromHeaders(headers)
    if (credentials == null) {
      call.close(
        Status.UNAUTHENTICATED.withDescription("Credentials not found in headers"),
        Metadata(),
      )
      return object : ServerCall.Listener<ReqT>() {}
    }
    val principal = MeasurementPrincipal.fromName(credentials.name)
    if (principal == null) {
      call.close(Status.UNAUTHENTICATED.withDescription("No valid Principal found"), Metadata())
      return object : ServerCall.Listener<ReqT>() {}
    }
    val context = Context.current().withPrincipal(principal)
    return Contexts.interceptCall(context, call, headers, next)
  }
}

/** Installs [MetadataPrincipalServerInterceptor] on the service. */
fun BindableService.withMetadataPrincipalIdentities(): ServerServiceDefinition =
  ServerInterceptors.interceptForward(this, MetadataPrincipalServerInterceptor())
