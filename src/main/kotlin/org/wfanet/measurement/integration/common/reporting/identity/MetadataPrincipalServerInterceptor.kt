// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.common.reporting.identity

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
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig
import org.wfanet.measurement.reporting.service.api.v1alpha.ContextKeys
import org.wfanet.measurement.reporting.service.api.v1alpha.ReportingPrincipal
import org.wfanet.measurement.reporting.service.api.v1alpha.principalFromCurrentContext
import org.wfanet.measurement.reporting.service.api.v1alpha.withPrincipal

/**
 * Extracts a name from the gRPC [Metadata] and creates a [ReportingPrincipal] to add to the gRPC
 * [Context].
 *
 * To install, wrap a service with:
 * ```
 *   yourService.withMetadataPrincipalIdentities()
 * ```
 *
 * The principal can be accessed within gRPC services via [principalFromCurrentContext].
 *
 * This expects the Metadata to have a key "reporting_principal" associated with a value equal to
 * the resource name of the principal. The recommended way to set this is to use [withPrincipalName]
 * on a stub.
 */
class MetadataPrincipalServerInterceptor(private val config: MeasurementConsumerConfig) :
  ServerInterceptor {
  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    if (ContextKeys.PRINCIPAL_CONTEXT_KEY.get() != null) {
      return Contexts.interceptCall(Context.current(), call, headers, next)
    }

    val principalName = headers[REPORTING_PRINCIPAL_NAME_METADATA_KEY]
    if (principalName == null) {
      call.close(
        Status.UNAUTHENTICATED.withDescription("$REPORTING_PRINCIPAL_NAME_METADATA_KEY not found"),
        Metadata(),
      )
      return object : ServerCall.Listener<ReqT>() {}
    }
    val principal = ReportingPrincipal.fromConfigs(principalName, config)
    if (principal == null) {
      call.close(Status.UNAUTHENTICATED.withDescription("No valid Principal found"), Metadata())
      return object : ServerCall.Listener<ReqT>() {}
    }
    val context = Context.current().withPrincipal(principal)
    return Contexts.interceptCall(context, call, headers, next)
  }
}

/** Installs [MetadataPrincipalServerInterceptor] on the service. */
fun BindableService.withMetadataPrincipalIdentities(
  config: MeasurementConsumerConfig
): ServerServiceDefinition =
  ServerInterceptors.interceptForward(this, MetadataPrincipalServerInterceptor(config))
