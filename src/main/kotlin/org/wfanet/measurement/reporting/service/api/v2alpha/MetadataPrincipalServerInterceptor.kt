/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

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
import io.grpc.stub.AbstractStub
import io.grpc.stub.MetadataUtils
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs

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
class MetadataPrincipalServerInterceptor(
  private val measurementConsumerConfigs: MeasurementConsumerConfigs
) : ServerInterceptor {
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

    val config = measurementConsumerConfigs.configsMap[principalName]
    if (config == null) {
      call.close(Status.UNAUTHENTICATED.withDescription("No valid Principal found"), Metadata())
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

  companion object {
    private const val KEY_NAME = "reporting_principal"
    private val REPORTING_PRINCIPAL_NAME_METADATA_KEY: Metadata.Key<String> =
      Metadata.Key.of(KEY_NAME, Metadata.ASCII_STRING_MARSHALLER)

    /**
     * Sets metadata key "reporting_principal" on all outgoing requests. On the server side, use
     * [MetadataPrincipalServerInterceptor].
     */
    fun <T : AbstractStub<T>> T.withPrincipalName(name: String): T {
      val extraHeaders = Metadata()
      extraHeaders.put(REPORTING_PRINCIPAL_NAME_METADATA_KEY, name)
      return withInterceptors(MetadataUtils.newAttachHeadersInterceptor(extraHeaders))
    }

    /** Installs [MetadataPrincipalServerInterceptor] on the service. */
    fun BindableService.withMetadataPrincipalIdentities(
      measurementConsumerConfigs: MeasurementConsumerConfigs
    ): ServerServiceDefinition =
      ServerInterceptors.interceptForward(
        this,
        MetadataPrincipalServerInterceptor(measurementConsumerConfigs),
      )
  }
}
