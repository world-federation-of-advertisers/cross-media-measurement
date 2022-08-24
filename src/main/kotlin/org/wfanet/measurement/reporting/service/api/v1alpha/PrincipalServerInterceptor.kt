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

package org.wfanet.measurement.reporting.service.api.v1alpha

import com.google.protobuf.ByteString
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
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.AuthorityKeyServerInterceptor
import org.wfanet.measurement.common.identity.authorityKeyIdentifiersFromCurrentContext
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig

/**
 * Returns a [ReportingPrincipal] in the current gRPC context. Requires [PrincipalServerInterceptor]
 * to be installed.
 *
 * Callers can trust that the [ReportingPrincipal] is authenticated (but not necessarily
 * authorized).
 */
val principalFromCurrentContext: ReportingPrincipal
  get() =
    ContextKeys.PRINCIPAL_CONTEXT_KEY.get()
      ?: failGrpc(Status.UNAUTHENTICATED) { "No ReportingPrincipal found" }

/**
 * Executes [block] with [principal] installed in a new [Context].
 *
 * The caller of [withPrincipal] is responsible for guaranteeing that [block] can act as [principal]
 * -- in other words, [principal] is treated as already authenticated.
 */
fun <T> withPrincipal(principal: ReportingPrincipal, block: () -> T): T {
  return Context.current().withPrincipal(principal).call(block)
}

/** Executes [block] with a [MeasurementConsumerPrincipal] installed in a new [Context]. */
fun <T> withMeasurementConsumerPrincipal(
  measurementConsumerName: String,
  config: MeasurementConsumerConfig,
  block: () -> T
): T {
  return Context.current()
    .withPrincipal(
      MeasurementConsumerPrincipal(
        MeasurementConsumerKey.fromName(measurementConsumerName)!!,
        config
      )
    )
    .call(block)
}

/** Adds [principal] to the receiver and returns the new [Context]. */
fun Context.withPrincipal(principal: ReportingPrincipal): Context {
  return withValue(ContextKeys.PRINCIPAL_CONTEXT_KEY, principal)
}

/**
 * gRPC [ServerInterceptor] to extract a [ReportingPrincipal] from a request.
 *
 * If the [ReportingPrincipal] has already been set in the context, this does nothing. Otherwise,
 * this derives the [ReportingPrincipal] from an X509 cert's authority key identifier.
 */
class PrincipalServerInterceptor(private val principalLookup: PrincipalLookup) : ServerInterceptor {

  interface PrincipalLookup {
    fun get(authorityKeyIdentifier: ByteString): ReportingPrincipal?
  }

  interface ConfigLookup {
    fun get(measurementConsumerName: String): MeasurementConsumerConfig?
  }

  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>
  ): ServerCall.Listener<ReqT> {
    if (ContextKeys.PRINCIPAL_CONTEXT_KEY.get() != null) {
      return Contexts.interceptCall(Context.current(), call, headers, next)
    }

    val authorityKeyIdentifiers: List<ByteString> = authorityKeyIdentifiersFromCurrentContext
    val principals = authorityKeyIdentifiers.map(principalLookup::get)
    return when (principals.size) {
      0 -> unauthenticatedError(call, "No principal found")
      1 -> {
        val context =
          Context.current().withValue(ContextKeys.PRINCIPAL_CONTEXT_KEY, principals.single())
        Contexts.interceptCall(context, call, headers, next)
      }
      else -> unauthenticatedError(call, "More than one principal found")
    }
  }

  private fun <ReqT> unauthenticatedError(
    call: ServerCall<*, *>,
    message: String
  ): ServerCall.Listener<ReqT> {
    call.close(Status.UNAUTHENTICATED.withDescription(message), Metadata())
    return object : ServerCall.Listener<ReqT>() {}
  }
}

/** Convenience helper for [PrincipalServerInterceptor]. */
fun BindableService.withPrincipalsFromX509AuthorityKeyIdentifiers(
  principalLookup: PrincipalServerInterceptor.PrincipalLookup
): ServerServiceDefinition =
  ServerInterceptors.interceptForward(
    this,
    AuthorityKeyServerInterceptor(),
    PrincipalServerInterceptor(principalLookup)
  )
