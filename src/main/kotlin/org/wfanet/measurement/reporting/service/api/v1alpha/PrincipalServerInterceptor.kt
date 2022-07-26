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
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.AuthorityKeyServerInterceptor
import org.wfanet.measurement.common.identity.authorityKeyIdentifiersFromCurrentContext
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig

/**
 * Returns a [Principal] in the current gRPC context. Requires [PrincipalServerInterceptor] to be
 * installed.
 *
 * Callers can trust that the [Principal] is authenticated (but not necessarily authorized).
 */
val principalFromCurrentContext: Principal<*>
  get() =
    PrincipalConstants.PRINCIPAL_CONTEXT_KEY.get()
      ?: failGrpc(Status.UNAUTHENTICATED) { "No Principal found" }

/**
 * Returns an API key in the current gRPC context. Requires [PrincipalServerInterceptor] to be
 * installed.
 */
val apiKeyFromCurrentContext: String
  get() =
    PrincipalConstants.API_KEY_CONTEXT_KEY.get()
      ?: failGrpc(Status.PERMISSION_DENIED) { "No API key found" }

/**
 * Executes [block] with [principal] installed in a new [Context].
 *
 * The caller of [withPrincipal] is responsible for guaranteeing that [block] can act as [Principal]
 * -- in other words, [principal] is treated as already authenticated.
 */
fun <T> withPrincipal(principal: Principal<*>, block: () -> T): T {
  return Context.current().withPrincipal(principal).call(block)
}

/** Executes [block] with a [MeasurementConsumer] [Principal] installed in a new [Context]. */
fun <T> withMeasurementConsumerPrincipal(measurementConsumerName: String, block: () -> T): T {
  return Context.current()
    .withPrincipal(
      Principal.MeasurementConsumer(MeasurementConsumerKey.fromName(measurementConsumerName)!!)
    )
    .call(block)
}

/** Adds [MeasurementConsumer] [Principal] to the receiver and returns the new [Context]. */
fun Context.withMeasurementConsumerPrincipal(measurementConsumerName: String): Context {
  return withPrincipal(
    Principal.MeasurementConsumer(MeasurementConsumerKey.fromName(measurementConsumerName)!!)
  )
}

/** Adds API key to the receiver and returns the new [Context]. */
fun Context.withApiKey(apiKey: String): Context {
  return withValue(PrincipalConstants.API_KEY_CONTEXT_KEY, apiKey)
}

/** Adds [principal] to the receiver and returns the new [Context]. */
fun Context.withPrincipal(principal: Principal<*>): Context {
  return withValue(PrincipalConstants.PRINCIPAL_CONTEXT_KEY, principal)
}

/**
 * gRPC [ServerInterceptor] to extract a [Principal] from a request.
 *
 * If the [Principal] has already been set in the context, this does nothing. Otherwise, this
 * derives the [Principal] from an X509 cert's authority key identifier.
 */
class PrincipalServerInterceptor(
  private val principalLookup: PrincipalLookup,
  private val configLookup: ConfigLookup
) : ServerInterceptor {

  interface PrincipalLookup {
    fun get(authorityKeyIdentifier: ByteString): Principal<*>?
  }

  interface ConfigLookup {
    fun get(measurementConsumerName: String): MeasurementConsumerConfig?
  }

  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>
  ): ServerCall.Listener<ReqT> {
    if (PrincipalConstants.PRINCIPAL_CONTEXT_KEY.get() != null) {
      return Contexts.interceptCall(Context.current(), call, headers, next)
    }

    val authorityKeyIdentifiers: List<ByteString> = authorityKeyIdentifiersFromCurrentContext
    val principals = authorityKeyIdentifiers.map(principalLookup::get)
    return when (principals.size) {
      0 -> error(call, "No principal found", Status.UNAUTHENTICATED)
      1 -> {
        val config =
          principals.single()?.resourceKey?.let { configLookup.get(it.toName()) }
            ?: return error(
              call,
              "Config not found for MeasurementConsumer",
              Status.PERMISSION_DENIED
            )

        val context =
          Context.current()
            .withValues(
              PrincipalConstants.PRINCIPAL_CONTEXT_KEY,
              principals.single(),
              PrincipalConstants.API_KEY_CONTEXT_KEY,
              config.apiKey
            )
        Contexts.interceptCall(context, call, headers, next)
      }
      else -> error(call, "More than one principal found", Status.UNAUTHENTICATED)
    }
  }

  private fun <ReqT> error(
    call: ServerCall<*, *>,
    message: String,
    status: Status,
  ): ServerCall.Listener<ReqT> {
    call.close(status.withDescription(message), Metadata())
    return object : ServerCall.Listener<ReqT>() {}
  }
}

/** Convenience helper for [PrincipalServerInterceptor]. */
fun BindableService.withPrincipalsFromX509AuthorityKeyIdentifiers(
  principalLookup: PrincipalServerInterceptor.PrincipalLookup,
  configLookup: PrincipalServerInterceptor.ConfigLookup
): ServerServiceDefinition =
  ServerInterceptors.interceptForward(
    this,
    AuthorityKeyServerInterceptor(),
    PrincipalServerInterceptor(principalLookup, configLookup)
  )
