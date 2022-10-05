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

package org.wfanet.measurement.api.v2alpha

import com.google.protobuf.ByteString
import io.grpc.BindableService
import io.grpc.Context
import io.grpc.ServerInterceptors
import io.grpc.ServerServiceDefinition
import io.grpc.Status
import org.wfanet.measurement.common.api.PrincipalLookup
import org.wfanet.measurement.common.api.grpc.AkidPrincipalServerInterceptor
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.AuthorityKeyServerInterceptor

/**
 * Returns a [MeasurementPrincipal] in the current gRPC context. Requires
 * [PrincipalServerInterceptor] to be installed.
 *
 * Callers can trust that the [MeasurementPrincipal] is authenticated (but not necessarily
 * authorized).
 */
val principalFromCurrentContext: MeasurementPrincipal
  get() =
    ContextKeys.PRINCIPAL_CONTEXT_KEY.get()
      ?: failGrpc(Status.UNAUTHENTICATED) { "No MeasurementPrincipal found" }

/**
 * Executes [block] with [principal] installed in a new [Context].
 *
 * The caller of [withPrincipal] is responsible for guaranteeing that [block] can act as [principal]
 * -- in other words, [principal] is treated as already authenticated.
 */
fun <T> withPrincipal(principal: MeasurementPrincipal, block: () -> T): T {
  return Context.current().withPrincipal(principal).call(block)
}

/** Executes [block] with a [DataProviderPrincipal] installed in a new [Context]. */
fun <T> withDataProviderPrincipal(dataProviderName: String, block: () -> T): T {
  return Context.current()
    .withPrincipal(DataProviderPrincipal(DataProviderKey.fromName(dataProviderName)!!))
    .call(block)
}

/** Executes [block] with a [ModelProviderPrincipal] installed in a new [Context]. */
fun <T> withModelProviderPrincipal(modelProviderName: String, block: () -> T): T {
  return Context.current()
    .withPrincipal(ModelProviderPrincipal(ModelProviderKey.fromName(modelProviderName)!!))
    .call(block)
}

/** Executes [block] with a [DuchyPrincipal] installed in a new [Context]. */
fun <T> withDuchyPrincipal(duchyName: String, block: () -> T): T {
  return Context.current().withPrincipal(DuchyPrincipal(DuchyKey.fromName(duchyName)!!)).call(block)
}

/** Executes [block] with a [MeasurementConsumerPrincipal] installed in a new [Context]. */
fun <T> withMeasurementConsumerPrincipal(measurementConsumerName: String, block: () -> T): T {
  return Context.current()
    .withPrincipal(
      MeasurementConsumerPrincipal(MeasurementConsumerKey.fromName(measurementConsumerName)!!)
    )
    .call(block)
}

/** Adds [principal] to the receiver and returns the new [Context]. */
fun Context.withPrincipal(principal: MeasurementPrincipal): Context {
  return withValue(ContextKeys.PRINCIPAL_CONTEXT_KEY, principal)
}

/** Convenience helper for [AkidPrincipalServerInterceptor]. */
fun BindableService.withPrincipalsFromX509AuthorityKeyIdentifiers(
  akidPrincipalLookup: PrincipalLookup<MeasurementPrincipal, ByteString>
): ServerServiceDefinition {
  return ServerInterceptors.interceptForward(
    this,
    AuthorityKeyServerInterceptor(),
    AkidPrincipalServerInterceptor(
      ContextKeys.PRINCIPAL_CONTEXT_KEY,
      AuthorityKeyServerInterceptor.AUTHORITY_KEY_IDENTIFIERS_CONTEXT_KEY,
      akidPrincipalLookup
    )
  )
}
