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
import org.wfanet.measurement.common.grpc.withContext
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
 * Immediately calls [action] with a [Context] that has [authenticatedPrincipal] installed as the
 * [current][Context.current] [Context].
 *
 * The caller is responsible for ensuring that [authenticatedPrincipal] has already been
 * authenticated.
 *
 * @param authenticatedPrincipal the authenticated principal
 * @return the result of [action]
 */
inline fun <R> withPrincipal(authenticatedPrincipal: MeasurementPrincipal, action: () -> R): R {
  return withContext(Context.current().withPrincipal(authenticatedPrincipal), action)
}

/** Executes [block] with a [DataProviderPrincipal] installed in a new [Context]. */
inline fun <T> withDataProviderPrincipal(dataProviderName: String, block: () -> T): T {
  return withPrincipal(
    DataProviderPrincipal(requireNotNull(DataProviderKey.fromName(dataProviderName))),
    block,
  )
}

/** Executes [block] with a [ModelProviderPrincipal] installed in a new [Context]. */
inline fun <T> withModelProviderPrincipal(modelProviderName: String, block: () -> T): T {
  return withPrincipal(
    ModelProviderPrincipal(requireNotNull(ModelProviderKey.fromName(modelProviderName))),
    block,
  )
}

/** Executes [block] with a [DuchyPrincipal] installed in a new [Context]. */
inline fun <T> withDuchyPrincipal(duchyName: String, block: () -> T): T {
  return withPrincipal(DuchyPrincipal(requireNotNull(DuchyKey.fromName(duchyName))), block)
}

/** Executes [block] with a [MeasurementConsumerPrincipal] installed in a new [Context]. */
inline fun <T> withMeasurementConsumerPrincipal(
  measurementConsumerName: String,
  block: () -> T,
): T {
  return withPrincipal(
    MeasurementConsumerPrincipal(
      requireNotNull(MeasurementConsumerKey.fromName(measurementConsumerName))
    ),
    block,
  )
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
      AuthorityKeyServerInterceptor.CLIENT_AUTHORITY_KEY_IDENTIFIER_CONTEXT_KEY,
      akidPrincipalLookup,
    ),
  )
}
