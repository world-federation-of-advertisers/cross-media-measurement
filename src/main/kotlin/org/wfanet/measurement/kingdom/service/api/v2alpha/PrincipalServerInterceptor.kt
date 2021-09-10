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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.protobuf.ByteString
import io.grpc.Context
import io.grpc.Contexts
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.Status
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ResourceKey
import org.wfanet.measurement.common.identity.authorityKeyIdentifiersFromCurrentContext

/** Identifies the sender of an inbound gRPC request. */
sealed interface Principal<T : ResourceKey> {
  val resourceKey: T

  class DataProvider(override val resourceKey: DataProviderKey) : Principal<DataProviderKey>
  class ModelProvider(override val resourceKey: ModelProviderKey) : Principal<ModelProviderKey>
}

/**
 * Returns a [Principal] in the current gRPC context. Requires [PrincipalServerInterceptor] to be
 * installed.
 */
val principalFromCurrentContext: Principal<*>
  get() = requireNotNull(PRINCIPAL_CONTEXT_KEY.get()) { "No Principal found" }

private val PRINCIPAL_CONTEXT_KEY: Context.Key<Principal<*>> =
  Context.key("principal-from-x509-certificate")

/**
 * gRPC [ServerInterceptor] to extract a [Principal] from a request.
 *
 * Currently, this only supports deriving the [Principal] from an X509 cert's authority key
 * identifier.
 *
 * TODO(@sanjayvas): consider supporting other Principals derived from more request metadata.
 */
class PrincipalServerInterceptor(private val principalLookup: PrincipalLookup) : ServerInterceptor {

  interface PrincipalLookup {
    fun get(authorityKeyIdentifier: ByteString): Principal<*>?
  }

  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>
  ): ServerCall.Listener<ReqT> {
    val authorityKeyIdentifiers: List<ByteString> = authorityKeyIdentifiersFromCurrentContext
    val principals = authorityKeyIdentifiers.map(principalLookup::get)
    return when (principals.size) {
      0 -> unauthenticatedError(call, "No principal found")
      1 -> {
        val context = Context.current().withValue(PRINCIPAL_CONTEXT_KEY, principals.single())
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
