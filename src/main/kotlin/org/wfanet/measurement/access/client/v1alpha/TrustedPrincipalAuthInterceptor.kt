/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.access.client.v1alpha

import io.grpc.BindableService
import io.grpc.CallCredentials
import io.grpc.Context
import io.grpc.Contexts
import io.grpc.Metadata
import io.grpc.Metadata.AsciiMarshaller
import io.grpc.Metadata.BinaryMarshaller
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.ServerInterceptors
import io.grpc.ServerServiceDefinition
import io.grpc.Status
import io.grpc.stub.AbstractStub
import java.util.concurrent.Executor
import org.wfanet.measurement.access.v1alpha.Principal

/**
 * gRPC [ServerInterceptor] for [Principal]-based authentication when the server trusts the client
 * to have already authenticated the caller.
 *
 * This sets [ContextKeys] directly from [Credentials].
 */
object TrustedPrincipalAuthInterceptor : ServerInterceptor {
  override fun <ReqT : Any, RespT : Any> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    val credentials = Credentials.fromHeaders(headers)
    if (credentials == null) {
      call.close(
        Status.UNAUTHENTICATED.withDescription("Credentials not found in headers"),
        Metadata(),
      )
      return object : ServerCall.Listener<ReqT>() {}
    }

    val context =
      Context.current().withPrincipalAndScopes(credentials.principal, credentials.scopes)
    return Contexts.interceptCall(context, call, headers, next)
  }

  /** Trusted credentials that specify the [principal] and [scopes] directly. */
  class Credentials(val principal: Principal, val scopes: Set<String>) : CallCredentials() {
    override fun applyRequestMetadata(
      requestInfo: RequestInfo,
      appExecutor: Executor,
      applier: MetadataApplier,
    ) {
      val headers =
        Metadata().apply {
          put(PRINCIPAL_METADATA_KEY, principal)
          put(SCOPES_METADATA_KEY, scopes)
        }
      applier.apply(headers)
    }

    companion object {
      private val PRINCIPAL_METADATA_KEY =
        Metadata.Key.of("x-trusted-principal-bin", PrincipalMarshaller)
      private val SCOPES_METADATA_KEY = Metadata.Key.of("x-trusted-scopes", ScopesMarshaller)

      fun fromHeaders(headers: Metadata): Credentials? {
        val principal = headers[PRINCIPAL_METADATA_KEY] ?: return null
        val scopes = headers.get(SCOPES_METADATA_KEY) ?: return null
        return Credentials(principal, scopes)
      }
    }

    private object PrincipalMarshaller : BinaryMarshaller<Principal> {
      override fun toBytes(value: Principal): ByteArray {
        return value.toByteArray()
      }

      override fun parseBytes(serialized: ByteArray): Principal {
        return Principal.parseFrom(serialized)
      }
    }

    private object ScopesMarshaller : AsciiMarshaller<Set<String>> {
      override fun toAsciiString(value: Set<String>): String {
        return value.joinToString(" ")
      }

      override fun parseAsciiString(serialized: String): Set<String> {
        return serialized.split(" ").toSet()
      }
    }
  }
}

fun BindableService.withTrustedPrincipalAuthentication(): ServerServiceDefinition =
  ServerInterceptors.interceptForward(this, TrustedPrincipalAuthInterceptor)

fun <T : AbstractStub<T>> T.withForwardedTrustedCredentials(): T {
  val principal = checkNotNull(ContextKeys.PRINCIPAL.get()) { "Principal not found in context" }
  val scopes = checkNotNull(ContextKeys.SCOPES.get()) { "Scopes not found in context" }
  return withCallCredentials(TrustedPrincipalAuthInterceptor.Credentials(principal, scopes))
}
