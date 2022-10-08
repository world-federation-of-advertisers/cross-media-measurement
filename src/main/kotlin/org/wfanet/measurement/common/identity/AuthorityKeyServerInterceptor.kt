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

package org.wfanet.measurement.common.identity

import com.google.protobuf.ByteString
import io.grpc.Context
import io.grpc.Contexts
import io.grpc.Grpc
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.Status
import java.security.cert.X509Certificate
import javax.net.ssl.SSLSession
import org.wfanet.measurement.api.v2alpha.ContextKeys
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.grpc.failGrpc

/** Returns an [X509Certificate] installed in the current [io.grpc.Context]. */
val authorityKeyIdentifiersFromCurrentContext: List<ByteString>
  get() =
    AuthorityKeyServerInterceptor.AUTHORITY_KEY_IDENTIFIERS_CONTEXT_KEY.get()
      ?: failGrpc(Status.UNAUTHENTICATED) { "No authority keys available" }

/**
 * gRPC [ServerInterceptor] that extracts Authority Key Identifiers from X509 certificates.
 *
 * The Authority Key Identifiers can be accessed by using
 * [authorityKeyIdentifiersFromCurrentContext].
 */
class AuthorityKeyServerInterceptor : ServerInterceptor {
  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>
  ): ServerCall.Listener<ReqT> {
    if (ContextKeys.PRINCIPAL_CONTEXT_KEY.get() != null) {
      return Contexts.interceptCall(Context.current(), call, headers, next)
    }

    val sslSession: SSLSession? = call.attributes[Grpc.TRANSPORT_ATTR_SSL_SESSION]

    if (sslSession == null) {
      call.close(Status.UNAUTHENTICATED.withDescription("No SSL session found"), Metadata())
      return object : ServerCall.Listener<ReqT>() {}
    }

    val x509Certificates = sslSession.peerCertificates.filterIsInstance<X509Certificate>()
    val authorityKeys: List<ByteString> = x509Certificates.mapNotNull { it.authorityKeyIdentifier }

    if (authorityKeys.size != x509Certificates.size) {
      call.close(
        Status.UNAUTHENTICATED.withDescription(
          "X509 certificate is missing an authority key identifier"
        ),
        Metadata()
      )
      return object : ServerCall.Listener<ReqT>() {}
    }

    val context = Context.current().withValue(AUTHORITY_KEY_IDENTIFIERS_CONTEXT_KEY, authorityKeys)
    return Contexts.interceptCall(context, call, headers, next)
  }

  companion object {
    val AUTHORITY_KEY_IDENTIFIERS_CONTEXT_KEY: Context.Key<List<ByteString>> =
      Context.key("authority-key-identifiers")
  }
}
