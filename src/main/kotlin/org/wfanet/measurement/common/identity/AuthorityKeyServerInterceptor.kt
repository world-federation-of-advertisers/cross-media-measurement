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
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier

/**
 * gRPC [ServerInterceptor] that extracts the Authority Key Identifier (AKID) from the client
 * certificate into [CLIENT_AUTHORITY_KEY_IDENTIFIER_CONTEXT_KEY].
 */
class AuthorityKeyServerInterceptor : ServerInterceptor {
  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    val sslSession: SSLSession =
      call.attributes[Grpc.TRANSPORT_ATTR_SSL_SESSION]
        ?: return closeCall(
          call,
          Status.PERMISSION_DENIED.withDescription("Connection is not secure"),
        )
    val clientCertificate =
      sslSession.peerCertificates.singleOrNull() as? X509Certificate
        ?: return next.startCall(call, headers)
    val authorityKeyIdentifier =
      clientCertificate.authorityKeyIdentifier ?: return next.startCall(call, headers)

    val context =
      Context.current()
        .withValue(CLIENT_AUTHORITY_KEY_IDENTIFIER_CONTEXT_KEY, authorityKeyIdentifier)
    return Contexts.interceptCall(context, call, headers, next)
  }

  private fun <ReqT, RespT> closeCall(
    call: ServerCall<ReqT, RespT>,
    status: Status,
    trailers: Metadata = Metadata(),
  ): ServerCall.Listener<ReqT> {
    call.close(status, trailers)
    return object : ServerCall.Listener<ReqT>() {}
  }

  companion object {
    /** Context key for authority key identifier (AKID) of client certificate. */
    val CLIENT_AUTHORITY_KEY_IDENTIFIER_CONTEXT_KEY: Context.Key<ByteString> =
      Context.key("client-authority-key-identifier")
  }
}
