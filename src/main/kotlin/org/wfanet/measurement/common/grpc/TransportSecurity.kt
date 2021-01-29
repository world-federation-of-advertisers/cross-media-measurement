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

package org.wfanet.measurement.common.grpc

import io.grpc.netty.GrpcSslContexts
import io.netty.handler.ssl.ClientAuth
import io.netty.handler.ssl.SslContext
import io.netty.handler.ssl.SslContextBuilder
import org.wfanet.measurement.common.crypto.SigningCerts

private const val TLS_V13_PROTOCOL = "TLSv1.3"

/**
 * Converts this [SigningCerts] into an [SslContext] for a gRPC client with
 * client authentication (mTLS).
 */
fun SigningCerts.toClientTlsContext(): SslContext {
  return GrpcSslContexts.forClient()
    .protocols(TLS_V13_PROTOCOL)
    .keyManager(privateKey, certificate)
    .trustManager(trustedCertificates)
    .build()
}

/**
 * Converts this [SigningCerts] into an [SslContext] for a gRPC server with TLS.
 */
fun SigningCerts.toServerTlsContext(clientAuth: ClientAuth = ClientAuth.NONE): SslContext {
  return GrpcSslContexts.configure(SslContextBuilder.forServer(privateKey, certificate))
    .protocols(TLS_V13_PROTOCOL)
    .trustManager(trustedCertificates)
    .clientAuth(clientAuth)
    .build()
}
