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

import io.grpc.Context
import io.grpc.Contexts
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.Status
import io.grpc.StatusException
import java.time.Clock
import org.wfanet.measurement.access.v1alpha.Principal
import org.wfanet.measurement.access.v1alpha.PrincipalKt.oAuthUser
import org.wfanet.measurement.access.v1alpha.PrincipalKt.tlsClient
import org.wfanet.measurement.access.v1alpha.PrincipalsGrpcKt
import org.wfanet.measurement.access.v1alpha.lookupPrincipalRequest
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.grpc.BearerTokenCallCredentials
import org.wfanet.measurement.common.grpc.ClientCertificateAuthentication
import org.wfanet.measurement.common.grpc.OAuthTokenAuthentication
import org.wfanet.measurement.common.grpc.SuspendableServerInterceptor
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.config.access.OpenIdProvidersConfig

/** [io.grpc.ServerInterceptor] for Principal-based authentication. */
class PrincipalAuthInterceptor(
  openIdProvidersConfig: OpenIdProvidersConfig,
  private val principalsStub: PrincipalsGrpcKt.PrincipalsCoroutineStub,
  private val tlsClientSupported: Boolean,
  clock: Clock = Clock.systemUTC(),
) : SuspendableServerInterceptor() {
  private val authentication =
    OAuthTokenAuthentication(
      openIdProvidersConfig.audience,
      openIdProvidersConfig.providerConfigByIssuerMap.map { (issuer, config) ->
        OAuthTokenAuthentication.OpenIdProviderConfig(issuer, config.jwks.toJson())
      },
      clock,
    )

  override suspend fun <ReqT : Any, RespT : Any> interceptCallSuspending(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    val tokenCredentials = BearerTokenCallCredentials.fromHeaders(headers)
    val (lookupRequest, scopes) =
      if (tokenCredentials == null) {
        if (!tlsClientSupported) {
          return closeCall(
            call,
            Status.UNAUTHENTICATED.withDescription("No bearer token found in headers"),
          )
        }

        val tlsClient =
          try {
            buildTlsClientIdentity(call)
          } catch (e: StatusException) {
            return closeCall(call, e.status, e.trailers)
          }

        lookupPrincipalRequest { this.tlsClient = tlsClient } to setOf("*")
      } else {
        val verifiedToken =
          try {
            authentication.verifyAndDecodeBearerToken(tokenCredentials)
          } catch (e: StatusException) {
            return closeCall(call, e.status, e.trailers)
          }

        lookupPrincipalRequest {
          user = oAuthUser {
            issuer = verifiedToken.issuer
            subject = verifiedToken.subject
          }
        } to verifiedToken.scopes
      }

    val principal: Principal =
      try {
        principalsStub.lookupPrincipal(lookupRequest)
      } catch (e: StatusException) {
        val status =
          when (e.status.code) {
            Status.Code.NOT_FOUND -> Status.UNAUTHENTICATED.withDescription("Principal not found")
            else -> Status.INTERNAL
          }.withCause(e)
        return closeCall(call, status)
      }

    return Contexts.interceptCall(
      Context.current().withPrincipalAndScopes(principal, scopes),
      call,
      headers,
      next,
    )
  }

  private fun <ReqT : Any, RespT : Any> closeCall(
    call: ServerCall<ReqT, RespT>,
    status: Status,
    trailers: Metadata? = null,
  ): ServerCall.Listener<ReqT> {
    call.close(status, trailers ?: Metadata())
    return object : ServerCall.Listener<ReqT>() {}
  }

  private fun <ReqT : Any, RespT : Any> buildTlsClientIdentity(
    call: ServerCall<ReqT, RespT>
  ): Principal.TlsClient {
    val clientCertificate = ClientCertificateAuthentication.extractClientCertificate(call)
    val authorityKeyIdentifier =
      clientCertificate.authorityKeyIdentifier
        ?: throw Status.UNAUTHENTICATED.withDescription(
            "Client certificate missing authority key identifier"
          )
          .asException()

    return tlsClient { this.authorityKeyIdentifier = authorityKeyIdentifier }
  }
}
