/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.service

import com.google.auth.oauth2.TokenVerifier
import io.grpc.Context
import io.grpc.Contexts
import io.grpc.Grpc
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.Status
import java.net.URI
import java.time.Clock
import javax.net.ssl.SSLPeerUnverifiedException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.wfanet.measurement.config.edpaggregator.VidLabelingMachineApiConfig

/** Authorizes Google service-account identities for VID-labeling machine operations. */
class VidLabelingMachineAuthorizer(
  config: VidLabelingMachineApiConfig,
  private val clock: Clock = Clock.systemUTC(),
  private val tokenVerifier: TokenVerifier =
    TokenVerifier.newBuilder()
      .setAudience(config.audience)
      .setIssuer(GOOGLE_ID_TOKEN_ISSUER)
      .setClock { clock.millis() }
      .build(),
) {
  enum class Role {
    DISPATCHER,
    HEALING_CONTROLLER,
  }

  private val subjectsByRole =
    mapOf(
      Role.DISPATCHER to config.dispatcherServiceAccountSubjectsList.toSet(),
      Role.HEALING_CONTROLLER to config.healingControllerServiceAccountSubjectsList.toSet(),
    )

  init {
    val audience = URI(config.audience)
    require(audience.scheme == "https" && !audience.host.isNullOrEmpty()) {
      "VID-labeling machine API audience must be an absolute HTTPS URI"
    }
    require(
      subjectsByRole.values.all { subjects -> subjects.all { it.matches(Regex("[0-9]+")) } }
    ) {
      "VID-labeling machine roles require numeric IAM service-account unique IDs"
    }
    require(
      subjectsByRole
        .getValue(Role.DISPATCHER)
        .intersect(subjectsByRole.getValue(Role.HEALING_CONTROLLER))
        .isEmpty()
    ) {
      "Dispatcher and healing-controller service-account identities must be distinct"
    }
  }

  suspend fun requireRole(role: Role) {
    val token =
      TOKEN_CONTEXT_KEY.get()
        ?: throw Status.UNAUTHENTICATED.withDescription("A Google ID token is required")
          .asRuntimeException()
    val payload =
      try {
        withContext(Dispatchers.IO) { tokenVerifier.verify(token).payload }
      } catch (e: TokenVerifier.VerificationException) {
        if (e.message == PUBLIC_KEY_FETCH_ERROR) {
          throw Status.UNAVAILABLE.withDescription("Google ID-token verification is unavailable")
            .asRuntimeException()
        }
        throw Status.UNAUTHENTICATED.withDescription("Invalid Google ID token").asRuntimeException()
      } catch (e: IllegalArgumentException) {
        throw Status.UNAUTHENTICATED.withDescription("Malformed Google ID token")
          .asRuntimeException()
      }

    val expiration = payload.expirationTimeSeconds
    if (
      expiration == null ||
        expiration <= clock.instant().epochSecond ||
        payload.subject.isNullOrEmpty()
    ) {
      throw Status.UNAUTHENTICATED.withDescription("Google ID token has no active identity")
        .asRuntimeException()
    }
    if (payload.subject !in subjectsByRole.getValue(role)) {
      throw Status.PERMISSION_DENIED.withDescription(
          "Caller is not authorized for this machine operation"
        )
        .asRuntimeException()
    }
  }

  /** Captures an ID token only after the transport authenticated a client certificate. */
  object Interceptor : ServerInterceptor {
    override fun <ReqT, RespT> interceptCall(
      call: ServerCall<ReqT, RespT>,
      headers: Metadata,
      next: ServerCallHandler<ReqT, RespT>,
    ): ServerCall.Listener<ReqT> {
      val session = call.attributes[Grpc.TRANSPORT_ATTR_SSL_SESSION]
      val hasClientCertificate =
        try {
          session != null && session.peerCertificates.isNotEmpty()
        } catch (e: SSLPeerUnverifiedException) {
          false
        }
      if (!hasClientCertificate) {
        call.close(Status.UNAUTHENTICATED.withDescription("Mutual TLS is required"), Metadata())
        return object : ServerCall.Listener<ReqT>() {}
      }
      val authorization = headers[AUTHORIZATION_METADATA_KEY]
      val token = authorization?.takeIf { it.startsWith("Bearer ") }?.removePrefix("Bearer ")
      if (token.isNullOrBlank()) {
        call.close(
          Status.UNAUTHENTICATED.withDescription("A Google ID token is required"),
          Metadata(),
        )
        return object : ServerCall.Listener<ReqT>() {}
      }
      return Contexts.interceptCall(
        Context.current().withValue(TOKEN_CONTEXT_KEY, token),
        call,
        headers,
        next,
      )
    }
  }

  companion object {
    const val GOOGLE_ID_TOKEN_ISSUER = "https://accounts.google.com"
    // TokenVerifier exposes key-source failures through this SDK diagnostic.
    private const val PUBLIC_KEY_FETCH_ERROR = "Error fetching PublicKey from certificate location"
    private val TOKEN_CONTEXT_KEY: Context.Key<String> =
      Context.key("vid-labeling-machine-id-token")
    private val AUTHORIZATION_METADATA_KEY =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)
  }
}
