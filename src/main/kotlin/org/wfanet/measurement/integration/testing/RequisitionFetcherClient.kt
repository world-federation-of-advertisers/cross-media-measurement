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

package org.wfanet.measurement.integration.testing

import com.google.api.client.http.GenericUrl
import com.google.api.client.http.HttpTransport
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.http.HttpTransportFactory
import com.google.auth.oauth2.GoogleCredentials
import com.google.auth.oauth2.IdTokenCredentials
import com.google.auth.oauth2.IdTokenProvider
import com.google.auth.oauth2.IdentityPoolCredentials
import com.google.auth.oauth2.ImpersonatedCredentials
import java.net.URI
import java.time.Duration
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/** Triggers RequisitionFetcher using refreshable, audience-scoped Google ID tokens. */
class RequisitionFetcherClient(
  private val endpoint: URI,
  credentials: GoogleCredentials,
  transport: HttpTransport,
) {
  private val idTokenCredentials =
    IdTokenCredentials.newBuilder()
      .setIdTokenProvider(idTokenProvider(credentials, transport))
      .setTargetAudience(endpoint.toString())
      .setOptions(listOf(IdTokenProvider.Option.INCLUDE_EMAIL))
      .build()

  private val requestFactory =
    transport.createRequestFactory(HttpCredentialsAdapter(idTokenCredentials))

  suspend fun trigger() {
    withContext(Dispatchers.IO) {
      val request = requestFactory.buildGetRequest(GenericUrl(endpoint))
      request.connectTimeout = REQUEST_TIMEOUT.toMillis().toInt()
      request.readTimeout = REQUEST_TIMEOUT.toMillis().toInt()
      request.numberOfRetries = 0
      request.unsuccessfulResponseHandler = null
      val response = request.execute()
      try {
        check(response.statusCode == 200) {
          "RequisitionFetcher returned HTTP " + response.statusCode
        }
      } finally {
        response.disconnect()
      }
    }
  }

  companion object {
    private val REQUEST_TIMEOUT: Duration = Duration.ofSeconds(120)
    private val SCOPES = listOf("https://www.googleapis.com/auth/cloud-platform")

    private fun idTokenProvider(
      credentials: GoogleCredentials,
      transport: HttpTransport,
    ): IdTokenProvider {
      if (credentials is IdTokenProvider) return credentials

      require(credentials is IdentityPoolCredentials) {
        "RequisitionFetcher requires service-account, compute, impersonated, or identity-pool ADC"
      }
      val serviceAccount =
        requireNotNull(credentials.serviceAccountEmail) {
          "Identity-pool ADC must configure service-account impersonation"
        }
      // Use the federated identity directly, avoiding a service account impersonating itself.
      val sourceCredentials =
        IdentityPoolCredentials.newBuilder(credentials)
          .apply { setAccessToken(null) }
          .setServiceAccountImpersonationUrl(null)
          .setScopes(SCOPES)
          .build()
      return ImpersonatedCredentials.newBuilder()
        .setSourceCredentials(sourceCredentials)
        .setTargetPrincipal(serviceAccount)
        .setScopes(SCOPES)
        .setHttpTransportFactory(HttpTransportFactory { transport })
        .build()
    }
  }
}
