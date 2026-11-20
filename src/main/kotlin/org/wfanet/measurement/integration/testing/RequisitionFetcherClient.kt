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
import com.google.api.client.http.HttpExecuteInterceptor
import com.google.api.client.http.HttpResponseException
import com.google.api.client.http.HttpTransport
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.http.HttpTransportFactory
import com.google.auth.oauth2.GoogleCredentials
import com.google.auth.oauth2.IdTokenCredentials
import com.google.auth.oauth2.IdTokenProvider
import com.google.auth.oauth2.IdentityPoolCredentials
import com.google.auth.oauth2.ImpersonatedCredentials
import java.net.URI
import kotlin.coroutines.CoroutineContext
import kotlin.random.Random
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withTimeout

/** Triggers RequisitionFetcher using refreshable, audience-scoped Google ID tokens. */
class RequisitionFetcherClient(
  private val endpoint: URI,
  credentials: GoogleCredentials,
  transport: HttpTransport,
  private val blockingContext: CoroutineContext = Dispatchers.IO,
  private val timeSource: TimeSource = TimeSource.Monotonic,
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
    withTimeout(RETRY_BUDGET) {
      val started = timeSource.markNow()
      repeat(MAX_RETRIES + 1) { attempt ->
        val coroutineContext = currentCoroutineContext()
        try {
          runInterruptible(blockingContext) {
            val request = requestFactory.buildGetRequest(GenericUrl(endpoint))
            val authentication = request.interceptor
            request.interceptor = HttpExecuteInterceptor { authenticatedRequest ->
              authentication?.intercept(authenticatedRequest)
              coroutineContext.ensureActive()
              val remainingMillis = (RETRY_BUDGET - started.elapsedNow()).inWholeMilliseconds
              check(remainingMillis > 1) { "RequisitionFetcher retry budget exhausted" }
              // Connect and read can each consume their timeout, so share the remaining budget.
              val connectMillis = minOf(REQUEST_TIMEOUT.inWholeMilliseconds, remainingMillis / 3)
              authenticatedRequest.connectTimeout = connectMillis.coerceAtLeast(1).toInt()
              authenticatedRequest.readTimeout =
                minOf(REQUEST_TIMEOUT.inWholeMilliseconds, remainingMillis - connectMillis).toInt()
            }
            request.numberOfRetries = 0
            request.unsuccessfulResponseHandler = null
            request.throwExceptionOnExecuteError = false
            val response = request.execute()
            try {
              if (response.statusCode != 200) throw HttpResponseException(response)
            } finally {
              response.disconnect()
            }
          }
          return@withTimeout
        } catch (e: HttpResponseException) {
          if (e.statusCode !in RETRYABLE_STATUS_CODES || attempt == MAX_RETRIES) throw e
          val pause = RETRY_DELAYS[attempt] * Random.nextDouble(0.8, 1.2)
          if (pause >= RETRY_BUDGET - started.elapsedNow()) throw e
          delay(pause)
        }
      }
    }
  }

  companion object {
    private val REQUEST_TIMEOUT = 120.seconds
    private val RETRY_BUDGET = 3.minutes
    private const val MAX_RETRIES = 3
    private val RETRY_DELAYS = listOf(5.seconds, 10.seconds, 20.seconds)
    private val RETRYABLE_STATUS_CODES = setOf(429, 500, 502, 503, 504)
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
