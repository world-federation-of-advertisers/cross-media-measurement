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

import com.google.api.client.http.HttpResponseException
import com.google.api.client.http.HttpTransport
import com.google.api.client.http.LowLevelHttpRequest
import com.google.api.client.http.LowLevelHttpResponse
import com.google.api.client.testing.http.MockLowLevelHttpRequest
import com.google.api.client.testing.http.MockLowLevelHttpResponse
import com.google.auth.http.HttpTransportFactory
import com.google.auth.oauth2.GoogleCredentials
import com.google.auth.oauth2.IdToken
import com.google.auth.oauth2.IdTokenProvider
import com.google.common.truth.Truth.assertThat
import java.io.IOException
import java.net.URI
import java.util.Base64
import kotlin.test.assertFailsWith
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Test

@OptIn(ExperimentalCoroutinesApi::class)
class RequisitionFetcherClientTest {
  @Test
  fun `trigger refreshes an expired token before the next request`() = runTest {
    val expiredToken = token(1)
    val currentToken = token(4102444800)
    val credentials =
      TokenCredentials(listOf(Result.success(expiredToken), Result.success(currentToken)))
    val transport = RecordingTransport(200)
    val client = client(credentials, transport)

    client.trigger()
    client.trigger()

    assertThat(transport.requests.map { it.authorization })
      .containsExactly("Bearer $expiredToken", "Bearer $currentToken")
      .inOrder()
    assertThat(credentials.audiences).containsExactly(ENDPOINT.toString(), ENDPOINT.toString())
    assertThat(credentials.options)
      .containsExactly(
        listOf(IdTokenProvider.Option.INCLUDE_EMAIL),
        listOf(IdTokenProvider.Option.INCLUDE_EMAIL),
      )
  }

  @Test
  fun `trigger uses the federated identity to mint a service account ID token`() = runTest {
    val transport = RecordingTransport(200)
    val credentials = identityPoolCredentials(transport)

    client(credentials, transport).trigger()

    assertThat(transport.requests.map { it.url })
      .containsExactly(
        "https://tokens.example.test/subject",
        "https://sts.googleapis.com/v1/token",
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test@example.test:generateIdToken",
        ENDPOINT.toString(),
      )
      .inOrder()
    val mintRequest = transport.requests.single { it.url.endsWith(":generateIdToken") }
    assertThat(mintRequest.authorization).isEqualTo("Bearer federated-token")
    assertThat(mintRequest.content).contains(ENDPOINT.toString())
    assertThat(mintRequest.content).contains("\"includeEmail\":true")
    assertThat(transport.requests.last().authorization).isEqualTo("Bearer " + token(4102444800))
  }

  @Test
  fun `trigger exchanges a federated token when credentials already contain an impersonated token`() =
    runTest {
      val transport = RecordingTransport(200)
      val credentials = identityPoolCredentials(transport)
      credentials.refresh()
      assertThat(credentials.accessToken.tokenValue).isEqualTo("impersonated-token")
      transport.requests.clear()

      client(credentials, transport).trigger()

      assertThat(transport.requests.map { it.url })
        .containsExactly(
          "https://tokens.example.test/subject",
          "https://sts.googleapis.com/v1/token",
          "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test@example.test:generateIdToken",
          ENDPOINT.toString(),
        )
        .inOrder()
      val mintRequest = transport.requests.single { it.url.endsWith(":generateIdToken") }
      assertThat(mintRequest.authorization).isEqualTo("Bearer federated-token")
      assertThat(credentials.accessToken.tokenValue).isEqualTo("impersonated-token")
    }

  @Test
  fun `trigger fails promptly when the function rejects authentication`() = runTest {
    val credentials = TokenCredentials(listOf(Result.success(token(4102444800))))
    val transport = RecordingTransport(401)

    val failure =
      assertFailsWith<HttpResponseException> { client(credentials, transport).trigger() }

    assertThat(failure.statusCode).isEqualTo(401)
    assertThat(transport.requests).hasSize(1)
  }

  @Test
  fun `trigger does not send an expired token when renewal fails`() = runTest {
    val credentials =
      TokenCredentials(
        listOf(Result.success(token(1)), Result.failure(IOException("renewal failed")))
      )
    val transport = RecordingTransport(200)
    val client = client(credentials, transport)
    client.trigger()

    assertFailsWith<IOException> { client.trigger() }

    assertThat(transport.requests).hasSize(1)
  }

  @Test
  fun `client rejects credentials that cannot issue ID tokens`() {
    val transport = RecordingTransport(200)
    assertFailsWith<IllegalArgumentException> {
      RequisitionFetcherClient(ENDPOINT, GoogleCredentials.newBuilder().build(), transport)
    }
    assertThat(transport.requests).isEmpty()
  }

  private fun TestScope.client(
    credentials: GoogleCredentials,
    transport: RecordingTransport,
  ): RequisitionFetcherClient {
    transport.nowMillis = { testScheduler.currentTime }
    return RequisitionFetcherClient(
      ENDPOINT,
      credentials,
      transport,
      blockingContext = Dispatchers.Unconfined,
      timeSource = testScheduler.timeSource,
    )
  }

  @Test
  fun `trigger retries transient statuses with bounded jittered backoff`() = runTest {
    for (status in listOf(429, 500, 502, 503, 504)) {
      val transport = RecordingTransport(status, status, status, 200)
      client(TokenCredentials(listOf(Result.success(token(4102444800)))), transport).trigger()
      val times = transport.requests.map { it.timeMillis }
      assertThat(times).hasSize(4)
      for ((index, range) in listOf(4000L..6000L, 8000L..12000L, 16000L..24000L).withIndex()) {
        assertThat(times[index + 1] - times[index]).isAtLeast(range.first)
        assertThat(times[index + 1] - times[index]).isAtMost(range.last)
      }
      assertThat(transport.disconnectedFunctionResponses).isEqualTo(4)
      assertThat(transport.requests.first().readTimeout).isEqualTo(120_000)
      assertThat(transport.requests.all { it.connectTimeout + it.readTimeout <= 180_000 }).isTrue()
    }
  }

  @Test
  fun `trigger preserves the final HTTP failure after three retries`() = runTest {
    val transport = RecordingTransport(500)
    val failure =
      assertFailsWith<HttpResponseException> {
        client(TokenCredentials(listOf(Result.success(token(4102444800)))), transport).trigger()
      }
    assertThat(failure.statusCode).isEqualTo(500)
    assertThat(failure.content).isEqualTo("{}")
    assertThat(transport.requests).hasSize(4)
    assertThat(transport.disconnectedFunctionResponses).isEqualTo(4)
  }

  @Test
  fun `trigger rejects permanent failures without retries`() = runTest {
    for (status in listOf(400, 401, 403, 404, 409)) {
      val transport = RecordingTransport(status)
      val failure =
        assertFailsWith<HttpResponseException> {
          client(TokenCredentials(listOf(Result.success(token(4102444800)))), transport).trigger()
        }
      assertThat(failure.statusCode).isEqualTo(status)
      assertThat(transport.requests).hasSize(1)
      assertThat(transport.disconnectedFunctionResponses).isEqualTo(1)
    }
  }

  @Test
  fun `trigger stops when the remaining budget cannot cover another retry`() = runTest {
    val transport = RecordingTransport(500)
    transport.onFunctionRequest = { testScheduler.advanceTimeBy(179_000L) }
    assertFailsWith<HttpResponseException> {
      client(TokenCredentials(listOf(Result.success(token(4102444800)))), transport).trigger()
    }
    assertThat(transport.requests).hasSize(1)
    assertThat(testScheduler.currentTime).isEqualTo(179_000L)
  }

  @Test
  fun `trigger cancels at the three minute overall deadline`() = runTest {
    val transport = RecordingTransport(200)
    transport.onFunctionRequest = { testScheduler.advanceTimeBy(181_000L) }
    assertFailsWith<TimeoutCancellationException> {
      client(TokenCredentials(listOf(Result.success(token(4102444800)))), transport).trigger()
    }
    assertThat(transport.requests).hasSize(1)
    assertThat(transport.disconnectedFunctionResponses).isEqualTo(1)
  }

  @Test
  fun `cancelling a retry wait prevents further requests`() = runTest {
    val transport = RecordingTransport(500)
    val client = client(TokenCredentials(listOf(Result.success(token(4102444800)))), transport)
    val task = launch { client.trigger() }
    runCurrent()
    task.cancelAndJoin()
    assertThat(transport.requests).hasSize(1)
  }

  private fun identityPoolCredentials(transport: HttpTransport): GoogleCredentials =
    GoogleCredentials.fromStream(
      """
        {
          "type": "external_account",
          "audience": "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/test/providers/test",
          "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
          "token_url": "https://sts.googleapis.com/v1/token",
          "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test@example.test:generateAccessToken",
          "credential_source": {
            "url": "https://tokens.example.test/subject",
            "format": {"type": "json", "subject_token_field_name": "token"}
          }
        }
        """
        .trimIndent()
        .byteInputStream(),
      HttpTransportFactory { transport },
    )

  private class TokenCredentials(private val tokens: List<Result<String>>) :
    GoogleCredentials(), IdTokenProvider {
    val audiences = mutableListOf<String>()
    val options = mutableListOf<List<IdTokenProvider.Option>>()

    override fun idTokenWithAudience(
      targetAudience: String,
      options: MutableList<IdTokenProvider.Option>?,
    ): IdToken {
      audiences += targetAudience
      this.options += options.orEmpty()
      return IdToken.create(tokens[audiences.lastIndex].getOrThrow())
    }
  }

  private data class RecordedRequest(
    val url: String,
    val authorization: String?,
    val content: String,
    val connectTimeout: Int,
    val readTimeout: Int,
    val timeMillis: Long,
  )

  private class RecordingTransport(private vararg val functionStatuses: Int) : HttpTransport() {
    val requests = mutableListOf<RecordedRequest>()
    var nowMillis: () -> Long = { 0L }
    var onFunctionRequest: () -> Unit = {}
    var disconnectedFunctionResponses = 0
    private var functionRequests = 0

    override fun buildRequest(method: String, url: String): LowLevelHttpRequest =
      object : MockLowLevelHttpRequest(url) {
        private var connectTimeout = 0
        private var readTimeout = 0

        override fun setTimeout(connectTimeout: Int, readTimeout: Int) {
          this.connectTimeout = connectTimeout
          this.readTimeout = readTimeout
        }

        override fun execute(): LowLevelHttpResponse {
          requests +=
            RecordedRequest(
              url,
              getFirstHeaderValue("Authorization"),
              contentAsString,
              connectTimeout,
              readTimeout,
              nowMillis(),
            )
          if (url == ENDPOINT.toString()) onFunctionRequest()
          val content =
            when {
              url == "https://tokens.example.test/subject" -> "{\"token\":\"subject-token\"}"
              url == "https://sts.googleapis.com/v1/token" ->
                """{"access_token":"federated-token","token_type":"Bearer","expires_in":3600,"issued_token_type":"urn:ietf:params:oauth:token-type:access_token"}"""
              url.endsWith(":generateAccessToken") ->
                """{"accessToken":"impersonated-token","expireTime":"2100-01-01T00:00:00Z"}"""
              url.endsWith(":generateIdToken") -> "{\"token\":\"" + token(4102444800) + "\"}"
              url == ENDPOINT.toString() -> "{}"
              else -> error("Unexpected request URL: $url")
            }
          return object : MockLowLevelHttpResponse() {
              override fun disconnect() {
                if (url == ENDPOINT.toString()) disconnectedFunctionResponses++
                super.disconnect()
              }
            }
            .setStatusCode(
              if (url == ENDPOINT.toString())
                functionStatuses.getOrElse(functionRequests++) { functionStatuses.last() }
              else 200
            )
            .setContentType("application/json")
            .setContent(content)
        }
      }
  }

  companion object {
    private val ENDPOINT: URI = URI.create("https://fetcher.example.test/requisition-fetcher")

    private fun token(expiration: Long): String {
      val encoder = Base64.getUrlEncoder().withoutPadding()
      val header = encoder.encodeToString("""{"alg":"RS256"}""".toByteArray())
      val payload =
        encoder.encodeToString("""{"aud":"$ENDPOINT","exp":$expiration}""".toByteArray())
      val signature = encoder.encodeToString("signature".toByteArray())
      return "$header.$payload.$signature"
    }
  }
}
