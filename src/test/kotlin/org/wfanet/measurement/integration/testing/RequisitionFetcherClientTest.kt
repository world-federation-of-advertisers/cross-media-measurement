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
import kotlinx.coroutines.test.runTest
import org.junit.Test

class RequisitionFetcherClientTest {
  @Test
  fun `trigger refreshes an expired token before the next request`() = runTest {
    val expiredToken = token(1)
    val currentToken = token(4102444800)
    val credentials =
      TokenCredentials(listOf(Result.success(expiredToken), Result.success(currentToken)))
    val transport = RecordingTransport(200)
    val client = RequisitionFetcherClient(ENDPOINT, credentials, transport)

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

    RequisitionFetcherClient(ENDPOINT, credentials, transport).trigger()

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

      RequisitionFetcherClient(ENDPOINT, credentials, transport).trigger()

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
      assertFailsWith<HttpResponseException> {
        RequisitionFetcherClient(ENDPOINT, credentials, transport).trigger()
      }

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
    val client = RequisitionFetcherClient(ENDPOINT, credentials, transport)
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
  )

  private class RecordingTransport(private val functionStatus: Int) : HttpTransport() {
    val requests = mutableListOf<RecordedRequest>()

    override fun buildRequest(method: String, url: String): LowLevelHttpRequest =
      object : MockLowLevelHttpRequest(url) {
        override fun execute(): LowLevelHttpResponse {
          requests += RecordedRequest(url, getFirstHeaderValue("Authorization"), contentAsString)
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
          return MockLowLevelHttpResponse()
            .setStatusCode(if (url == ENDPOINT.toString()) functionStatus else 200)
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
