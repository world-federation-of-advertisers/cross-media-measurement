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

package org.wfanet.measurement.reporting.validation

import com.google.auth.oauth2.GoogleCredentials
import com.google.auth.oauth2.IdToken
import com.google.auth.oauth2.IdTokenProvider
import com.google.protobuf.InvalidProtocolBufferException
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import org.wfanet.measurement.api.v2alpha.DataProviderImpressionQueryRequest
import org.wfanet.measurement.api.v2alpha.DataProviderImpressionQueryResponse

/**
 * Thrown when a call to a DataProvider's impression validation cloud function fails.
 *
 * @param httpStatus the HTTP status code, or null if the request failed before receiving a
 *   response.
 */
class CloudFunctionException(
  message: String,
  val httpStatus: Int? = null,
  cause: Throwable? = null,
) : Exception(message, cause)

/**
 * HTTP client for calling a DataProvider's impression validation cloud function.
 *
 * Serializes a [DataProviderImpressionQueryRequest] as binary protobuf, posts it to the cloud
 * function endpoint with a Google-issued ID token, and deserializes the
 * [DataProviderImpressionQueryResponse].
 *
 * @param httpClient the [HttpClient] to use for requests.
 * @param idTokenProvider provider of audience-scoped ID tokens for authenticating to the cloud
 *   function.
 */
class ValidationCloudFunctionClient(
  private val httpClient: HttpClient = HttpClient.newHttpClient(),
  private val idTokenProvider: IdTokenProvider =
    GoogleCredentials.getApplicationDefault() as? IdTokenProvider
      ?: throw IllegalArgumentException("Application Default Credentials do not provide ID token"),
) {

  /**
   * Calls the cloud function at the given endpoint.
   *
   * @param endpointUri HTTPS URI of the cloud function.
   * @param request the validation request to send.
   * @param timeout request timeout.
   * @return the parsed response.
   * @throws CloudFunctionException if the call fails or returns a non-success status.
   */
  fun call(
    endpointUri: String,
    request: DataProviderImpressionQueryRequest,
    timeout: Duration,
  ): DataProviderImpressionQueryResponse {
    val idToken: IdToken = idTokenProvider.idTokenWithAudience(endpointUri, emptyList())
    val httpRequest =
      HttpRequest.newBuilder()
        .uri(URI.create(endpointUri))
        .timeout(timeout)
        .header("Authorization", "Bearer ${idToken.tokenValue}")
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        .POST(HttpRequest.BodyPublishers.ofByteArray(request.toByteArray()))
        .build()

    val httpResponse =
      try {
        httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray())
      } catch (e: IOException) {
        throw CloudFunctionException("Cloud function call to $endpointUri failed", cause = e)
      } catch (e: InterruptedException) {
        Thread.currentThread().interrupt()
        throw CloudFunctionException(
          "Cloud function call to $endpointUri was interrupted",
          cause = e,
        )
      }

    val statusCode = httpResponse.statusCode()
    if (statusCode !in 200..299) {
      throw CloudFunctionException(
        "Cloud function at $endpointUri returned HTTP $statusCode",
        httpStatus = statusCode,
      )
    }

    return try {
      DataProviderImpressionQueryResponse.parseFrom(httpResponse.body())
    } catch (e: InvalidProtocolBufferException) {
      throw CloudFunctionException(
        "Cloud function at $endpointUri returned an unparseable response",
        httpStatus = statusCode,
        cause = e,
      )
    }
  }

  companion object {
    private const val CONTENT_TYPE_PROTOBUF = "application/x-protobuf"
  }
}
