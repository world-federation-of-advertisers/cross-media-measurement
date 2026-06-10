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

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.DataProviderImpressionValidationRequest
import org.wfanet.measurement.api.v2alpha.DataProviderImpressionValidationResponse

/**
 * HTTP client for calling a DataProvider's impression validation cloud function.
 *
 * Serializes a [DataProviderImpressionValidationRequest] as binary protobuf, posts it to the cloud
 * function endpoint, and deserializes the [DataProviderImpressionValidationResponse]. Handles
 * timeouts and retries on 5xx errors.
 *
 * @param httpClient the [HttpClient] to use for requests.
 * @param maxRetries maximum number of retries on 5xx responses.
 * @param retryDelay initial delay between retries.
 */
class ValidationCloudFunctionClient(
  private val httpClient: HttpClient = HttpClient.newHttpClient(),
  private val maxRetries: Int = DEFAULT_MAX_RETRIES,
  private val retryDelay: Duration = DEFAULT_RETRY_DELAY,
) {

  /**
   * Outcome of a cloud function call.
   *
   * @param response the parsed response, or null if the call failed.
   * @param error description of the failure, or null on success.
   * @param httpStatus the HTTP status code, or -1 if the request failed before receiving a
   *   response.
   */
  data class CallResult(
    val response: DataProviderImpressionValidationResponse?,
    val error: String?,
    val httpStatus: Int,
  )

  /**
   * Calls the cloud function at the given endpoint.
   *
   * @param endpointUri HTTPS URI of the cloud function.
   * @param request the validation request to send.
   * @param timeout request timeout.
   * @return the call result with response or error details.
   */
  fun call(
    endpointUri: String,
    request: DataProviderImpressionValidationRequest,
    timeout: Duration,
  ): CallResult {
    val httpRequest =
      HttpRequest.newBuilder()
        .uri(URI.create(endpointUri))
        .timeout(timeout)
        .header("Content-Type", CONTENT_TYPE_PROTOBUF)
        .POST(HttpRequest.BodyPublishers.ofByteArray(request.toByteArray()))
        .build()

    var lastException: Exception? = null
    var lastStatus = -1

    for (attempt in 0..maxRetries) {
      if (attempt > 0) {
        val delay = retryDelay.multipliedBy(attempt.toLong())
        logger.info("Retrying cloud function call (attempt ${attempt + 1}/${maxRetries + 1})")
        Thread.sleep(delay.toMillis())
      }

      try {
        val httpResponse = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray())
        lastStatus = httpResponse.statusCode()

        if (lastStatus in 200..299) {
          val response =
            DataProviderImpressionValidationResponse.parseFrom(httpResponse.body())
          return CallResult(response = response, error = null, httpStatus = lastStatus)
        }

        if (lastStatus in 500..599) {
          lastException =
            Exception("Cloud function returned HTTP $lastStatus")
          logger.log(Level.WARNING, "Cloud function returned HTTP $lastStatus, will retry")
          continue
        }

        return CallResult(
          response = null,
          error = "Cloud function returned HTTP $lastStatus",
          httpStatus = lastStatus,
        )
      } catch (e: Exception) {
        lastException = e
        lastStatus = -1
        logger.log(Level.WARNING, "Cloud function call failed", e)
      }
    }

    return CallResult(
      response = null,
      error = "Cloud function call failed after ${maxRetries + 1} attempts: ${lastException?.message}",
      httpStatus = lastStatus,
    )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(ValidationCloudFunctionClient::class.java.name)

    private const val CONTENT_TYPE_PROTOBUF = "application/x-protobuf"
    private const val DEFAULT_MAX_RETRIES = 2
    private val DEFAULT_RETRY_DELAY: Duration = Duration.ofSeconds(2)
  }
}
