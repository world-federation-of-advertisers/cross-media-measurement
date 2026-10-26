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

package org.wfanet.measurement.loadtest.reporting

import com.google.protobuf.util.JsonFormat
import java.io.IOException
import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.delay
import okhttp3.OkHttpClient
import okhttp3.Request
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.coerceAtMost
import org.wfanet.measurement.reporting.v2alpha.BasicReport

/** Reads BasicReports from the HTTP gateway with bounded retries for transient read failures. */
class BasicReportHttpClient(
  private val httpClient: OkHttpClient,
  private val retryBackoff: ExponentialBackoff,
  private val maximumRetryDelay: Duration,
  private val maxAttempts: Int,
) {
  init {
    require(maxAttempts > 0)
    require(!maximumRetryDelay.isNegative && !maximumRetryDelay.isZero)
  }

  /** Reads [request], which must use GET. Creation requests are never retried here. */
  suspend fun getBasicReport(request: Request): BasicReport {
    require(request.method == "GET")
    var attempt = 1
    while (true) {
      val json =
        try {
          httpClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
              val message = "Error retrieving BasicReport: ${response.code} ${response.message}"
              if (response.code !in TRANSIENT_HTTP_CODES) throw IllegalStateException(message)
              throw IOException(message)
            }
            checkNotNull(response.body).string()
          }
        } catch (e: IOException) {
          if (attempt >= maxAttempts) throw e
          val retryDelay = retryBackoff.durationForAttempt(attempt).coerceAtMost(maximumRetryDelay)
          logger.warning(
            "Transient BasicReport read failure on attempt $attempt of $maxAttempts; retrying"
          )
          delay(retryDelay.toMillis())
          attempt++
          continue
        }
      // Parse outside the I/O retry catch: malformed successful responses are not transient.
      return BasicReport.newBuilder()
        .also { JsonFormat.parser().ignoringUnknownFields().merge(json, it) }
        .build()
    }
  }

  companion object {
    private val logger = Logger.getLogger(BasicReportHttpClient::class.java.name)
    private val TRANSIENT_HTTP_CODES = setOf(429, 500, 502, 503, 504)
  }
}
