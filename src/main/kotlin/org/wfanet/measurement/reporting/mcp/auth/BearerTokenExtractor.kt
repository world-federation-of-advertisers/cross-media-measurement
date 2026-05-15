/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.mcp.auth

import io.ktor.http.HttpHeaders
import io.ktor.server.request.ApplicationRequest

object BearerTokenExtractor {
  private const val BEARER_PREFIX = "Bearer "

  /**
   * Extracts a bearer token from the Authorization header.
   *
   * @return the token string, or `null` if the header is missing or malformed.
   */
  fun extract(request: ApplicationRequest): String? {
    val header: String = request.headers[HttpHeaders.Authorization] ?: return null
    if (!header.startsWith(BEARER_PREFIX, ignoreCase = true)) {
      return null
    }
    return header.substring(BEARER_PREFIX.length).trim().ifEmpty { null }
  }
}
