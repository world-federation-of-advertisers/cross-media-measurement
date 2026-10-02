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

package org.wfanet.measurement.common.telemetry

import java.util.logging.Level
import java.util.logging.Logger

/** Writes payload-free lifecycle evidence using an explicit field allowlist. */
object XmmTraceLogging {
  fun log(
    logger: Logger,
    level: Level,
    error: Throwable?,
    event: String,
    safeFieldNames: Set<String>,
    vararg fields: Pair<String, String?>,
  ) {
    require(EVENT_PATTERN.matches(event)) { "Invalid trace event name" }

    val message = buildString {
      append("event=").append(event)
      for ((name, value) in fields) {
        require(name in safeFieldNames) { "Unsupported trace log field: $name" }
        if (!value.isNullOrBlank()) {
          append(' ').append(name).append('=').append(sanitizeToken(value))
        }
      }
    }
    logger.log(level, message, error)
  }

  val COMMON_SAFE_FIELD_NAMES: Set<String> =
    setOf(
      XmmTraceAttributes.WORK_ITEM_NAME_STRING,
      XmmTraceAttributes.WORK_ITEM_ATTEMPT_NAME_STRING,
      XmmTraceAttributes.WORK_ITEM_GENERATION_STRING,
      XmmTraceAttributes.LIFECYCLE_STAGE_STRING,
      XmmTraceAttributes.OUTCOME_STRING,
      XmmTraceAttributes.ERROR_TYPE_STRING,
      XmmTraceAttributes.ERROR_CODE_STRING,
    )

  private fun sanitizeToken(value: String): String =
    value.replace(WHITESPACE_PATTERN, "_").take(MAX_VALUE_LENGTH)

  private val EVENT_PATTERN = Regex("[a-zA-Z0-9._-]+")
  private val WHITESPACE_PATTERN = Regex("\\s+")
  private const val MAX_VALUE_LENGTH = 1000
}
