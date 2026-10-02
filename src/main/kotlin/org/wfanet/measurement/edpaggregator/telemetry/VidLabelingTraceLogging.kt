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

package org.wfanet.measurement.edpaggregator.telemetry

import java.security.MessageDigest
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.common.telemetry.XmmTraceLogging

/** Writes payload-free VID-labeling lifecycle evidence. */
object VidLabelingTraceLogging {
  /** Returns a stable, one-way SHA-256 digest suitable for correlating a sensitive identifier. */
  fun sha256(value: String): String =
    MessageDigest.getInstance("SHA-256").digest(value.toByteArray(Charsets.UTF_8)).joinToString(
      separator = ""
    ) { byte ->
      (byte.toInt() and 0xff).toString(16).padStart(2, '0')
    }

  fun log(logger: Logger, event: String, vararg fields: Pair<String, String?>) {
    log(logger, Level.INFO, event, *fields)
  }

  fun log(logger: Logger, level: Level, event: String, vararg fields: Pair<String, String?>) {
    XmmTraceLogging.log(logger, level, null, event, SAFE_FIELD_NAMES, *fields)
  }

  private val SAFE_FIELD_NAMES =
    XmmTraceLogging.COMMON_SAFE_FIELD_NAMES + VidLabelingTraceAttributes.SAFE_LOG_FIELD_NAMES
}
