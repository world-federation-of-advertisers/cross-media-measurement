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

package org.wfanet.measurement.edpaggregator.dataavailability

import java.security.MessageDigest
import java.util.UUID

/** Deterministic synchronization-attempt identities for durable availability tasks. */
object DataAvailabilitySyncTaskAttempts {
  fun leaseId(taskName: String, attemptCount: Int): String {
    require(taskName.isNotEmpty()) { "taskName must not be empty" }
    require(attemptCount > 0) { "attemptCount must be positive" }
    val bytes =
      MessageDigest.getInstance("SHA-256")
        .digest("dataAvailabilitySyncTaskAttempt:$taskName:$attemptCount".toByteArray())
    bytes[6] = ((bytes[6].toInt() and 0x0f) or 0x40).toByte()
    bytes[8] = ((bytes[8].toInt() and 0x3f) or 0x80).toByte()
    var mostSignificantBits = 0L
    var leastSignificantBits = 0L
    for (index in 0 until 8) {
      mostSignificantBits = (mostSignificantBits shl 8) or (bytes[index].toLong() and BYTE_MASK)
    }
    for (index in 8 until 16) {
      leastSignificantBits = (leastSignificantBits shl 8) or (bytes[index].toLong() and BYTE_MASK)
    }
    return UUID(mostSignificantBits, leastSignificantBits).toString()
  }

  private const val BYTE_MASK = 0xffL
}
