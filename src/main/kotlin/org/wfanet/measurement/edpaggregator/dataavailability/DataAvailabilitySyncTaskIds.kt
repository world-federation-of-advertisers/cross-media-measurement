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

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.UUID

/** Deterministic identifiers for one exact labeled-output object version. */
object DataAvailabilitySyncTaskIds {
  fun canonicalDoneBlobUri(uri: String): String {
    val parsed = URI.create(uri)
    require(parsed.scheme.equals("gs", ignoreCase = true)) { "done blob URI must use gs://" }
    require(!parsed.authority.isNullOrEmpty() && parsed.rawPath.length > 1) {
      "done blob URI must include a bucket and object"
    }
    require(parsed.rawQuery == null && parsed.rawFragment == null) {
      "done blob URI must not include a query or fragment"
    }
    return "gs://${parsed.authority}${parsed.rawPath}"
  }

  fun pathHash(uri: String): String =
    sha256(canonicalDoneBlobUri(uri)).joinToString("") { byte ->
      (byte.toInt() and 0xff).toString(16).padStart(2, '0')
    }

  fun resourceId(uri: String, generation: Long): String {
    require(generation > 0) { "generation must be positive" }
    val identityHash =
      sha256("${pathHash(uri)}:$generation").joinToString("") { byte ->
        (byte.toInt() and 0xff).toString(16).padStart(2, '0')
      }
    return "das-${identityHash.take(32)}"
  }

  fun requestId(uri: String, generation: Long): String =
    uuid4FromBytes(sha256("dataAvailabilitySyncTask:${pathHash(uri)}:$generation"))

  private fun sha256(value: String): ByteArray =
    MessageDigest.getInstance("SHA-256").digest(value.toByteArray(UTF_8))

  private fun uuid4FromBytes(bytes: ByteArray): String {
    bytes[6] = ((bytes[6].toInt() and 0x0f) or 0x40).toByte()
    bytes[8] = ((bytes[8].toInt() and 0x3f) or 0x80).toByte()
    var mostSignificantBits = 0L
    var leastSignificantBits = 0L
    for (index in 0 until 8) {
      mostSignificantBits = (mostSignificantBits shl 8) or (bytes[index].toLong() and 0xff)
    }
    for (index in 8 until 16) {
      leastSignificantBits = (leastSignificantBits shl 8) or (bytes[index].toLong() and 0xff)
    }
    return UUID(mostSignificantBits, leastSignificantBits).toString()
  }
}
