// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.common.identity

import java.math.BigInteger
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode

/**
 * Typesafe wrapper around Long to represent the integer format used below the service layer for the
 * internal representation of external identifiers.
 *
 * @property[value] a non-negative integer identifier
 */
data class ExternalId(val value: Long) {
  init {
    require(value >= 0) { "Negative id numbers are not permitted: $value" }
  }

  val apiId: ApiId by lazy { ApiId(value.toByteArray().base64UrlEncode()) }

  override fun toString(): String = "ExternalId($value / ${apiId.value})"
}

/**
 * Typesafe wrapper around String to represent the service-layer identifier format.
 *
 * This eagerly decodes [value] into an [ExternalId] to validate the base64 encoding.
 *
 * @property[value] the websafe base64 external identifier
 */
data class ApiId(val value: String) {
  val externalId: ExternalId = ExternalId(value.base64UrlDecode().toLong())
}

/** Typesafe wrapper around Long to represent the integer id format used internally. */
data class InternalId(val value: Long)

// An alternative is: toBigInteger().toByteArray(), but that includes the sign bit, which uses an
// extra byte.
private fun Long.toByteArray(): ByteArray =
  ByteArray(8) {
    ((this shr ((7 - it) * 8)) and 0xFF).toByte()
  }

private fun ByteArray.toLong(): Long {
  require(size == 8) { "Invalid conversion: $this is not 8 bytes" }
  return BigInteger(this).toLong().also {
    require(it >= 0L) {
      "Negative id numbers are not permitted: $it"
    }
  }
}
