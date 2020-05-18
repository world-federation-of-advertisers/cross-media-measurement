package org.wfanet.measurement.common

import java.math.BigInteger
import java.util.Base64

/**
 * Typesafe wrapper to represent the integer identifier format used below the service layer.
 *
 * @property[value] a non-negative integer identifier
 */
data class ExternalId(val value: Long) {
  init {
    require(value >= 0) { "Negative id numbers are not permitted: $value" }
  }

  val apiId: ApiId by lazy {
    ApiId(Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(value.toByteArray()))
  }
}

/**
 * Typesafe wrapper around String to represent the service-layer identifier format.
 *
 * This eagerly decodes [value] into an [ExternalId] to validate the base64 encoding.
 *
 * @property[value] the websafe base64 external identifier
 */
data class ApiId(val value: String) {
  val externalId: ExternalId = ExternalId(Base64.getUrlDecoder().decode(value).toLong())
}

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
