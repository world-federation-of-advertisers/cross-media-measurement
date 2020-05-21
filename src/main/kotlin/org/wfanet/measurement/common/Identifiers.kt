package org.wfanet.measurement.common

import java.math.BigInteger
import java.util.Base64

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

/** Encodes [ByteArray] with RFC 7515's Base64url encoding into a base-64 string. */
private fun ByteArray.base64UrlEncode() : String =
  Base64.getUrlEncoder().withoutPadding().encodeToString(this)

private fun String.base64UrlDecode() : ByteArray =
  Base64.getUrlDecoder().decode(this)

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
