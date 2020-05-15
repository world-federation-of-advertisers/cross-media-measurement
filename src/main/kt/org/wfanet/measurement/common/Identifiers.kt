package org.wfanet.measurement.common

import java.math.BigInteger
import java.util.Base64

/** Typesafe wrapper around Long to represent the integer format used below the service layer. */
data class ExternalId(val value: Long) {
  init {
    if (value < 0) throw IllegalArgumentException("Negative id numbers are not permitted: $value")
  }
  val apiId: ApiId
    get() = ApiId(Base64.getUrlEncoder()
                    .withoutPadding()
                    .encodeToString(value.toByteArray()))
}

/** Typesafe wrapper around String to represent the service-layer identifier format. */
data class ApiId(val value: String) {
  val externalId: ExternalId
    get() = ExternalId(Base64.getUrlDecoder()
                         .decode(value)
                         .toLong())
}

// An alternative is: toBigInteger().toByteArray(), but that includes the sign bit, which uses an
// extra byte.
private fun Long.toByteArray(): ByteArray =
  ByteArray(8) {
    ((this shr ((7 - it) * 8)) and 0xFF).toByte()
  }

private fun ByteArray.toLong(): Long {
  if (size != 8) {
    throw IllegalArgumentException("Invalid conversion: $this is not 8 bytes")
  }
  return BigInteger(this).toLong().also {
    if (it < 0L) {
      throw IllegalArgumentException("Negative id numbers are not permitted: $it")
    }
  }
}
