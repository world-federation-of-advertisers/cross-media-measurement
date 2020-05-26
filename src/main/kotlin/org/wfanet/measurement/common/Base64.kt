package org.wfanet.measurement.common

import java.util.Base64

/** Encodes [ByteArray] with RFC 7515's Base64url encoding into a base-64 string. */
fun ByteArray.base64UrlEncode(): String =
  Base64.getUrlEncoder().withoutPadding().encodeToString(this)

/** Decodes a [String] encoded with RFC 7515's Base64url into a [ByteArray]. */
fun String.base64UrlDecode(): ByteArray =
  Base64.getUrlDecoder().decode(this)
