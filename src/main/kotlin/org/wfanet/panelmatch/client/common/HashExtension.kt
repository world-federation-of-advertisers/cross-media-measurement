package org.wfanet.panelmatch.common

import com.google.common.hash.HashFunction
import com.google.common.hash.Hashing
import com.google.protobuf.ByteString

private val SHA256_HASHER: HashFunction = Hashing.sha256()

/**
 * Computes the SHA-256 hash of the [ByteString].
 *
 * @return A new [ByteString] representing the SHA-256 hash.
 */
fun ByteString.sha256(): ByteString {
  require(!this.isEmpty) { "Input ByteString cannot be empty for SHA-256 hashing." }

  val hasher = SHA256_HASHER.newHasher()
  hasher.putBytes(this.asReadOnlyByteBuffer())
  return ByteString.copyFrom(hasher.hash().asBytes())
}