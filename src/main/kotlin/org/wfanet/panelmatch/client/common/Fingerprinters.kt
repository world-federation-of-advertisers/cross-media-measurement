package org.wfanet.panelmatch.client.common

import com.google.common.hash.Hashing
import com.google.protobuf.ByteString
import com.google.protobuf.Message
import com.google.protobuf.kotlin.toByteString

/** Helper for fingerprinting data. */
object Fingerprinters {

  /** Returns the FarmHash Fingerprint64 of [bytes]. */
  fun farmHashFingerprint64(bytes: ByteString): ByteString {
    return Hashing.farmHashFingerprint64().hashBytes(bytes.toByteArray()).asBytes().toByteString()
  }

  /** Returns the FarmHash Fingerprint64 of this [Message]. */
  fun Message.farmHashFingerprint64(): ByteString = farmHashFingerprint64(toByteString())
}
