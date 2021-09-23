// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.common.crypto

import com.google.protobuf.ByteString
import java.security.MessageDigest

private const val HASH_ALGORITHM = "SHA-256"

/**
 * Converts a [ByteArray] to a [Long]. A [Long] is 64 bits so the input [ByteArray] must be at least
 * 8 bytes long.
 */
private fun ByteArray.toLong(): Long {
  require(this.size >= 8)
  var result: Long = 0
  var bitCount: Int = 0
  for (byte in this.takeLast(8).toByteArray()) {
    result = result or (byte.toLong() shl bitCount)
    bitCount += 8
  }
  return result
}

/**
 * Generates a long representing the SHA-256 of [data] without a salt. To properly distribute the
 * data across shards and buckets, we use a two step process:
 * 1. Convert to Long: A 8 byte long has 64 bits of information which is 1/4 the space of sha256.
 * 2. Limit the hash space: We limit the hash space to the input maxValue which is typically
 * numShards * numBuckets. A very small number of hashes will fall above the maximum multiple of
 * maxValue less than MAX_LONG. They represent such a small number of overall hashes, that we do not
 * recursively rehash them.
 */
fun hashSha256ToSpace(data: ByteString, maxValue: Long): Long {
  val sha256MessageDigest = MessageDigest.getInstance(HASH_ALGORITHM)
  // TODO Batch hash and convert to long using the fingerprinter in this same repo
  return sha256MessageDigest.digest(data.toByteArray()).toLong() % maxValue
}
