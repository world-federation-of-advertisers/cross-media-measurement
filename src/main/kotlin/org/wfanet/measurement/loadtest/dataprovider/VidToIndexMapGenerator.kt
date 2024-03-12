/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.ByteString
import java.math.BigInteger
import java.nio.ByteOrder
import org.wfanet.measurement.common.HexString
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.toByteString

object VidToIndexMapGenerator {
  /** Generates the hash of (vid + salt). */
  private fun generateHash(vid: Long, salt: ByteString): String {
    val hashInput = vid.toByteString(ByteOrder.BIG_ENDIAN).concat(salt)
    return HexString(Hashing.hashSha256(hashInput)).toString()
  }

  /**
   * Generates the map (vid, (bucket index, normalized hash)) for all vids in the vid universe. Each
   * vid is concatenated with a `salt`, then the sha256 of the combined string is computed. The
   * vid's are sorted based on its hash value. The bucket index of a vid is its located in the
   * sorted array.
   */
  fun generateMapping(salt: ByteString, vidUniverse: List<Long>): Map<Long, Pair<Int, Double>> {
    require(vidUniverse.size > 0) { "The vid universe must be greater than or equal to 0." }

    val hashes = mutableListOf<Pair<Long, BigInteger>>()

    for (vid in vidUniverse) {
      val hash = BigInteger(generateHash(vid, salt), 16)
      hashes.add(Pair(vid, hash))
    }

    // Sorts by the hash values. There are no collisions due to the use of secure cryptography hash.
    hashes.sortBy { it.second }

    // Maps the hash values to the unit interval and generates the vid to index and normalized hash
    // map.
    val maxHashValue = BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE).toDouble()
    val vidMap = mutableMapOf<Long, Pair<Int, Double>>()
    for ((index, pair) in hashes.withIndex()) {
      val normalizedHashValue = pair.second.toDouble() / maxHashValue
      vidMap[pair.first] = Pair(index, normalizedHashValue)
    }

    return vidMap
  }
}
