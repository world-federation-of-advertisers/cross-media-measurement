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
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.toByteString

data class IndexedValue(val index: Int, val value: Double)

object VidToIndexMapGenerator {
  /** Generates the hash of (vid + salt). */
  private fun generateHash(vid: Long, salt: ByteString): ByteString {
    val hashInput = vid.toByteString(ByteOrder.BIG_ENDIAN).concat(salt)
    return Hashing.hashSha256(hashInput)
  }

  /**
   * Generates the map (vid, (bucket index, normalized hash)) for all vids in the vid universe.
   *
   * Each vid is concatenated with a `salt`, then the sha256 of the combined string is computed. The
   * vid's are sorted based on its hash value. The bucket index of a vid is its location in the
   * sorted array.
   */
  fun generateMapping(
    vidUniverse: Sequence<Long>,
    salt: ByteString = ByteString.EMPTY,
  ): Map<Long, IndexedValue> {
    require(!vidUniverse.none()) { "The vid universe must not be empty." }

    val hashes = mutableListOf<Pair<Long, Double>>()

    for (vid in vidUniverse) {
      // Converts the hash to a non-negative BigInteger.
      val hash = BigInteger(1, generateHash(vid, salt).toByteArray()).toDouble()
      hashes.add(Pair(vid, hash))
    }

    // Sorts by the hash values and uses vid to break tie in case of collision.
    hashes.sortWith(compareBy<Pair<Long, Double>> { it.second }.thenBy { it.first })

    // Maps the hash values to the unit interval and generates the vid to index and normalized hash
    // value map.
    val maxHashValue = BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE).toDouble()
    val vidMap = mutableMapOf<Long, IndexedValue>()
    for ((index, pair) in hashes.withIndex()) {
      val normalizedHashValue = pair.second / maxHashValue
      vidMap[pair.first] = IndexedValue(index, normalizedHashValue)
    }

    return vidMap
  }
}
