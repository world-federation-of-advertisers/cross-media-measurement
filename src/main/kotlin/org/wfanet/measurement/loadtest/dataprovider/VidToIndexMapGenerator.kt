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
import java.lang.management.ManagementFactory
import java.nio.ByteOrder
import java.security.MessageDigest
import org.wfanet.measurement.common.toByteString

fun printMemoryUsage() {
  fun Double.format(digits: Int) = "%.${digits}f".format(this)

  //  System.gc()
  val runtime = Runtime.getRuntime()

  println("Total memory: ${(runtime.totalMemory() / 1048576.0).format(2)} MB")
  println(
    "Used memory: ${((runtime.totalMemory() - runtime.freeMemory()) / 1048576.0).format(2)} MB"
  )
  println("Free memory: ${(runtime.freeMemory() / 1048576.0).format(2)} MB")
  println("Max memory: ${(runtime.maxMemory() / 1048576.0).format(2)} MB")

  val mxBean = ManagementFactory.getMemoryMXBean()
  val heapUsage = mxBean.heapMemoryUsage
  val nonHeapUsage = mxBean.nonHeapMemoryUsage

  println("Heap Memory Usage: $heapUsage")
  println("Non-Heap Memory Usage: $nonHeapUsage")
}

// Helper extension to format as MB with 2 decimals

data class IndexedValue(val index: Int, val value: Double)

object VidToIndexMapGenerator {

  private val hashing = MessageDigest.getInstance("SHA-256")
  /** Generates the hash of (vid + salt). */
  private fun generateHash(vid: Long, salt: ByteString): ByteString {
    val data = vid.toByteString(ByteOrder.BIG_ENDIAN).concat(salt)
    for (buffer in data.asReadOnlyByteBufferList()) {
      hashing.update(buffer)
    }
    return hashing.digest().toByteString()
  }

  private fun ByteArray.toULong(): ULong {
    var unsignedLong = 0UL
    for (digit in this) {
      unsignedLong = (unsignedLong shl 8) or (digit.toUByte().toULong())
    }
    return unsignedLong
  }

  data class VidToHash(val first: Long, val second: ULong)

  /**
   * Generates the map (vid, (bucket index, normalized hash)) for all vids in the vid universe.
   *
   * Each vid is concatenated with a `salt`, then the sha256 of the combined string is computed. The
   * vid's are sorted based on its hash value. The bucket index of a vid is its location in the
   * sorted array.
   */
  fun generateMapping(salt: ByteString, vidUniverse: Sequence<Long>): Map<Long, IndexedValue> {
    require(!vidUniverse.none()) { "The vid universe must not be empty." }

    val size = vidUniverse.count()

    val hashes = Array(size) { VidToHash(0L, 0UL) }

    var n = 0
    printMemoryUsage()
    for (vid in vidUniverse) {
      // Converts the hash to a non-negative BigInteger.
      val hash = generateHash(vid, salt).toByteArray().copyOfRange(0, 8).toULong()
      hashes[n] = VidToHash(vid, hash)

      n += 1
    }
    println("generateMapping 2")
    printMemoryUsage()

    // Sorts by the hash values and uses vid to break tie in case of collision.
    hashes.sortWith(compareBy<VidToHash> { it.second }.thenBy { it.first })
    println("generateMapping 3")

    // Maps the hash values to the unit interval and generates the vid to index and normalized hash
    // value map.
    val maxHashValue = ULong.MAX_VALUE.toDouble()
    val vidMap = mutableMapOf<Long, IndexedValue>() // 33M * 24 = ~1GB
    for ((index, pair) in hashes.withIndex()) {
      val normalizedHashValue = pair.second.toDouble() / maxHashValue
      vidMap[pair.first] = IndexedValue(index, normalizedHashValue)
    }
    println("generateMapping 4")

    return vidMap
  }
}
