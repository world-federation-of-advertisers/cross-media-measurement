// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.eventdataprovider.shareshuffle

import com.google.protobuf.ByteString
import java.io.DataOutputStream
import org.roaringbitmap.RoaringBitmap
import org.wfanet.frequencycount.FrequencyVector
import org.wfanet.frequencycount.frequencyVector

/** Returns a [CompressedFrequencyVector] representation of this [FrequencyVector]. */
fun FrequencyVector.compress(maxFrequency: Int): CompressedFrequencyVector {
  val bitmaps: List<RoaringBitmap> = List(maxFrequency) { RoaringBitmap() }

  for ((register, frequency) in dataList.withIndex()) {
    if (frequency == 0) {
      continue
    }
    val bitmap = bitmaps[frequency - 1]
    bitmap.add(register)
  }

  val serializedBitmaps: List<ByteString> =
    List(maxFrequency) { index ->
      val bitmap = bitmaps[index]
      bitmap.runOptimize()
      ByteString.newOutput().use { byteStringOutput ->
        DataOutputStream(byteStringOutput).use { dataOutputStream ->
          bitmap.serialize(dataOutputStream)
        }
        byteStringOutput.toByteString()
      }
    }

  return CompressedFrequencyVector(
    registerCount = dataCount,
    maxFrequency = maxFrequency,
    registerBitmaps = serializedBitmaps,
  )
}

/**
 * Returns a [FrequencyVector] obtained by merging the register-wise data of all
 * [CompressedFrequencyVector]s in this iterable.
 *
 * Requires that this iterable be non-empty. Also requires that all elements being merged have the
 * same register count and max frequency.
 */
fun Iterable<CompressedFrequencyVector>.merge(): FrequencyVector {
  val iterator: Iterator<CompressedFrequencyVector> = iterator()
  require(iterator.hasNext()) { "merge() requires at least one element" }

  val first = iterator.next()

  fun IntArray.merge(compressedFrequencyVector: CompressedFrequencyVector) {
    require(compressedFrequencyVector.registerCount == first.registerCount) {
      "Register count ${compressedFrequencyVector.registerCount} does not match expected ${first.registerCount}"
    }
    require(compressedFrequencyVector.maxFrequency == first.maxFrequency) {
      "Max frequency ${compressedFrequencyVector.maxFrequency} does not match expected ${first.maxFrequency}"
    }
    for ((register, frequency) in compressedFrequencyVector) {
      this[register] = (this[register] + frequency).coerceAtMost(first.maxFrequency)
    }
  }

  val mergedFrequencyData = IntArray(first.registerCount).apply { merge(first) }
  while (iterator.hasNext()) {
    mergedFrequencyData.merge(iterator.next())
  }

  return frequencyVector { data += mergedFrequencyData.asList() }
}
