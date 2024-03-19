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
import com.google.protobuf.Message
import java.util.Collections
import org.wfanet.measurement.api.v2alpha.MeasurementSpec

class ShareShuffleSketchGenerator(
  private val vidUniverse: List<Long>,
  private val salt: ByteString,
  private val inputVidToIndexMap: Map<Long, IndexedValue>,
  private val eventQuery: EventQuery<Message>,
  private val vidSamplingInterval: MeasurementSpec.VidSamplingInterval,
) {
  private val sortedNormalizedHashValues: List<Double>
  private val vidToIndexMap: Map<Long, IndexedValue>

  init {
    vidToIndexMap =
      if (inputVidToIndexMap.isNotEmpty()) inputVidToIndexMap
      else VidToIndexMapGenerator.generateMapping(salt, vidUniverse)
    sortedNormalizedHashValues = vidToIndexMap.values.toList().map { it.value }.sorted()
  }
  /** Generates a frequency vector for the specified [eventGroupSpecs]. */
  fun generate(eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>): IntArray {
    require(vidUniverse.isNotEmpty()) { "The vid universe size must be positive." }

    var samplingIntervalEnd: Double =
      (vidSamplingInterval.start + vidSamplingInterval.width).toDouble()

    val isWrappedAround = (samplingIntervalEnd > 1.0)

    if (isWrappedAround) {
      samplingIntervalEnd -= 1.0
    }

    val startIndex = lowerBound(sortedNormalizedHashValues, vidSamplingInterval.start.toDouble())
    val endIndexExclusive = upperBound(sortedNormalizedHashValues, samplingIntervalEnd)

    var sketchSize: Int = 0
    val validIntervals = mutableListOf<IntRange>()

    if (!isWrappedAround) {
      if (startIndex < endIndexExclusive) {
        validIntervals.add(startIndex..(endIndexExclusive - 1))
        sketchSize = endIndexExclusive - startIndex
      } else {
        sketchSize = 0
      }
    } else {
      if (startIndex < vidUniverse.size) {
        validIntervals.add(startIndex..(vidUniverse.size - 1))
      }
      sketchSize += (vidUniverse.size - startIndex)

      if (endIndexExclusive > 0) {
        validIntervals.add(0..(endIndexExclusive - 1))
      }
      sketchSize += endIndexExclusive
    }

    require(sketchSize > 0) { "The sampling interval is too small." }

    val sketch = IntArray(sketchSize) { 0 }

    for (eventGroupSpec in eventGroupSpecs) {
      eventQuery
        .getUserVirtualIds(eventGroupSpec)
        .filter { isInSelectedIntervals(vidToIndexMap.getValue(it).index, validIntervals) }
        .forEach {
          val bucketIndex: Int = vidToIndexMap.getValue(it).index - startIndex
          if (bucketIndex >= 0) {
            sketch[bucketIndex] += 1
          } else {
            sketch[bucketIndex + vidUniverse.size] += 1
          }
        }
    }

    return sketch
  }

  private fun isInSelectedIntervals(index: Int, intervals: List<IntRange>): Boolean {
    return intervals.any { index in it }
  }

  /**
   * Finds the smallest index i such that sortedList[i] >= target.
   *
   * The value `sortedList.size` is returned in case all values are less than `target`. The
   * implementation is based on the c++ std::lower_bound.
   */
  private fun lowerBound(sortedList: List<Double>, target: Double): Int {
    require(sortedList.isNotEmpty()) { "Input list cannot be empty." }

    // Obtains the index of the target if there is a match, otherwise return (-insertionPoint - 1).
    var index = Collections.binarySearch(sortedList, target)

    // Finds the lower bound index.
    //
    // If there is an exact match, finds the first index where the value is not less than target.
    // Otherwise, the lower bound is the insertion point.
    if (index >= 0) {
      while (index > 0 && sortedList[index - 1] >= target) {
        index--
      }
      return index
    } else {
      return -(index + 1)
    }
  }

  /**
   * Finds the smallest index i such that sortedList[i] > target.
   *
   * The value `sortedList.size` is returned in case all values are less than or equal to `target`.
   * The implementation is based on the c++ std::upper_bound
   */
  private fun upperBound(sortedList: List<Double>, target: Double): Int {
    require(sortedList.isNotEmpty()) { "Input list cannot be empty." }

    // Obtains the index of the target if there is a match, otherwise return (-insertionPoint - 1).
    var index = Collections.binarySearch(sortedList, target)

    // Finds the upper bound index.
    //
    // If there is an exact match, finds the first index where the value is greater than target (if
    // the rest of the list is equal to target, the upper bound is sortedList.size).
    // Otherwise, the lower bound is the insertion point.
    if (index >= 0) {
      while (index < sortedList.size && sortedList[index] <= target) {
        index++
      }
      return index
    } else {
      return -(index + 1)
    }
  }
}
