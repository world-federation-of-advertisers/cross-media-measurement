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
import org.wfanet.measurement.api.v2alpha.MeasurementSpec

class ShareShuffleSketchGenerator(
  private val vidUniverse: List<Long>,
  private val salt: ByteString,
  private var vidToIndexMap: Map<Long, IndexedValue>,
  private val eventQuery: EventQuery<Message>,
  private val vidSamplingInterval: MeasurementSpec.VidSamplingInterval,
) {
  init {
    if (vidToIndexMap.isEmpty()) {
      vidToIndexMap = VidToIndexMapGenerator.generateMapping(salt, vidUniverse)
    }
  }
  /** Generates a frequency vector for the specified [eventGroupSpecs]. */
  fun generate(eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>): IntArray {
    require(vidUniverse.isNotEmpty()) { "The vid universe size must be positive." }

    val sortedNormalizedHashValues: List<Double> =
      vidToIndexMap.values.toList().map { it.value }.sorted()

    var samplingIntervalEnd: Double =
      (vidSamplingInterval.start + vidSamplingInterval.width).toDouble()

    val isWrappedAround = (samplingIntervalEnd > 1.0)

    if (isWrappedAround) {
      samplingIntervalEnd -= 1.0
    }

    val start = lowerBound(sortedNormalizedHashValues, vidSamplingInterval.start.toDouble())
    val end = upperBound(sortedNormalizedHashValues, samplingIntervalEnd) - 1

    var sketchSize: Int = 0
    val validIntervals = mutableListOf<IntRange>()

    if (!isWrappedAround) {
      if (start <= end) {
        validIntervals.add(start..end)
        sketchSize = end - start + 1
      } else {
        return IntArray(0)
      }
    } else {
      if (start < vidUniverse.size) {
        validIntervals.add(start..(vidUniverse.size - 1))
      }
      sketchSize += (vidUniverse.size - start)

      if (end >= 0) {
        validIntervals.add(0..end)
      }
      sketchSize += (end + 1)
    }

    val sketch = IntArray(sketchSize) { 0 }

    for (eventGroupSpec in eventGroupSpecs) {
      eventQuery
        .getUserVirtualIds(eventGroupSpec)
        .filter { isInSelectedIntervals(vidToIndexMap.getValue(it).index, validIntervals) }
        .forEach {
          val bucketIndex: Int = vidToIndexMap.getValue(it).index - start
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

    var current = 0
    var count = sortedList.size
    while (count > 0) {
      val step = count / 2
      if (sortedList[current + step] < target) {
        current += (step + 1)
        count -= (step + 1)
      } else {
        count = step
      }
    }

    return current
  }

  /**
   * Finds the smallest index i such that sortedList[i] > target.
   *
   * The value `sortedList.size` is returned in case all values are less than or equal to `target`.
   * The implementation is based on the c++ std::upper_bound
   */
  private fun upperBound(sortedList: List<Double>, target: Double): Int {
    require(sortedList.isNotEmpty()) { "Input list cannot be empty." }

    var current = 0
    var count = sortedList.size
    while (count > 0) {
      val step = count / 2
      if (sortedList[current + step] <= target) {
        current += (step + 1)
        count -= (step + 1)
      } else {
        count = step
      }
    }

    return current
  }
}
