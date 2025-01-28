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

import com.google.protobuf.Message
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.common.lowerBound
import org.wfanet.measurement.common.upperBound

class FrequencyVectorGenerator(
  private val vidToIndexMap: Map<Long, IndexedValue>,
  private val eventQuery: EventQuery<Message>,
  private val vidSamplingInterval: MeasurementSpec.VidSamplingInterval,
) {
  private val sortedNormalizedHashValues: List<Double>

  init {
    sortedNormalizedHashValues = vidToIndexMap.values.toList().map { it.value }.sorted()
  }

  /** Generates a frequency vector for the specified [eventGroupSpecs]. */
  fun generate(eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>): IntArray {
    require(vidToIndexMap.isNotEmpty()) { "The VID to index map not be empty." }

    val isWrappedAround = (vidSamplingInterval.start + vidSamplingInterval.width > 1.0)
    val samplingIntervalEnd =
      if (isWrappedAround) (vidSamplingInterval.start + vidSamplingInterval.width - 1).toDouble()
      else (vidSamplingInterval.start + vidSamplingInterval.width).toDouble()

    val startIndex = lowerBound(sortedNormalizedHashValues, vidSamplingInterval.start.toDouble())
    val endIndexExclusive = upperBound(sortedNormalizedHashValues, samplingIntervalEnd)

    var sketchSize = 0
    val validIntervals = mutableListOf<IntRange>()

    if (!isWrappedAround) {
      if (startIndex < endIndexExclusive) {
        validIntervals.add(startIndex until endIndexExclusive)
        sketchSize = endIndexExclusive - startIndex
      } else {
        sketchSize = 0
      }
    } else {
      if (startIndex < vidToIndexMap.size) {
        validIntervals.add(startIndex until vidToIndexMap.size)
      }
      sketchSize += (vidToIndexMap.size - startIndex)

      if (endIndexExclusive > 0) {
        validIntervals.add(0 until endIndexExclusive)
      }
      sketchSize += endIndexExclusive
    }

    require(sketchSize > 0) {
      "The sampling interval is too small to yield sketch size larger than 0."
    }

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
            sketch[bucketIndex + vidToIndexMap.size] += 1
          }
        }
    }

    return sketch
  }

  private fun isInSelectedIntervals(index: Int, intervals: List<IntRange>): Boolean {
    return intervals.any { index in it }
  }
}
