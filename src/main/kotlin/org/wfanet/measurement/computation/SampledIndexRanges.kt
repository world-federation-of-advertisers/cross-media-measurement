/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.computation

/**
 * The global VID indexes a VID sampling interval covers, in frequency vector order.
 *
 * @property primaryRange the indexes of [start, min(start + width, 1)), or of [start, 1) for an
 *   interval that wraps around
 * @property wrappedRange the indexes of [0, start + width - 1) for an interval that wraps around,
 *   otherwise empty
 */
data class SampledIndexRanges(val primaryRange: IntRange, val wrappedRange: IntRange) {
  /** The number of indexes, which is the length of a frequency vector over the interval. */
  val size: Int
    get() = primaryRange.count() + wrappedRange.count()

  companion object {
    /**
     * Returns the ranges of the interval of [start] and [width] over [populationSize] global
     * indexes, computed in `Float` so that every party derives the same indexes.
     *
     * @throws IllegalArgumentException if the interval is invalid
     */
    fun of(populationSize: Int, start: Float, width: Float): SampledIndexRanges {
      require(width > 0.0f && width <= 1.0f) { "Invalid sampling width $width" }
      require(start in 0.0f..1.0f) { "Invalid sampling start $start" }
      // For a wrapping interval, globalEndIndex is beyond the population.
      val globalStartIndex = (populationSize * start).toInt()
      val globalEndIndex = (populationSize * (start + width)).toInt() - 1
      return SampledIndexRanges(
        primaryRange = globalStartIndex..minOf(globalEndIndex, populationSize - 1),
        wrappedRange =
          if (globalEndIndex >= populationSize) {
            0..(globalEndIndex - populationSize)
          } else {
            IntRange.EMPTY
          },
      )
    }
  }
}
