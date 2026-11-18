// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common

import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidator.validateVidRangesList
import org.wfanet.measurement.computation.ResultMinimumThresholds

/** Sampling indexes and frequency caps for a measurement. */
class FrequencyVectorSampling(
  populationSpec: PopulationSpec,
  measurementSpec: MeasurementSpec,
  resultMinimumThresholds: ResultMinimumThresholds?,
  overrideImpressionMaxFrequencyPerUser: Int?,
) {
  /** The maximum frequency allowed in the output frequency vector. */
  val maxFrequency: Int

  init {

    if (measurementSpec.hasReachAndFrequency()) {
      require(measurementSpec.reachAndFrequency.maximumFrequency >= 1) {
        "measurementSpec.reachAndFrequency.maximumFrequency must be >= 1"
      }
    }

    if (measurementSpec.hasImpression()) {
      require(measurementSpec.impression.maximumFrequencyPerUser >= 1) {
        "measurementSpec.impression.maximumFrequencyPerUser must be >= 1"
      }
    }

    if (measurementSpec.hasReach() && resultMinimumThresholds != null) {
      require(resultMinimumThresholds.reachMaxFrequencyPerUser >= 1) {
        "resultMinimumThresholds.maxFrequencyPerUser must be >= 1 for reach measurements with small-cell suppression"
      }
    }

    if (overrideImpressionMaxFrequencyPerUser != null) {
      require(
        overrideImpressionMaxFrequencyPerUser >= 1 || overrideImpressionMaxFrequencyPerUser == -1
      ) {
        "overrideImpressionMaxFrequencyPerUser must be >= 1 or -1 (uncapped)"
      }
    }

    maxFrequency =
      if (
        measurementSpec.hasImpression() &&
          overrideImpressionMaxFrequencyPerUser != null &&
          overrideImpressionMaxFrequencyPerUser != -1
      ) {
        overrideImpressionMaxFrequencyPerUser
      } else if (measurementSpec.hasReachAndFrequency()) {
        measurementSpec.reachAndFrequency.maximumFrequency
      } else if (measurementSpec.hasImpression()) {
        measurementSpec.impression.maximumFrequencyPerUser
      } else if (measurementSpec.hasReach()) {
        resultMinimumThresholds?.reachMaxFrequencyPerUser ?: 1
      } else if (measurementSpec.hasMulti()) {
        // A MultiMeasurementSpec carries no frequency cap. The TEE folds the histogram downstream,
        // so the only bound is the cell: a frequency saturates at the largest signed byte, since a
        // larger value would reach the TEE negative and be rejected.
        Byte.MAX_VALUE.toInt()
      } else {
        1
      }
  }

  /**
   * For a non-wrapping sampling interval this is the entire range of indexes in the VidIndexMap
   * that correspond to the sampling interval [start, start+width) For a wrapping sampling interval
   * this is the range of indexes that correspond to the sub-interval [start, 1.0)
   */
  val primaryRange: IntRange

  val primaryRangeCount: Int

  /**
   * For a wrapping VidSamplingInterval this is the part of the range than spans [0, width-start)
   */
  val wrappedRange: IntRange

  init {
    val vidSamplingInterval = measurementSpec.vidSamplingInterval
    require(vidSamplingInterval.width > 0 && vidSamplingInterval.width <= 1.0) {
      "MeasurementSpec.VidSamplingInterval.width must be > 0 and <= 1"
    }
    require(vidSamplingInterval.start in 0.0..1.0) {
      "MeasurementSpec.VidSamplingInterval.start must be >= 0 and <= 1"
    }

    validateVidRangesList(populationSpec).getOrThrow()
    val populationSizeLong = populationSpec.size
    require(populationSizeLong > 0 && populationSizeLong < Int.MAX_VALUE) {
      "population size must be > 0 and < Int.MAX_VALUE"
    }
    val populationSize = populationSizeLong.toInt()

    // If we have a wrapping interval globalEndIndex will be larger than the populationSize
    val globalStartIndex = (populationSize * vidSamplingInterval.start).toInt()
    val globalEndIndex =
      (populationSize * (vidSamplingInterval.start + vidSamplingInterval.width)).toInt() - 1
    primaryRange = globalStartIndex..minOf(globalEndIndex, populationSize - 1)
    primaryRangeCount = primaryRange.count()
    wrappedRange =
      if (globalEndIndex >= populationSize) {
        0..(globalEndIndex - populationSize)
      } else {
        IntRange.EMPTY
      }
  }

  val size: Int
    get() = primaryRangeCount + wrappedRange.count()

  /** Returns the sampled, capped frequency at [sampledIndex], treating missing input as zero. */
  fun frequencyAt(frequencyData: ByteArray, sampledIndex: Int): Int {
    val sourceIndex =
      if (sampledIndex < primaryRangeCount) primaryRange.first + sampledIndex
      else sampledIndex - primaryRangeCount
    return if (sourceIndex < frequencyData.size) {
      minOf(frequencyData[sourceIndex].toInt() and 0xFF, maxFrequency)
    } else {
      0
    }
  }

  /** Copies the sampled frequencies into an independently owned byte array. */
  fun sample(frequencyData: ByteArray): ByteArray =
    ByteArray(size) { frequencyAt(frequencyData, it).toByte() }
}
