// Copyright 2024 The Cross-Media Measurement Authors
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

import org.wfanet.frequencycount.FrequencyVector
import org.wfanet.frequencycount.frequencyVector
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.VidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidator.validateVidRangesList

/**
 * TODO: update this
 * A utility for building a [FrequencyVector].
 *
 * This class is a utility for building an appropriately sized [FrequencyVector] for a given
 * [VidSamplingInterval], and the [PopulationSpec] associated with a given [VidIndexMap].
 *
 * This class does not provide a constructor that accepts a [PopulationSpec] because this class
 * would have to use it to create a [VidIndexMap], which is a reasonably expensive operation.
 * Therefore, the caller is expected to create the [VidIndexMap] and pass it in.
 *
 * The [MeasurementSpec] is used in several ways. First, it must specify either a Reach
 * or ReachAndFrequency measurement. In the case of Reach, the FrequencyVectorBuilder will
 * clamp all frequency values at maximum of 1. In the case of a ReachAndFrequency measurement,
 * the max frequency parameter is used to clamp the frequency value. In both cases the
 * vidSamplingInterval is used to appropriately size the output vector and if the client
 * does not do its own filtering, filter the VIDs.
 *
 * @constructor Creates a [FrequencyVectorBuilder] for the given [VidIndeMap] and [PopulationSpec]
 *
 * @param vidIndexMap The [VidIndexMap] for the population being measured
 * @param measurementSpec A [MeasurementSpec] that specifies a Reach or ReachAndFrequency
 * @param strict TODO
 * measurement.
 */
class FrequencyVectorBuilder(
  val populationSpec: PopulationSpec,
  val measurementSpec: MeasurementSpec,
  val strict: Boolean = true) {

  /**
   * The accumulated frequency data
   */
  private val frequencyData : IntArray

  /**
  * For a non-wrapping sampling interval this is the entire range of indexes in the VidIndexMap
  * that correspond to the sampling interval [start, start+width)
  * For a wrapping sampling interval this is the range of indexes that correspond to
  * the sub-interval [start, 1.0]
  */
  private val primaryRange: IntRange

  /**
   * For a wrapping VidSamplingInterval this is the part of the range than spans
   * [0, width-start)
   */
  private val wrappedRange: IntRange

  /**
   * The maximum frequency allowed in the output frequency vector.
   */
  private val maxFrequency: Int

  /**
   * The size of the frequency vector being managed
   */
  val size: Int
    get() = frequencyData.size

  init {
    require(measurementSpec.hasReach() || measurementSpec.hasReachAndFrequency())
    { "MeasurementSpec must have either a Reach or ReachAndFrequency measurementType" }

    if (measurementSpec.hasReachAndFrequency()) {
      require(measurementSpec.reachAndFrequency.maximumFrequency >= 1)
      { "measurementSpec.reachAndFrequency.maximumFrequency must be >= 1" }
    }

    maxFrequency =
      if (measurementSpec.hasReachAndFrequency()) {
        measurementSpec.reachAndFrequency.maximumFrequency
      } else {
        1
      }

    val vidSamplingInterval = measurementSpec.vidSamplingInterval
    require(vidSamplingInterval.width > 0 && vidSamplingInterval.width <= 1.0)
    { "MeasurementSpec.VidSamplingInterval.width must be > 0 and <= 1.0" }
    require(vidSamplingInterval.start in 0.0..1.0)
    { "MeasurementSpec.VidSamplingInterval.start must be >= 0 and <= 1.0" }

    validateVidRangesList(populationSpec).getOrThrow()
    val populationSize = getPopulationSize(populationSpec)

    // If we have a wrapping interval that globalEndIndex will be larger than
    // the populationSize
    val globalStartIndex = (populationSize * vidSamplingInterval.start).toInt()
    val globalEndIndex = (populationSize * (vidSamplingInterval.start + vidSamplingInterval.width)).toInt() - 1
    primaryRange = globalStartIndex .. minOf(globalEndIndex, populationSize - 1)
    wrappedRange = if (globalEndIndex >= populationSize) {
       0 .. (globalEndIndex - populationSize)
    }  else {
      IntRange.EMPTY
    }

    // Zero out the frequency vector
    val frequencyVectorSize = primaryRange.count() + wrappedRange.count()
    frequencyData = IntArray(frequencyVectorSize)
  }

  /**
   * Same as the primary constructor, but also allow an input frequencyVector to initialize
   * this instance with.
   *
   * @throws IllegalArgumentException if the [frequencyVector] is not the same size as
   * the frequency vector to be built by [this]
   */
  constructor(
    populationSpec: PopulationSpec,
    measurementSpec: MeasurementSpec,
    frequencyVector: FrequencyVector,
    strict: Boolean = true
  ) : this(populationSpec, measurementSpec, strict) {
    require(frequencyVector.dataCount == frequencyData.size)
    {"frequencyVector is of incompatible size: ${frequencyVector.dataCount} " +
     "expected: ${frequencyData.size}" }

    frequencyVector.dataList.forEachIndexed { i, freq ->
      frequencyData[i] = minOf(freq, maxFrequency)}
  }

  /** Build a FrequencyVector. */
  fun build(): FrequencyVector =
    FrequencyVector.newBuilder()!!.addAllData(frequencyData.asList()).build()

  /**
   * Increment the frequency vector for the VID at the given globalIndex.
   *
   * A "globalIndex" is the index of the VID reported by the VidIndexMap. If the globalIndex
   * is not within the VidSamplingInterval provided with the MeasurementSpec then it is
   * ignored.
   *
   * @throws [IllegalArgumentException] if strict is true and globalIndex is out of the
   * range supported by this builder
   */
  fun increment(globalIndex: Int) {
    if (! (globalIndex in primaryRange || globalIndex in wrappedRange)) {
      if (strict) {
        require(globalIndex in primaryRange || globalIndex in wrappedRange)
        { "globalIndex: $globalIndex is out of bounds" }
      } else {
        return
      }
    }

    val localIndex =
    if (globalIndex in primaryRange) {
       globalIndex - primaryRange.first
    } else {
       primaryRange.count() + globalIndex
    }
    frequencyData[localIndex] = minOf(frequencyData[localIndex] + 1, maxFrequency)
  }

  /**
   * Add each globalIndex in the input Collection to the [FrequencyVector] according to the
   * criteria described by [addVid]
   */
  fun incrementAll(globalIndexes: Collection<Int>) {
    globalIndexes.map { increment(it) }
  }

  /**
   * Add all vids in the [other] Builder to the [FrequencyVector].
   *
   * @throws [IllegalArgumentException] if the ranges of [this] and [other] are incompatible.
   */
  fun incrementAll(other: FrequencyVectorBuilder) {
    require(other.primaryRange == primaryRange)
    { "Primary ranges incompatible.  other: ${other.primaryRange} this: ${this.primaryRange}" }
    require(other.wrappedRange == wrappedRange)
    { "Wrapped ranges incompatible.  other: ${other.wrappedRange} this: ${this.wrappedRange}" }

    other.frequencyData.forEachIndexed { i : Int, freq : Int ->
      frequencyData[i] = minOf(freq + frequencyData[i], maxFrequency)}
  }

    companion object {
      fun getPopulationSize(populationSpec: PopulationSpec) : Int {
        var out = 0;
        for (subPop in populationSpec.subpopulationsList) {
          for (range in subPop.vidRangesList) {
            out += (range.startVid..range.endVidInclusive).count()
          }
        }
        return out
      }
    }
}
