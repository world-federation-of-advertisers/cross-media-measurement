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

package org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common

import org.wfanet.frequencycount.FrequencyVector
import org.wfanet.frequencycount.frequencyVector
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidator.validateVidRangesList
import org.wfanet.measurement.computation.KAnonymityParams

/**
 * Get the number of VIDs represented by a PopulationSpec
 *
 * TODO(@kungfucraig): Move this into the package where Population Spec resides
 */
val PopulationSpec.size: Long
  get() =
    subpopulationsList.sumOf { subPop ->
      subPop.vidRangesList.sumOf { (it.startVid..it.endVidInclusive).count().toLong() }
    }

/**
 * A utility for building an appropriately sized [FrequencyVector] for a given [MeasurementSpec] and
 * [PopulationSpec].
 *
 * The [PopulationSpec] is used to determine the overall size of the population being measured. We
 * assume that the client has created an appropriate [VidIndexMap] (or equivalent) for the
 * [PopulationSpec] and that it is being used to determine indexes provided to the [increment] and
 * [incrementAll] methods.
 *
 * The [MeasurementSpec] is used in several ways. First, it must specify either a Reach or
 * ReachAndFrequency measurement. In the case of Reach, the FrequencyVectorBuilder will clamp all
 * frequency values at maximum of 1. In the case of a ReachAndFrequency measurement, the max
 * frequency parameter is used to clamp the frequency value. In both cases the vidSamplingInterval
 * is used to appropriately size the output vector and if the client does not do its own filtering,
 * filter the inputs to [increment] and [incrementAll].
 *
 * @param populationSpec specification of the population being measured
 * @param measurementSpec a [MeasurementSpec] that specifies a Reach or ReachAndFrequency
 * @param strict If false the various increment methods ignore indexes that are out of bounds. If
 *   true, an out of bounds index will result in an exception being thrown. It is normal for indexes
 *   to be out of bounds of a given vid interval.
 * @param kAnonymityParams the kAnonymityParams used for maximimumFrequencyPerUser for reach
 *   calculations.
 * @constructor Create a [FrequencyVectorBuilder]
 */
class FrequencyVectorBuilder(
  val populationSpec: PopulationSpec,
  val measurementSpec: MeasurementSpec,
  val strict: Boolean = true,
  val kAnonymityParams: KAnonymityParams? = null,
) {

  /** The maximum frequency allowed in the output frequency vector. */
  private val maxFrequency: Int

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

    if (measurementSpec.hasReach() && kAnonymityParams != null) {
      require(kAnonymityParams.reachMaxFrequencyPerUser >= 1) {
        "kAnonymityParams.maxFrequencyPerUser must be >= 1 for reach measurements with kAnonymity"
      }
    }

    maxFrequency =
      if (measurementSpec.hasReachAndFrequency()) {
        measurementSpec.reachAndFrequency.maximumFrequency
      } else if (measurementSpec.hasImpression()) {
        measurementSpec.impression.maximumFrequencyPerUser
      } else if (measurementSpec.hasReach()) {
        kAnonymityParams?.reachMaxFrequencyPerUser ?: 1
      } else {
        1
      }
  }

  /**
   * For a non-wrapping sampling interval this is the entire range of indexes in the VidIndexMap
   * that correspond to the sampling interval [start, start+width) For a wrapping sampling interval
   * this is the range of indexes that correspond to the sub-interval [start, 1.0)
   */
  private val primaryRange: IntRange

  private val primaryRangeCount: Int

  /**
   * For a wrapping VidSamplingInterval this is the part of the range than spans [0, width-start)
   */
  private val wrappedRange: IntRange

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

  /** The accumulated frequency data */
  private val frequencyData: IntArray

  val frequencyDataArray: IntArray
    get() = frequencyData

  /** The size of the frequency vector being managed */
  val size: Int
    get() = frequencyData.size

  init {
    // Initialize the frequency vector
    val frequencyVectorSize = primaryRangeCount + wrappedRange.count()
    frequencyData = IntArray(frequencyVectorSize)
  }

  /**
   * Same as the primary constructor, but also allow an input frequencyVector to initialize this
   * instance.
   *
   * @throws IllegalArgumentException if the [frequencyVector] is not the same size as the frequency
   *   vector to be built by [this]
   */
  constructor(
    populationSpec: PopulationSpec,
    measurementSpec: MeasurementSpec,
    frequencyVector: FrequencyVector,
    strict: Boolean = true,
  ) : this(populationSpec, measurementSpec, strict) {
    require(frequencyVector.dataCount == frequencyData.size) {
      "frequencyVector is of incompatible size: ${frequencyVector.dataCount} " +
        "expected: ${frequencyData.size}"
    }

    frequencyVector.dataList.forEachIndexed { i, freq ->
      frequencyData[i] = minOf(freq, maxFrequency)
    }
  }

  /**
   * Constructor that initializes the frequency vector from a ByteArray.
   *
   * Each byte is treated as an unsigned value (0-255) representing the frequency for that VID
   * index. This constructor is more memory-efficient than first converting to IntArray as it avoids
   * intermediate array allocation. It applies VID sampling by directly copying the primary and
   * wrapped ranges in batch operations.
   *
   * @param populationSpec specification of the population being measured
   * @param measurementSpec a [MeasurementSpec] that specifies a Reach or ReachAndFrequency
   * @param frequencyDataBytes byte array where each byte is an unsigned frequency value
   * @param strict If false the constructor ignores indexes that are out of bounds
   * @param kAnonymityParams the kAnonymityParams used for maximumFrequencyPerUser
   * @throws IllegalArgumentException if the frequencyDataBytes size doesn't match population size
   */
  constructor(
    populationSpec: PopulationSpec,
    measurementSpec: MeasurementSpec,
    frequencyDataBytes: ByteArray,
    strict: Boolean = true,
    kAnonymityParams: KAnonymityParams? = null,
  ) : this(populationSpec, measurementSpec, strict, kAnonymityParams) {
    // Batch copy primary range
    var destIndex = 0
    for (sourceIndex in primaryRange) {
      if (sourceIndex < frequencyDataBytes.size) {
        val frequency = frequencyDataBytes[sourceIndex].toInt() and 0xFF
        frequencyData[destIndex] = minOf(frequency, maxFrequency)
      }
      destIndex++
    }

    // Batch copy wrapped range if it exists
    if (!wrappedRange.isEmpty()) {
      for (sourceIndex in wrappedRange) {
        if (sourceIndex < frequencyDataBytes.size) {
          val frequency = frequencyDataBytes[sourceIndex].toInt() and 0xFF
          frequencyData[destIndex] = minOf(frequency, maxFrequency)
        }
        destIndex++
      }
    }
  }

  /** Build a FrequencyVector. */
  fun build(): FrequencyVector = frequencyVector { data += frequencyData.asList() }

  /**
   * Increment the frequency vector for the VID at the given globalIndex.
   *
   * A "globalIndex" is the index of the VID reported by the VidIndexMap. If the globalIndex is not
   * within the VidSamplingInterval provided with the MeasurementSpec then it is ignored if
   * strict=false, otherwise if strict=true an exception is thrown.
   *
   * @throws [IllegalArgumentException] if strict is true and globalIndex is out of the range
   *   supported by this builder
   */
  fun increment(globalIndex: Int) {
    incrementBy(globalIndex, 1)
  }

  /**
   * Increment the frequency vector for the VID at globalIndex by amount.
   *
   * See [increment] for additional information.
   */
  fun incrementBy(globalIndex: Int, amount: Int) {
    require(amount > 0) { "amount must be > 0 got ${amount}" }

    if (!(globalIndex in primaryRange || globalIndex in wrappedRange)) {
      if (strict) {
        require(globalIndex in primaryRange || globalIndex in wrappedRange) {
          "globalIndex: $globalIndex is out of bounds"
        }
      } else {
        return
      }
    }

    val localIndex =
      if (globalIndex in primaryRange) {
        globalIndex - primaryRange.first
      } else {
        primaryRangeCount + globalIndex
      }
    frequencyData[localIndex] = minOf(frequencyData[localIndex] + amount, maxFrequency)
  }

  /**
   * Add each globalIndex in the input Collection to the [FrequencyVector] according to the criteria
   * described by [increment]
   */
  fun incrementAll(globalIndexes: Collection<Int>) {
    globalIndexes.map { incrementBy(it, 1) }
  }

  /**
   * Add each globalIndex in the input Collection to the [FrequencyVector] according to the criteria
   * described by [incrementBy]
   */
  fun incrementAllBy(globalIndexes: Collection<Int>, amount: Int) {
    globalIndexes.map { incrementBy(it, amount) }
  }

  /**
   * Add all values in the [other] builder to the [FrequencyVector].
   *
   * @throws [IllegalArgumentException] if the ranges of [this] and [other] are incompatible.
   */
  fun incrementAll(other: FrequencyVectorBuilder) {
    require(other.primaryRange == primaryRange) {
      "Primary ranges incompatible.  other: ${other.primaryRange} this: ${this.primaryRange}"
    }
    require(other.wrappedRange == wrappedRange) {
      "Wrapped ranges incompatible.  other: ${other.wrappedRange} this: ${this.wrappedRange}"
    }

    other.frequencyData.forEachIndexed { i: Int, freq: Int ->
      frequencyData[i] = minOf(freq + frequencyData[i], maxFrequency)
    }
  }

  companion object {
    /** Allow for DSL syntax when building a FrequencyVector. */
    fun build(
      populationSpec: PopulationSpec,
      measurementSpec: MeasurementSpec,
      bind: FrequencyVectorBuilder.() -> Unit,
    ): FrequencyVector = FrequencyVectorBuilder(populationSpec, measurementSpec).apply(bind).build()

    /** Allow for DSL syntax when building a FrequencyVector. */
    fun build(
      populationSpec: PopulationSpec,
      measurementSpec: MeasurementSpec,
      frequencyVector: FrequencyVector,
      bind: FrequencyVectorBuilder.() -> Unit,
    ): FrequencyVector =
      FrequencyVectorBuilder(populationSpec, measurementSpec, frequencyVector).apply(bind).build()
  }
}
