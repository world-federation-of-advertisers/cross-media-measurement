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
import org.wfanet.measurement.computation.ResultMinimumThresholds

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
 * The [MeasurementSpec] is used in several ways. First, it must specify a Reach, ReachAndFrequency,
 * or Multi measurement. In the case of Reach, the FrequencyVectorBuilder will clamp all frequency
 * values at maximum of 1. In the case of a ReachAndFrequency measurement, the max frequency
 * parameter is used to clamp the frequency value. A Multi measurement carries no frequency cap of
 * its own, so values saturate at the largest signed 8-bit value the wire format holds. In every
 * case the vidSamplingInterval is used to appropriately size the output vector and if the client
 * does not do its own filtering, filter the inputs to [increment] and [incrementAll].
 *
 * @param populationSpec specification of the population being measured
 * @param measurementSpec a [MeasurementSpec] that specifies a Reach, ReachAndFrequency, or Multi
 *   measurement
 * @param strict If false the various increment methods ignore indexes that are out of bounds. If
 *   true, an out of bounds index will result in an exception being thrown. It is normal for indexes
 *   to be out of bounds of a given vid interval.
 * @param resultMinimumThresholds the resultMinimumThresholds used for maximimumFrequencyPerUser for
 *   reach calculations.
 * @constructor Create a [FrequencyVectorBuilder]
 */
class FrequencyVectorBuilder(
  val populationSpec: PopulationSpec,
  val measurementSpec: MeasurementSpec,
  val strict: Boolean = true,
  val resultMinimumThresholds: ResultMinimumThresholds? = null,
  overrideImpressionMaxFrequencyPerUser: Int?,
) {

  private val sampling =
    FrequencyVectorSampling(
      populationSpec,
      measurementSpec,
      resultMinimumThresholds,
      overrideImpressionMaxFrequencyPerUser,
    )

  private val maxFrequency: Int
    get() = sampling.maxFrequency

  private val primaryRange: IntRange
    get() = sampling.primaryRange

  private val primaryRangeCount: Int
    get() = sampling.primaryRangeCount

  private val wrappedRange: IntRange
    get() = sampling.wrappedRange

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
    overrideImpressionMaxFrequencyPerUser: Int?,
    strict: Boolean = true,
    resultMinimumThresholds: ResultMinimumThresholds? = null,
  ) : this(
    populationSpec,
    measurementSpec,
    strict,
    resultMinimumThresholds,
    overrideImpressionMaxFrequencyPerUser,
  ) {
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
   * @param resultMinimumThresholds the resultMinimumThresholds used for maximumFrequencyPerUser
   * @throws IllegalArgumentException if the frequencyDataBytes size doesn't match population size
   */
  constructor(
    populationSpec: PopulationSpec,
    measurementSpec: MeasurementSpec,
    frequencyDataBytes: ByteArray,
    overrideImpressionMaxFrequencyPerUser: Int?,
    strict: Boolean = true,
    resultMinimumThresholds: ResultMinimumThresholds? = null,
  ) : this(
    populationSpec,
    measurementSpec,
    strict,
    resultMinimumThresholds,
    overrideImpressionMaxFrequencyPerUser,
  ) {
    for (index in frequencyData.indices) {
      frequencyData[index] = sampling.frequencyAt(frequencyDataBytes, index)
    }
  }

  /** Builds a byte payload, rejecting frequencies that cannot be represented as unsigned bytes. */
  fun buildBytes(): ByteArray =
    ByteArray(size) { index ->
      val frequency = frequencyData[index]
      require(frequency in 0..255) { "FrequencyVector value must be between 0 and 255" }
      frequency.toByte()
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
      overrideImpressionMaxFrequencyPerUser: Int? = null,
      bind: FrequencyVectorBuilder.() -> Unit,
    ): FrequencyVector =
      FrequencyVectorBuilder(
          populationSpec,
          measurementSpec,
          overrideImpressionMaxFrequencyPerUser = overrideImpressionMaxFrequencyPerUser,
        )
        .apply(bind)
        .build()

    /** Allow for DSL syntax when building a FrequencyVector. */
    fun build(
      populationSpec: PopulationSpec,
      measurementSpec: MeasurementSpec,
      frequencyVector: FrequencyVector,
      overrideImpressionMaxFrequencyPerUser: Int? = null,
      bind: FrequencyVectorBuilder.() -> Unit,
    ): FrequencyVector =
      FrequencyVectorBuilder(
          populationSpec,
          measurementSpec,
          frequencyVector,
          overrideImpressionMaxFrequencyPerUser = overrideImpressionMaxFrequencyPerUser,
        )
        .apply(bind)
        .build()
  }
}
