// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.virtualpeople.core.model

import com.google.common.hash.Hashing
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Descriptors.FieldDescriptor.Type
import java.nio.charset.StandardCharsets
import kotlin.math.floor
import org.wfanet.virtualpeople.common.LabelerEvent
import org.wfanet.virtualpeople.common.LabelerEventOrBuilder
import org.wfanet.virtualpeople.common.Multiplicity
import org.wfanet.virtualpeople.common.Multiplicity.MultiplicityRefCase
import org.wfanet.virtualpeople.common.fieldfilter.utils.getFieldFromProto
import org.wfanet.virtualpeople.common.fieldfilter.utils.getValueFromProto

class MultiplicityImpl
/**
 * @param multiplicityExtractor extractor for expected multiplicity. Either a [Double] or a
 * [MultiplicityFromField].
 * @param capAtMax whether to cap multiplicity at [maxValue]
 * @param maxValue when expected multiplicity > [maxValue], cap at [maxValue] if [capAtMax] is YES,
 * else throw an error.
 * @param personIndexField the field to set person index in.
 * @param randomSeed the random seed. It is used to compute multiplicity for a given event and
 * compute fingerprint for cloned event
 */
private constructor(
  private val multiplicityExtractor: Any,
  private val capAtMax: CapMultiplicityAtMax,
  private val maxValue: Double,
  private val personIndexField: List<FieldDescriptor>,
  private val randomSeed: String
) {

  /**
   * Computes multiplicity for [eventOrBuilder].
   * 1. Extracts the expected_multiplicity. Throws an error if [capAtMax] = NO and
   * expected_multiplicity > [maxValue].
   * 2. Pseudo-randomly generates an integer value that is either floor(expected_multiplicity) or
   * floor(expected_multiplicity) + 1, with expectation = expected_multiplicity. For example, with
   * expected_multiplicity = 1.4, this returns either 1 or 2, with 60% and 40% probabilities,
   * respectively. This always returns the same result for the same event.
   */
  fun computeEventMultiplicity(eventOrBuilder: LabelerEventOrBuilder): Int {
    var expectedMultiplicity =
      when (multiplicityExtractor) {
        is Double -> multiplicityExtractor
        is MultiplicityFromField -> {
          if (multiplicityExtractor.fieldDescriptors.isEmpty()) {
            error("Extractor has invalid field_descriptor.")
          }
          multiplicityExtractor.getValueFunction(
            eventOrBuilder,
            multiplicityExtractor.fieldDescriptors
          )
        }
        else -> error("Invalid multiplicity extractor.")
      }
    if (expectedMultiplicity > maxValue) {
      if (capAtMax == CapMultiplicityAtMax.YES) {
        expectedMultiplicity = maxValue
      } else {
        error(
          "Expected multiplicity = $expectedMultiplicity," +
            ", which exceeds the specified max value = $maxValue"
        )
      }
    }
    if (expectedMultiplicity < 0) {
      error("Expected multiplicity = $expectedMultiplicity, but multiplicity must >= 0.")
    }

    /**
     * The seed uses the string representation of actingFingerprint as an unsigned 64-bit integer
     */
    val eventSeed =
      Hashing.farmHashFingerprint64()
        .hashString(
          "$randomSeed${eventOrBuilder.actingFingerprint.toULong()}",
          StandardCharsets.UTF_8
        )
        .asLong()
        .toULong()
    return computeBimodalInteger(expectedMultiplicity, eventSeed)
  }

  /** Gets the person_index field descriptor. */
  fun personIndexFieldDescriptor(): List<FieldDescriptor> {
    return personIndexField
  }

  /**
   * Gets fingerprint for [index] using [input] and [randomSeed].
   *
   * Returns [input] as is for [index] = 0.
   */
  fun getFingerprintForIndex(input: ULong, index: Int): ULong {
    if (index == 0) {
      return input
    }
    return Hashing.farmHashFingerprint64()
      .hashString("$randomSeed-clone-$index-$input", StandardCharsets.UTF_8)
      .asLong()
      .toULong()
  }

  companion object {
    private inline fun <reified T> extractValue(
      eventOrBuilder: LabelerEventOrBuilder,
      source: List<FieldDescriptor>
    ): T {
      val fieldValue = getValueFromProto<T>(eventOrBuilder, source)
      if (fieldValue.isSet) {
        return fieldValue.value
      } else {
        error("The multiplicity field is not set.")
      }
    }

    /**
     * Returns an integer that is either N or N + 1, where N is the integral part of [expectation].
     *
     * The choice is made by comparing [seed] to the fractional part of [expectation]. This is used
     * to compute multiplicity, and [expectation] should be much lower than int max.
     */
    private fun computeBimodalInteger(expectation: Double, seed: ULong): Int {
      val integralPart: Int = floor(expectation).toInt()
      val fractionalPart: Double = expectation - integralPart
      val threshold: ULong = (fractionalPart * ULong.MAX_VALUE.toDouble()).toULong()
      return if (seed < threshold) integralPart + 1 else integralPart
    }

    /** Gets a function to extract multiplicity value from a field. */
    private fun getExtractMultiplicityFunction(
      type: Type
    ): (LabelerEventOrBuilder, List<FieldDescriptor>) -> Double {
      return when (type) {
        Type.INT32 -> { a, b -> extractValue<Int>(a, b).toDouble() }
        Type.UINT32 -> { a, b -> extractValue<UInt>(a, b).toDouble() }
        Type.INT64 -> { a, b -> extractValue<Long>(a, b).toDouble() }
        Type.UINT64 -> { a, b -> extractValue<ULong>(a, b).toDouble() }
        Type.FLOAT -> { a, b -> extractValue<Float>(a, b).toDouble() }
        Type.DOUBLE -> { a, b -> extractValue(a, b) }
        else -> error("Unsupported field type for multiplicity: $type")
      }
    }

    private fun isIntegerFieldType(type: Type): Boolean {
      return type in listOf(Type.INT32, Type.UINT32, Type.INT64, Type.UINT64)
    }

    /**
     * Always use build to get an [MultiplicityImpl] object. Users should not call the constructor
     * directly.
     *
     * Throws an error if any of the following happens:
     * 1. [config].multiplicity_ref is not set.
     * 2. [config].expected_multiplicity_field is set, but is not a valid field, or the field type
     * is not one of int32/int64/uint32/uint64/float/double.
     * 3. [config].person_index_field is not set, or is not a valid field, or the field type is not
     * one of int32/int64/uint32/uint64.
     * 4. [config].max_value is not set.
     * 5. [config].cap_at_max is not set.
     * 6. [config].random_seed is not set.
     */
    fun build(config: Multiplicity): MultiplicityImpl {
      if (!config.hasPersonIndexField()) {
        error("Multiplicity must set person_index_field. $config")
      }
      if (!config.hasMaxValue()) {
        error("Multiplicity must set max_value. $config")
      }
      if (!config.hasCapAtMax()) {
        error("Multiplicity must set cap_at_max. $config")
      }
      if (!config.hasRandomSeed()) {
        error("Multiplicity must set random_seed. $config")
      }

      /** Only [Double] or [MultiplicityFromField] is valid type for [multiplicityExtractor]. */
      val multiplicityExtractor: Any =
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
        when (config.multiplicityRefCase) {
          MultiplicityRefCase.EXPECTED_MULTIPLICITY -> config.expectedMultiplicity
          MultiplicityRefCase.EXPECTED_MULTIPLICITY_FIELD -> {
            val fieldDescriptors =
              getFieldFromProto(LabelerEvent.getDescriptor(), config.expectedMultiplicityField)
            MultiplicityFromField(
              fieldDescriptors,
              getExtractMultiplicityFunction(fieldDescriptors.last().type)
            )
          }
          MultiplicityRefCase.MULTIPLICITYREF_NOT_SET ->
            error("Multiplicity must set multiplicity_ref. $config")
        }

      val personIndexField =
        getFieldFromProto(LabelerEvent.getDescriptor(), config.personIndexField)
      if (!isIntegerFieldType(personIndexField.last().type)) {
        error("Invalid type for person_index_field. $config")
      }
      val capAtMax = if (config.capAtMax) CapMultiplicityAtMax.YES else CapMultiplicityAtMax.NO

      return MultiplicityImpl(
        multiplicityExtractor,
        capAtMax,
        config.maxValue,
        personIndexField,
        config.randomSeed
      )
    }
  }
}

enum class CapMultiplicityAtMax {
  NO,
  YES
}

/** Extracts multiplicity value from a field. */
data class MultiplicityFromField(
  /** The field descriptor. */
  val fieldDescriptors: List<FieldDescriptor>,
  /** The function to extract the multiplicity value. */
  val getValueFunction: (LabelerEventOrBuilder, List<FieldDescriptor>) -> Double
)
