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

package org.wfanet.measurement.api.v2alpha

import java.util.*
import org.wfanet.measurement.api.v2alpha.PopulationSpec.VidRange

/** An exception that encapsulates a list of validation errors. */
class PopulationSpecValidationException(
  message: String,
  val errors: List<PopulationSpecValidationError>,
) : Exception(message) {
  override fun toString(): String {
    val message = StringBuilder()
    message.append(super.toString() + "\n")
    for (error in errors) {
      message.append("  $error\n")
    }
    return message.toString()
  }
}

/** Base class for the errors reported by the [PopulationSpecValidator] */
abstract class PopulationSpecValidationError

/**
 * Indicates that a pair of [VidRange]s are not disjoint.
 *
 * @param [firstIndexMessage] describe the index of the first range in a [PopulationSpec]
 * @param [secondIndexMessage] describe the index of the second range in a [PopulationSpec]
 */
data class VidRangesNotDisjointError(
  val firstIndexMessage: String,
  val secondIndexMessage: String,
) : PopulationSpecValidationError() {
  override fun toString(): String {
    return "The ranges at '$firstIndexMessage' and '$secondIndexMessage' must be disjoint."
  }
}

/**
 * Indicates that the [VidRange.startVid] of the [VidRange] described by indexMessage is less than
 * or equal to zero.
 */
data class StartVidLessThanOrEqualToZeroError(val indexMessage: String) :
  PopulationSpecValidationError() {
  override fun toString(): String {
    return "The startVid of the range at '$indexMessage' must be greater than zero."
  }
}

/**
 * Indicates that the [VidRange.startVid] of the [VidRange] described by the indexMessage is less
 * than or equal to zero.
 */
data class EndVidInclusiveLessThanVidStartError(val indexMessage: String) :
  PopulationSpecValidationError() {
  override fun toString(): String {
    return "The endVidInclusive of the range at '$indexMessage' must be greater than or equal to the startVid."
  }
}

class PopulationSpecValidator(val populationSpec: PopulationSpec) {
  /**
   * The list of errors accumulated from making calls to [this] validator. This list is never reset.
   */
  val validationErrors: List<PopulationSpecValidationError>
    get() = Collections.unmodifiableList(validationErrorsInternal)

  private val validationErrorsInternal = mutableListOf<PopulationSpecValidationError>()

  companion object {
    /**
     * Validate the vidRangesList according to the criteria specified by [isVidRangesListValid]
     *
     * @throws [PopulationSpecValidationException] if the vidRangeList is not valid.
     */
    fun validateVidRangesList(populationSpec: PopulationSpec) {
      val populationSpecValidator = PopulationSpecValidator(populationSpec)
      if (!populationSpecValidator.isVidRangesListValid()) {
        throw PopulationSpecValidationException(
          "Invalid PopulationSpec.",
          populationSpecValidator.validationErrors,
        )
      }
    }
  }

  /**
   * Validates the [VidRange]s of the PopulationSpec.
   *
   * Ensure that each [VidRange] is valid by calling [validateVidRange] and ensure that collectively
   * the [VidRange]s are disjoint.
   *
   * Any [PopulationSpecValidationError]s are appended to [validationErrors].
   *
   * @return [true] if the ranges in the [PopulationSpec] are valid.
   * @throws
   */
  fun isVidRangesListValid(): Boolean {
    val errorCount = validationErrorsInternal.size
    val validVidRanges = mutableListOf<Pair<VidRange, String>>()

    // Validate ranges individually and make a list of the valid ones including an
    // index message. This is required both in and of itself and for checking disjointness.
    for ((subpopulationIndex, subpopulation) in populationSpec.subpopulationsList.withIndex()) {
      for ((vidRangeIndex, vidRange) in subpopulation.vidRangesList.withIndex()) {
        val indexMessage = "SubpopulationIndex: $subpopulationIndex VidRangeIndex: $vidRangeIndex"
        if (isVidRangeValid(vidRange, indexMessage)) {
          validVidRanges.add(Pair(vidRange, indexMessage))
        }
      }
    }

    // Validate disjointness of the valid ranges.
    validVidRanges.sortBy { it.first.startVid }
    if (validVidRanges.size > 1) {
      var (previousVidRange, previousIndexMessage) = validVidRanges[0]
      for ((currentVidRange, currentIndexMessage) in
        validVidRanges.slice(1..validVidRanges.lastIndex)) {
        if (previousVidRange.endVidInclusive >= currentVidRange.startVid) {
          validationErrorsInternal.add(
            VidRangesNotDisjointError(previousIndexMessage, currentIndexMessage)
          )
        }
        previousVidRange = currentVidRange
        previousIndexMessage = currentIndexMessage
      }
    }
    return errorCount == validationErrorsInternal.size
  }

  /**
   * Returns true if the range is valid.
   *
   * Any [PopulationSpecValidationError]s are appended to [validationErrors].
   *
   * @param [vidRange] is the range to validate
   * @param [indexMessage] is the message included in any validation error
   */
  private fun isVidRangeValid(vidRange: VidRange, indexMessage: String = ""): Boolean {
    val errorCount = validationErrorsInternal.size
    if (vidRange.startVid <= 0) {
      validationErrorsInternal.add(StartVidLessThanOrEqualToZeroError(indexMessage))
    }
    if (vidRange.endVidInclusive < vidRange.startVid) {
      validationErrorsInternal.add(EndVidInclusiveLessThanVidStartError(indexMessage))
    }
    return errorCount == validationErrorsInternal.size
  }
}
