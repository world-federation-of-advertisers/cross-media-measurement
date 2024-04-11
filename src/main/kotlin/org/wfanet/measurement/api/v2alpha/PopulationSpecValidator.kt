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

import org.wfanet.measurement.api.v2alpha.PopulationSpec.VidRange

/** An exception that encapsulates a list of validation errors. */
class PopulationSpecValidationException(message: String, val details: List<Detail>) :
  Exception(message) {
  override fun toString(): String {
    return buildString {
      appendLine(super.toString())
      for (detail in details) {
        appendLine("  $detail")
      }
    }
  }

  /** A common interface for the set of Details associated with this exception */
  interface Detail

  /**
   * Indicates that a pair of [VidRange]s are not disjoint.
   *
   * @param [firstIndexMessage] describe the index of the first range in a [PopulationSpec]
   * @param [secondIndexMessage] describe the index of the second range in a [PopulationSpec]
   */
  data class VidRangesNotDisjointDetail(
    val firstIndexMessage: String,
    val secondIndexMessage: String,
  ) : Detail {
    override fun toString(): String {
      return "The ranges at '$firstIndexMessage' and '$secondIndexMessage' must be disjoint."
    }
  }

  /**
   * Indicates that the [VidRange.startVid] described by indexMessage is less than or equal to zero.
   */
  data class StartVidNotPositiveDetail(val indexMessage: String) : Detail {
    override fun toString(): String {
      return "The startVid of the range at '$indexMessage' must be greater than zero."
    }
  }

  /**
   * Indicates that [VidRange.endVidInclusive] is less than [VidRange.startVid] for the [VidRange]
   * described by the indexMessage.
   */
  data class EndVidInclusiveLessThanVidStartDetail(val indexMessage: String) : Detail {
    override fun toString(): String {
      return "The endVidInclusive of the range at '$indexMessage' must be greater than or equal to the startVid."
    }
  }
}

object PopulationSpecValidator {
  /**
   * Validates the [VidRange]s of the PopulationSpec.
   *
   * Ensure that each [VidRange] is valid by calling [validateVidRange] and ensure that collectively
   * the [VidRange]s are disjoint.
   *
   * @return Result<Boolean> that is true upon success or contains a
   *   [PopulationSpecValidationException] on failure.
   */
  fun validateVidRangesList(populationSpec: PopulationSpec): Result<Boolean> {
    val details = mutableListOf<PopulationSpecValidationException.Detail>()
    val validVidRanges = mutableListOf<Pair<VidRange, String>>()

    // Validate ranges individually and make a list of the valid ones including an
    // index message. Invalid ranges are omitted from the disjointness check.
    for ((subpopulationIndex, subpopulation) in populationSpec.subpopulationsList.withIndex()) {
      for ((vidRangeIndex, vidRange) in subpopulation.vidRangesList.withIndex()) {
        val indexMessage = "SubpopulationIndex: $subpopulationIndex VidRangeIndex: $vidRangeIndex"
        val validationDetails = validateVidRange(vidRange, indexMessage)
        details.addAll(validationDetails)
        if (validationDetails.isEmpty()) {
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
          details.add(
            PopulationSpecValidationException.VidRangesNotDisjointDetail(
              previousIndexMessage,
              currentIndexMessage,
            )
          )
        }
        previousVidRange = currentVidRange
        previousIndexMessage = currentIndexMessage
      }
    }
    return when (details.isEmpty()) {
      true -> Result.success(true)
      false ->
        Result.failure(PopulationSpecValidationException("Invalid Population Spec.", details))
    }
  }

  /**
   * Validate a [VidRange]
   *
   * @param [vidRange] is the range to validate
   * @return If invalid, a non-empty [List<Error>], or an empty list if valid.
   */
  private fun validateVidRange(
    vidRange: VidRange,
    indexMessage: String = "",
  ): List<PopulationSpecValidationException.Detail> {
    val details = mutableListOf<PopulationSpecValidationException.Detail>()
    if (vidRange.startVid <= 0) {
      details.add(PopulationSpecValidationException.StartVidNotPositiveDetail(indexMessage))
    }
    if (vidRange.endVidInclusive < vidRange.startVid) {
      details.add(
        PopulationSpecValidationException.EndVidInclusiveLessThanVidStartDetail(indexMessage)
      )
    }
    return details
  }
}
