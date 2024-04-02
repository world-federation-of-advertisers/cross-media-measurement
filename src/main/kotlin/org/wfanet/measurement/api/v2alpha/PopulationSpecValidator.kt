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

import okhttp3.internal.toImmutableList
import org.wfanet.measurement.api.v2alpha.PopulationSpec.VidRange

  VID_RANGES_NOT_DISJOINT_ERROR("The ranges of the PopulationSpec are not disjoint."),
  VID_RANGE_("The ranges of the PopulationSpec are not disjoint."),
}

class ValidationException(message: String, val errors: List<ValidationError>) : Exception(message);

abstract class ValidationError;

/**
 * Indicates that the [VidRange] with the given index is not disjoint with the
 * set of disjoint ranges that came before it.
 */
class VidRangesNotDisjointError(index: Int) : ValidationError() {}

/**
 * Indicates that the [VidRange.startVid] of the [VidRange] at the given
 * index is less than or equal to zero.
 */
class StartVidLessThanOrEqualToZero(index: Int) : ValidationError() {}

/**
 * Indicates that the [VidRange.startVid] of the [VidRange] at the given
 * index is less than or equal to zero.
 */
class EndVidInclusiveLessThanVidStart(index: Int) : ValidationError() {}

class PopulationSpecValidator {

  /**
   * Validates the [VidRange]s of a PopulationSpec.
   *
   * Ensure that each [VidRange] is valid by calling [validateVidRange] and
   * ensure that collectively the [VidRange]s are disjoint.
   *
   * @return [true] if the ranges in the PopulationSpec are valid.
   * @throws
   */
  fun validateVidRanges(populationSpec: PopulationSpec) : List<ValidationError> {
    val validationErrors = mutableListOf<ValidationError>()

    for
    return false
  }

  /**
   * Returns an empty [List<ValidationError>] if the range is valid. Otherwise,
   * the list contains the reason(s) for why the range is invalid.
   */
  fun validateRange(vidRange: VidRange, vidIndex: Int) : List<ValidationError> {
    val validationErrors = mutableListOf<ValidationError>()
    if (vidRange.startVid <= 0) {
      validationErrors.add(StartVidLessThanOrEqualToZero(vidIndex)) }
    if (vidRange.endVidInclusive < vidRange.startVid)  {
      validationErrors.add(EndVidInclusiveLessThanVidStart(vidIndex))
    }
    return validationErrors.toImmutableList();
  }
}
