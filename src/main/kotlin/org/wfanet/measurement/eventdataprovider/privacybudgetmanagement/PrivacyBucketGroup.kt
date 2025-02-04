/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import java.time.LocalDate

enum class AgeGroup(val string: String) {
  RANGE_18_34("18_34"),
  RANGE_35_54("35_54"),
  ABOVE_54("55+"),
}

enum class Gender(val string: String) {
  MALE("M"),
  FEMALE("F"),
}

/** A set of users whose privacy budget usage is being tracked as a unit. */
data class PrivacyBucketGroup(
  val measurementConsumerId: String,
  val startingDate: LocalDate,
  val endingDate: LocalDate,
  val ageGroup: AgeGroup,
  val gender: Gender,
  val vidSampleStart: Float,
  val vidSampleWidth: Float,
) {
  init {
    if (this.vidSampleStart + this.vidSampleWidth > 1) {
      throw Exception("start + width cannot be larger than 1")
    }
  }

  fun overlapsWith(otherBucketGroup: PrivacyBucketGroup): Boolean {
    if (this.measurementConsumerId != otherBucketGroup.measurementConsumerId) {
      return false
    }
    if (
      otherBucketGroup.endingDate.isBefore(this.startingDate) ||
        this.endingDate.isBefore(otherBucketGroup.startingDate)
    ) {
      return false
    }
    if (this.ageGroup != otherBucketGroup.ageGroup) {
      return false
    }
    if (this.gender != otherBucketGroup.gender) {
      return false
    }

    val thisVidSampleEnd = this.vidSampleStart + this.vidSampleWidth
    val otherVidSampleEnd = otherBucketGroup.vidSampleStart + otherBucketGroup.vidSampleWidth

    // Vid ranges are half-open intervals. [0.1, 0.2) does not overlap with vid[0.2, 0.3)
    return this.vidSampleStart < otherVidSampleEnd &&
      otherBucketGroup.vidSampleStart < thisVidSampleEnd
  }
}
