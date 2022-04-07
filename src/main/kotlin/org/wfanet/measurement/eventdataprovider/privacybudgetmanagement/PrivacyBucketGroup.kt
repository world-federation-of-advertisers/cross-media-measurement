/**
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * ```
 *      http://www.apache.org/licenses/LICENSE-2.0
 * ```
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
  ABOVE_54("55+")
}

enum class Gender(val string: String) {
  MALE("M"),
  FEMALE("F")
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

  fun privacyBucketsOverlap(
    bucketGroup1: PrivacyBucketGroup,
    bucketGroup2: PrivacyBucketGroup,
  ): Boolean {
    if (bucketGroup1.measurementConsumerId != bucketGroup2.measurementConsumerId) {
      return false
    }
    if (bucketGroup2.endingDate.isBefore(bucketGroup1.startingDate) ||
        bucketGroup1.endingDate.isBefore(bucketGroup2.startingDate)
    ) {
      return false
    }
    if (bucketGroup1.ageGroup != bucketGroup2.ageGroup) {
      return false
    }
    if (bucketGroup1.gender != bucketGroup2.gender) {
      return false
    }

    // Prior wrap-around logic is removed
    val vidSampleEnd1 = bucketGroup1.vidSampleStart + bucketGroup1.vidSampleWidth
    val vidSampleEnd2 = bucketGroup2.vidSampleStart + bucketGroup2.vidSampleWidth

    return (bucketGroup1.vidSampleStart <= vidSampleEnd2) &&
      (bucketGroup2.vidSampleStart <= vidSampleEnd1)
  }
}
