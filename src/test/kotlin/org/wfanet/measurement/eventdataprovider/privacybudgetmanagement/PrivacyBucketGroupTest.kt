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

import com.google.common.truth.Truth.assertThat
import java.time.LocalDate
import kotlin.test.assertFails
import org.junit.Test

class PrivacyBucketGroupTest {
  companion object {
    private val bucketGroup =
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        0.2f
      )
  }

  @Test
  fun `PrivacyBucketGroup throws exception if start + width larger than 1`() {
    assertFails {
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.8f,
        0.5f
      )
    }
  }

  @Test
  fun `overlapsWith overlapping works as expected`() {
    val bucketGroup2 =
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        0.1f
      )

    assertThat(bucketGroup.overlapsWith(bucketGroup2)).isTrue()
  }

  @Test
  fun `overlapsWith non overlapping works as expected`() {
    val bucketGroup2 =
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.6f,
        0.2f
      )

    assertThat(bucketGroup.overlapsWith(bucketGroup2)).isFalse()
  }

  @Test
  fun `overlapsWith returns false for two neighboring, non-overlapping, bucket groups`() {
    val bucketGroup2 =
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.5f,
        0.1f
      )

    assertThat(bucketGroup.overlapsWith(bucketGroup2)).isFalse()
  }
}
