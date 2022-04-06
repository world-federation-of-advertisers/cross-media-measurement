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
import kotlin.test.assertEquals
import org.junit.Test

class PrivacyBucketGroupTest {
  @Test
  fun `privacyBucketsOverlap works as expected`() {
    val bucket1 =
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        0.1f
      )

    val bucket2 =
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.5f,
        0.4f
      )

    val bucket3 =
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.5f,
        0.1f
      )

    assertEquals(bucket1.privacyBucketsOverlap(bucket1, bucket2), false)
    assertEquals(bucket1.privacyBucketsOverlap(bucket2, bucket3), true)
  }
}
