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
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.TestPrivacyBucketMapper

private const val MEASUREMENT_CONSUMER_ID = "ACME"

@RunWith(JUnit4::class)
class PrivacyBucketFilterTest {

  private val privacyBucketFilter = PrivacyBucketFilter(TestPrivacyBucketMapper())
  private val today: LocalDateTime = LocalDate.now().atTime(4, 20)
  private val yesterday: LocalDateTime = today.minusDays(1)
  private val startOfTomorrow: LocalDateTime = today.plusDays(1).toLocalDate().atStartOfDay()
  private val timeRange =
    OpenEndTimeRange(yesterday.toInstant(ZoneOffset.UTC), startOfTomorrow.toInstant(ZoneOffset.UTC))

  @Test
  fun `Mapper fails for invalid filter expression`() {
    val privacyLandscapeMask =
      LandscapeMask(
        listOf(EventGroupSpec("person.age_group.value", timeRange)),
        0.0f,
        PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
      )

    assertFailsWith<PrivacyBudgetManagerException> {
      privacyBucketFilter.getPrivacyBucketGroups(MEASUREMENT_CONSUMER_ID, privacyLandscapeMask)
    }
  }

  @Test
  fun `Filter succeeds for filter expression with only privacy budget Fields`() {
    val privacyLandscapeMask =
      LandscapeMask(
        listOf(
          EventGroupSpec("person.age_group.value in [1] && person.gender.value == 2", timeRange)
        ),
        0.0f,
        PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
      )

    assertThat(
        privacyBucketFilter.getPrivacyBucketGroups(MEASUREMENT_CONSUMER_ID, privacyLandscapeMask)
      )
      .containsExactly(
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          today.toLocalDate(),
          today.toLocalDate(),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          0.0f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        ),
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          yesterday.toLocalDate(),
          yesterday.toLocalDate(),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          0.0f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        ),
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          today.toLocalDate(),
          today.toLocalDate(),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        ),
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          yesterday.toLocalDate(),
          yesterday.toLocalDate(),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        ),
      )
  }

  @Test
  fun `Mapper succeeds with privacy budget Field and non Privacy budget fields`() {
    val privacyLandscapeMask =
      LandscapeMask(
        listOf(
          EventGroupSpec(
            "person.age_group.value in [1] && person.gender.value == 2 && " +
              "banner_ad.viewable.value == true",
            timeRange
          )
        ),
        0.0f,
        PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
      )

    assertThat(
        privacyBucketFilter.getPrivacyBucketGroups(MEASUREMENT_CONSUMER_ID, privacyLandscapeMask)
      )
      .containsExactly(
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          today.toLocalDate(),
          today.toLocalDate(),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          0.0f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        ),
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          yesterday.toLocalDate(),
          yesterday.toLocalDate(),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          0.0f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        ),
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          today.toLocalDate(),
          today.toLocalDate(),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        ),
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          yesterday.toLocalDate(),
          yesterday.toLocalDate(),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        ),
      )
  }

  @Test
  fun `Mapper succeeds with left out privacy budget Fields`() {
    val privacyLandscapeMask =
      LandscapeMask(
        listOf(EventGroupSpec("person.age_group.value in [1] ", timeRange)),
        0.0f,
        PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
      )

    assertThat(
        privacyBucketFilter.getPrivacyBucketGroups(MEASUREMENT_CONSUMER_ID, privacyLandscapeMask)
      )
      .containsExactly(
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          today.toLocalDate(),
          today.toLocalDate(),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          0.0f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        ),
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          yesterday.toLocalDate(),
          yesterday.toLocalDate(),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          0.0f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        ),
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          today.toLocalDate(),
          today.toLocalDate(),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        ),
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          yesterday.toLocalDate(),
          yesterday.toLocalDate(),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        ),
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          today.toLocalDate(),
          today.toLocalDate(),
          AgeGroup.RANGE_18_34,
          Gender.MALE,
          0.0f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        ),
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          yesterday.toLocalDate(),
          yesterday.toLocalDate(),
          AgeGroup.RANGE_18_34,
          Gender.MALE,
          0.0f,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        ),
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          today.toLocalDate(),
          today.toLocalDate(),
          AgeGroup.RANGE_18_34,
          Gender.MALE,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        ),
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          yesterday.toLocalDate(),
          yesterday.toLocalDate(),
          AgeGroup.RANGE_18_34,
          Gender.MALE,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH,
          PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        ),
      )
  }

  @Test
  fun `Non Privacy Budget Fields may charge more buckets than necessary`() {
    val privacyLandscapeMask =
      LandscapeMask(
        listOf(
          EventGroupSpec(
            "person.age_group.value in [0] && person.gender.value == 1 || " +
              "banner_ad.viewable.value == true",
            timeRange
          )
        ),
        0.0f,
        PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
      )

    assertThat(
        privacyBucketFilter.getPrivacyBucketGroups(MEASUREMENT_CONSUMER_ID, privacyLandscapeMask)
      )
      .hasSize(24)
  }
}
