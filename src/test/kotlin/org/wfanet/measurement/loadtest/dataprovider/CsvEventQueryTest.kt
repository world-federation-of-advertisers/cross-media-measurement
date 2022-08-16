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
package org.wfanet.measurement.loadtest.dataprovider

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplate.Gender.Value as BannerGender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.AgeRange.Value as PrivacyAge
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.Gender.Value as PrivacyGender

private val BANNER_FEMALE = BannerGender.GENDER_FEMALE.ordinal
private val PRIVACY_35_54 = PrivacyAge.AGE_35_TO_54.ordinal
private val PRIVACY_MALE = PrivacyGender.GENDER_MALE.ordinal

private val MATCHING_EVENT_FILTER = eventFilter {
  expression =
    "privacy_budget.age.value == $PRIVACY_35_54 || banner_ad.gender.value == $BANNER_FEMALE"
}
private val NONMATCHING_EVENT_FILTER = eventFilter {
  expression =
    "privacy_budget.gender.value == $PRIVACY_MALE && banner_ad.gender.value == $BANNER_FEMALE"
}

private val CSV_HEADER =
  listOf("Publisher_ID", "Event_ID", "Sex", "Age_Group", "Social_Grade", "Date", "Complete", "VID")

private val CSV_EVENTS_LIST =
  listOf(
    listOf(1, 1, "M", "18_34", "ABC1", "04/03/2021", 1, 1000077),
    listOf(1, 2, "M", "35_54", "ABC1", "04/03/2021", 1, 1000650),
    listOf(1, 3, "M", "55+", "ABC1", "04/03/2021", 1, 1000694),
    listOf(1, 4, "M", "55+", "C2DE", "04/03/2021", 1, 1000759),
    listOf(1, 5, "M", "55+", "C2DE", "04/03/2021", 1, 1000759),
    listOf(1, 6, "F", "18_34", "ABC1", "04/03/2021", 1, 1000997),
    listOf(1, 7, "F", "18_34", "C2DE", "04/03/2021", 1, 1001028),
    listOf(1, 8, "F", "35_54", "ABC1", "04/03/2021", 1, 1001096),
    listOf(1, 9, "F", "35_54", "ABC1", "04/03/2021", 1, 1001096),
    listOf(1, 10, "F", "55+", "ABC1", "04/03/2021", 1, 1001289)
  )

private val EVENTS = CSV_EVENTS_LIST.map { event -> CSV_HEADER.zip(event).toMap() }

@RunWith(JUnit4::class)
class CsvEventQueryTest {

  @Test
  fun `filters when no matching conditions`() {
    val eventQuery = CsvEventQuery()
    eventQuery.readCSVData(EVENTS)
    val userVids = eventQuery.getUserVirtualIds(NONMATCHING_EVENT_FILTER)
    assertThat(userVids.toList()).isEmpty()
  }

  @Test
  fun `filters matching conditions`() {
    val matchingVids = listOf(1000650L, 1000997L, 1001028L, 1001096L, 1001096L, 1001289L)
    val eventQuery = CsvEventQuery()
    eventQuery.readCSVData(EVENTS)
    val userVids = eventQuery.getUserVirtualIds(MATCHING_EVENT_FILTER)
    assertThat(userVids.toList().sorted()).isEqualTo(matchingVids.sorted())
  }
}
