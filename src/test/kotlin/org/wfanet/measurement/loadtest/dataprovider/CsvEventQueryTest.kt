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
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDate
import java.time.ZoneOffset
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplate.Gender.Value as BannerGender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.Gender.Value as PrivacyGender
import org.wfanet.measurement.api.v2alpha.timeInterval
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.toProtoTime

private val directoryPath: Path =
  Paths.get(
    "wfa_measurement_system",
    "src",
    "test",
    "kotlin",
    "org",
    "wfanet",
    "measurement",
    "loadtest",
    "dataprovider",
  )
private const val FILE_NAME = "CsvEventQueryTestEvents.csv"
private val FILE: File = getRuntimePath(directoryPath.resolve(FILE_NAME))!!.toFile()

private const val PUBLISHER_ID_1 = 1
private const val BANNER_FEMALE = BannerGender.GENDER_FEMALE_VALUE
private const val PERSON_35_to_54 = Person.AgeGroup.YEARS_35_TO_54_VALUE
private const val PRIVACY_MALE = PrivacyGender.GENDER_MALE_VALUE

private val MATCHING_EVENT_FILTER = eventFilter {
  expression =
    "person.age_group.value == $PERSON_35_to_54 || banner_ad.gender.value == $BANNER_FEMALE"
}
private val NONMATCHING_EVENT_FILTER = eventFilter {
  expression =
    "privacy_budget.gender.value == $PRIVACY_MALE && banner_ad.gender.value == $BANNER_FEMALE"
}
private val EMPTY_EVENT_FILTER = RequisitionSpec.EventFilter.getDefaultInstance()

private val EVENT_DATE = LocalDate.of(2021, 4, 20)
private val TIME_INTERVAL = timeInterval {
  startTime = EVENT_DATE.atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
  endTime = EVENT_DATE.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
}

private val ALL_VIDS: List<Long> =
  listOf(1000077, 1000650, 1000694, 1000759, 1000840, 1000997, 1001028, 1001096, 1001096, 1001289)

@RunWith(JUnit4::class)
class CsvEventQueryTest {
  @Test
  fun `getUserVirtualIds return empty when no conditions match`() {
    val userVids: Sequence<Long> =
      eventQuery.getUserVirtualIds(TIME_INTERVAL, NONMATCHING_EVENT_FILTER)
    assertThat(userVids.toList()).isEmpty()
  }

  @Test
  fun `getUserVirtualIds returns matching VIDs`() {
    val matchingVids = listOf(1000650L, 1000997L, 1001028L, 1001096L, 1001096L, 1001289L)
    val userVids: Sequence<Long> =
      eventQuery.getUserVirtualIds(TIME_INTERVAL, MATCHING_EVENT_FILTER)
    assertThat(userVids.toList()).containsExactlyElementsIn(matchingVids)
  }

  @Test
  fun `getUserVirtualIds returns all VIDs when filter is empty`() {
    val vids = eventQuery.getUserVirtualIds(TIME_INTERVAL, EMPTY_EVENT_FILTER)

    assertThat(vids.toList()).containsExactlyElementsIn(ALL_VIDS)
  }

  companion object {
    private val eventQuery = CsvEventQuery(PUBLISHER_ID_1, FILE)
  }
}
