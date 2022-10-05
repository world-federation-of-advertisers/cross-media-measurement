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
import kotlin.test.assertFails
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplate.Gender.Value as BannerGender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.AgeRange.Value as PrivacyAge
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.Gender.Value as PrivacyGender
import org.wfanet.measurement.common.getRuntimePath

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
private val EMPTY_EVENT_FILTER = eventFilter { expression = "" }

@RunWith(JUnit4::class)
class CsvEventQueryTest {
  companion object {
    private val eventQuery = CsvEventQuery(PUBLISHER_ID_1, FILE)
  }

  @Test
  fun `filters when no matching conditions`() {
    val userVids: Sequence<Long> = eventQuery.getUserVirtualIds(NONMATCHING_EVENT_FILTER)
    assertThat(userVids.toList()).isEmpty()
  }

  @Test
  fun `filters matching conditions`() {
    val matchingVids = listOf(1000650L, 1000997L, 1001028L, 1001096L, 1001096L, 1001289L)
    val userVids: Sequence<Long> = eventQuery.getUserVirtualIds(MATCHING_EVENT_FILTER)
    assertThat(userVids.toList()).containsExactlyElementsIn(matchingVids)
  }

  @Test
  fun `empty filter should fail`() {
    assertFails { eventQuery.getUserVirtualIds(EMPTY_EVENT_FILTER) }
  }
}
