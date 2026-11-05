/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.loadtest.reporting.testing

import com.google.common.truth.Truth.assertThat
import java.time.LocalDate
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
import org.wfanet.measurement.reporting.v2alpha.BasicReport

class ReportingScaleTestDataTest {
  @Test
  fun `buildReport covers exactly fifty states without demographic grouping`() {
    val report = buildReport(50, false, false)
    assertThat(report.resultGroupSpecsCount).isEqualTo(100)
    val states =
      report.resultGroupSpecsList
        .map { it.dimensionSpec.filtersList.single().termsList.single().value.enumValue }
        .toSet()
    assertThat(states)
      .containsExactlyElementsIn(
        Common.UsState.values()
          .filter { it != Common.UsState.UNRECOGNIZED && it.number > 0 }
          .filter { it != Common.UsState.DISTRICT_OF_COLUMBIA }
          .map { it.name }
      )
    assertThat(report.resultGroupSpecsList.any { it.dimensionSpec.hasGrouping() }).isFalse()
    assertThat(report.reportingInterval.reportEnd.month).isEqualTo(7)
    assertThat(report.reportingInterval.reportEnd.day).isEqualTo(1)
  }

  @Test
  fun `buildReport includes DC when exercising native state grouping`() {
    val report = buildReport(51, false, false)
    assertThat(report.resultGroupSpecsCount).isEqualTo(2)
    for (spec in report.resultGroupSpecsList) {
      assertThat(spec.dimensionSpec.grouping.eventTemplateFieldsList)
        .containsExactly(ReportingScaleTestData.STATE_FIELD_PATH)
      assertThat(spec.dimensionSpec.filtersList).isEmpty()
    }
  }

  @Test
  fun `buildReport uses weekly unique reach and whole interval frequency`() {
    val report = buildReport(0, false, false)
    val weekly = report.resultGroupSpecsList.single { it.metricFrequency.hasWeekly() }
    assertThat(weekly.resultGroupMetricSpec.component.nonCumulativeUnique.reach).isTrue()
    assertThat(weekly.resultGroupMetricSpec.reportingUnit.stackedIncrementalReach).isFalse()
    assertThat(weekly.resultGroupMetricSpec.reportingUnit.cumulative.kPlusReach).isEqualTo(0)
    assertThat(weekly.resultGroupMetricSpec.reportingUnit.nonCumulative.kPlusReach).isEqualTo(0)
    val total = report.resultGroupSpecsList.single { it.metricFrequency.hasTotal() }
    assertThat(total.resultGroupMetricSpec.reportingUnit.stackedIncrementalReach).isTrue()
    assertThat(total.resultGroupMetricSpec.reportingUnit.cumulative.kPlusReach).isEqualTo(2)
    assertThat(report.impressionQualificationFiltersList.map { it.impressionQualificationFilter })
      .containsExactly("impressionQualificationFilters/ami", "impressionQualificationFilters/mrc")
  }

  @Test
  fun `buildReport retains age and gender across state cuts`() {
    for (stateCount in listOf(0, 50, 51)) {
      val report = buildReport(stateCount, true, true)
      for (spec in report.resultGroupSpecsList) {
        assertThat(spec.dimensionSpec.grouping.eventTemplateFieldsList)
          .containsAtLeast(
            ReportingScaleTestData.AGE_FIELD_PATH,
            ReportingScaleTestData.GENDER_FIELD_PATH,
          )
      }
    }
  }

  @Test
  fun `buildReport can remove gender while retaining age for a sixty day report`() {
    val report =
      ReportingScaleTestData.buildReport(
        campaignGroup = "measurementConsumers/test/reportingSets/campaign-test",
        dataProviders = listOf("dataProviders/a", "dataProviders/b", "dataProviders/c"),
        modelLine = "modelProviders/test/modelSuites/test/modelLines/test",
        reportStart = LocalDate.of(2026, 4, 1),
        reportDays = 60,
        stateCount = 50,
        filterIds = listOf("ami", "mrc"),
        groupByAge = true,
        groupByGender = false,
      )
    assertThat(report.reportingInterval.reportEnd.month).isEqualTo(5)
    assertThat(report.reportingInterval.reportEnd.day).isEqualTo(31)
    for (spec in report.resultGroupSpecsList) {
      assertThat(spec.dimensionSpec.grouping.eventTemplateFieldsList)
        .containsExactly(ReportingScaleTestData.AGE_FIELD_PATH)
    }
  }

  private fun buildReport(
    stateCount: Int,
    groupByAge: Boolean,
    groupByGender: Boolean,
  ): BasicReport =
    ReportingScaleTestData.buildReport(
      campaignGroup = "measurementConsumers/test/reportingSets/campaign-test",
      dataProviders = listOf("dataProviders/a", "dataProviders/b", "dataProviders/c"),
      modelLine = "modelProviders/test/modelSuites/test/modelLines/test",
      reportStart = LocalDate.of(2026, 4, 1),
      reportDays = 91,
      stateCount = stateCount,
      filterIds = listOf("ami", "mrc"),
      groupByAge = groupByAge,
      groupByGender = groupByGender,
    )
}
