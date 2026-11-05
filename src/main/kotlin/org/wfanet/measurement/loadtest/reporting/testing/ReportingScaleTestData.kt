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

import com.google.type.DayOfWeek
import com.google.type.date
import com.google.type.dateTime
import com.google.type.timeZone
import java.time.LocalDate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.DimensionSpec
import org.wfanet.measurement.reporting.v2alpha.DimensionSpecKt
import org.wfanet.measurement.reporting.v2alpha.EventTemplateFieldKt
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.dimensionSpec
import org.wfanet.measurement.reporting.v2alpha.eventFilter
import org.wfanet.measurement.reporting.v2alpha.eventTemplateField
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.reportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.reportingInterval
import org.wfanet.measurement.reporting.v2alpha.reportingUnit
import org.wfanet.measurement.reporting.v2alpha.resultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.resultGroupSpec

/** Builds synthetic reporting requests with reproducible geographic cardinality. */
object ReportingScaleTestData {
  const val STATE_FIELD_PATH = "common.us_state"

  fun buildReport(
    campaignGroup: String,
    dataProviders: List<String>,
    modelLine: String,
    reportStart: LocalDate,
    weeks: Int,
    stateCount: Int,
    filterIds: List<String>,
  ): BasicReport {
    require(dataProviders.size == 3 && dataProviders.distinct().size == 3)
    require(weeks in 1..13)
    require(stateCount in 0..51)
    require(filterIds.isNotEmpty() && filterIds.distinct().size == filterIds.size)
    require(filterIds.all { it in setOf("ami", "mrc") })
    val reportEnd = reportStart.plusWeeks(weeks.toLong())
    return basicReport {
      title = "Reporting scale: $weeks weeks, $stateCount states"
      this.campaignGroup = campaignGroup
      this.modelLine = modelLine
      reportingInterval = reportingInterval {
        this.reportStart = dateTime {
          year = reportStart.year
          month = reportStart.monthValue
          day = reportStart.dayOfMonth
          timeZone = timeZone { id = "UTC" }
        }
        this.reportEnd = date {
          year = reportEnd.year
          month = reportEnd.monthValue
          day = reportEnd.dayOfMonth
        }
      }
      impressionQualificationFilters +=
        filterIds.map { id ->
          reportingImpressionQualificationFilter {
            impressionQualificationFilter = "impressionQualificationFilters/$id"
          }
        }
      for ((index, dimension) in dimensions(stateCount).withIndex()) {
        resultGroupSpecs += resultGroupSpec {
          title = "Weekly reach $index"
          reportingUnit = reportingUnit { components += dataProviders }
          metricFrequency = metricFrequencySpec {
            weekly = DayOfWeek.valueOf(reportStart.dayOfWeek.name)
          }
          dimensionSpec = dimension
          resultGroupMetricSpec = resultGroupMetricSpec {
            populationSize = true
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                nonCumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              }
          }
        }
        resultGroupSpecs += resultGroupSpec {
          title = "Whole interval reach and frequency $index"
          reportingUnit = reportingUnit { components += dataProviders }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimension
          resultGroupMetricSpec = resultGroupMetricSpec {
            populationSize = true
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    reach = true
                    kPlusReach = 2
                    averageFrequency = true
                    impressions = true
                  }
                stackedIncrementalReach = true
              }
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                cumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    reach = true
                    kPlusReach = 2
                    averageFrequency = true
                    impressions = true
                  }
                cumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              }
          }
        }
      }
    }
  }

  private fun dimensions(stateCount: Int): List<DimensionSpec> {
    if (stateCount == 0) return listOf(dimensionSpec {})
    if (stateCount == 51) {
      return listOf(
        dimensionSpec {
          grouping = DimensionSpecKt.grouping { eventTemplateFields += STATE_FIELD_PATH }
        }
      )
    }
    return Common.UsState.values()
      .filter { it != Common.UsState.UNRECOGNIZED && it.number > 0 }
      .filter { it != Common.UsState.DISTRICT_OF_COLUMBIA }
      .sortedBy { it.number }
      .take(stateCount)
      .map { state ->
        dimensionSpec {
          filters += eventFilter {
            terms += eventTemplateField {
              path = STATE_FIELD_PATH
              value = EventTemplateFieldKt.fieldValue { enumValue = state.name }
            }
          }
        }
      }
  }
}
