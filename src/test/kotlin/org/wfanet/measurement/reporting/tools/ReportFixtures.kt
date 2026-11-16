/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.tools

import com.google.protobuf.TextFormat
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.ResultGroup

internal object ReportFixtures {
  fun full(): BasicReport =
    BasicReport.newBuilder()
      .setName("measurementConsumers/example/basicReports/example")
      .setTitle("Synthetic state report")
      .setState(BasicReport.State.SUCCEEDED)
      .addResultGroups(
        ResultGroup.newBuilder().setTitle("Weekly reach").addResults(row(120)).addResults(row(240))
      )
      .build()

  fun row(reach: Long): ResultGroup.Result {
    val builder = ResultGroup.Result.newBuilder()
    TextFormat.getParser()
      .merge(
        """
      metadata {
        reporting_unit_summary {
          reporting_unit_component_summary { component: "dataProviders/publisher-a" display_name: "Publisher A" reporting_set: "measurementConsumers/example/reportingSets/a" }
          reporting_unit_component_summary { component: "dataProviders/publisher-b" display_name: "Publisher B" reporting_set: "measurementConsumers/example/reportingSets/b" }
        }
        non_cumulative_metric_start_time { seconds: 1577836800 nanos: 123456789 }
        cumulative_metric_start_time { seconds: 1577836800 }
        metric_end_time { seconds: 1578441600 nanos: 987654321 }
        metric_frequency { weekly: WEDNESDAY }
        dimension_spec_summary { filters { terms { path: "common.us_state" value { enum_value: "CALIFORNIA" } } } }
        filter { impression_qualification_filter: "impressionQualificationFilters/ami" }
      }
      metric_set {
        population_size: 1000
        reporting_unit {
          non_cumulative { reach: ${reach} percent_reach: 12 k_plus_reach: 120 k_plus_reach: 60 percent_k_plus_reach: 12 percent_k_plus_reach: 6 average_frequency: 1.5 impressions: 180 grps: 18 }
          cumulative { reach: 300 }
          stacked_incremental_reach: 120
          stacked_incremental_reach: 30
        }
        components { key: "dataProviders/publisher-a" value { non_cumulative { reach: 100 } cumulative { reach: 200 } non_cumulative_unique { reach: 40 } cumulative_unique { reach: 80 } } }
        components { key: "dataProviders/publisher-b" value { non_cumulative { reach: 80 } cumulative { reach: 160 } non_cumulative_unique { reach: 20 } cumulative_unique { reach: 50 } } }
        component_intersections { components: "dataProviders/publisher-a" components: "dataProviders/publisher-b" non_cumulative { reach: 60 } cumulative { reach: 120 } }
      }
    """
          .trimIndent(),
        builder,
      )
    return builder.build()
  }
}
