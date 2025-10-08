/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner

import com.google.protobuf.timestamp
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.ReportResult
import org.wfanet.measurement.internal.reporting.v2.ResultGroup
import org.wfanet.measurement.internal.reporting.v2.ResultGroupKt
import org.wfanet.measurement.internal.reporting.v2.resultGroup

fun buildResultGroups(basicReport: BasicReport, reportResult: ReportResult): List<ResultGroup> {
  return buildList {
    for (resultGroupSpec in basicReport.details.resultGroupSpecsList) {
      val resultGroup = resultGroup {
        title = resultGroupSpec.title
        for (impressionQualificationFilter in basicReport.details.impressionQualificationFiltersList) {
          results += ResultGroupKt.result {
            metadata = ResultGroupKt.metricMetadata {
              reportingUnitSummary = ResultGroupKt.MetricMetadataKt.reportingUnitSummary {

              }
              nonCumulativeMetricStartTime = timestamp {  }
              cumulativeMetricStartTime = timestamp {  }
              metricEndTime = timestamp {  }
              metricFrequencySpec = resultGroupSpec.metricFrequency
              dimensionSpecSummary = ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                groupings = resultGroupSpec.dimensionSpec.groupingsList
                filters += resultGroupSpec.dimensionSpec.filtersList
              }
              filter = impressionQualificationFilter
            }
            metricSet = ResultGroupKt.metricSet {

            }
          }
        }
      }

      add(resultGroup)
    }
  }
}
