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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat as assertThatProto
import com.google.protobuf.timestamp
import kotlin.test.Test
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerEventGroupKey
import org.wfanet.measurement.internal.reporting.v2.ResultGroupKt as InternalResultGroupKt
import org.wfanet.measurement.internal.reporting.v2.resultGroup as internalResultGroup
import org.wfanet.measurement.reporting.v2alpha.ResultGroupKt

class BasicReportProtoConversionsTest {
  @Test
  fun `toResultGroup moves shared reporting unit summary to group metadata`() {
    val cmmsMeasurementConsumerId = "mc-id"
    val cmmsDataProviderId = "dp-id"
    val internalResultGroup = internalResultGroup {
      title = "title"
      results +=
        InternalResultGroupKt.result {
          metadata =
            InternalResultGroupKt.metricMetadata {
              reportingUnitSummary =
                InternalResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                  reportingUnitComponentSummary +=
                    InternalResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                      this.cmmsDataProviderId = cmmsDataProviderId
                      cmmsDataProviderDisplayName = "Display"
                      eventGroupSummaries +=
                        InternalResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                          .eventGroupSummary {
                            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                            cmmsEventGroupId = "event-group"
                          }
                    }
                }
              metricEndTime = timestamp { seconds = 123L }
            }
          metricSet = InternalResultGroupKt.metricSet { populationSize = 1L }
        }
      results +=
        InternalResultGroupKt.result {
          metadata =
            InternalResultGroupKt.metricMetadata {
              reportingUnitSummary =
                InternalResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                  reportingUnitComponentSummary +=
                    InternalResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                      this.cmmsDataProviderId = cmmsDataProviderId
                      cmmsDataProviderDisplayName = "Display"
                      eventGroupSummaries +=
                        InternalResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                          .eventGroupSummary {
                            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                            cmmsEventGroupId = "event-group"
                          }
                    }
                }
              metricEndTime = timestamp { seconds = 456L }
            }
          metricSet = InternalResultGroupKt.metricSet { populationSize = 2L }
        }
    }

    val resultGroup = internalResultGroup.toResultGroup()

    assertThatProto(resultGroup.metricMetadata.reportingUnitSummary)
      .isEqualTo(
        ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
          reportingUnitComponentSummary +=
            ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
              component = DataProviderKey(cmmsDataProviderId).toName()
              displayName = "Display"
              eventGroupSummaries +=
                ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt.eventGroupSummary {
                  eventGroup =
                    MeasurementConsumerEventGroupKey(
                        cmmsMeasurementConsumerId,
                        "event-group",
                      )
                      .toName()
                }
            }
        }
      )
    assertThat(resultGroup.resultsCount).isEqualTo(2)
    assertThat(resultGroup.resultsList[0].metadata.hasReportingUnitSummary()).isFalse()
    assertThat(resultGroup.resultsList[1].metadata.hasReportingUnitSummary()).isFalse()
    assertThat(resultGroup.resultsList[0].metadata.metricEndTime.seconds).isEqualTo(123L)
    assertThat(resultGroup.resultsList[1].metadata.metricEndTime.seconds).isEqualTo(456L)
    assertThat(resultGroup.resultsList[0].metricSet.populationSize).isEqualTo(1L)
    assertThat(resultGroup.resultsList[1].metricSet.populationSize).isEqualTo(2L)
  }
}
