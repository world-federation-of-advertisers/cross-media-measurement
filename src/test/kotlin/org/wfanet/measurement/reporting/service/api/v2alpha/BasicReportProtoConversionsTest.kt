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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerEventGroupKey
import org.wfanet.measurement.internal.reporting.v2.BasicReport as InternalBasicReport
import org.wfanet.measurement.internal.reporting.v2.ResultGroupKt as InternalResultGroupKt
import org.wfanet.measurement.internal.reporting.v2.basicReport as internalBasicReport
import org.wfanet.measurement.internal.reporting.v2.basicReportDetails
import org.wfanet.measurement.internal.reporting.v2.basicReportResultDetails
import org.wfanet.measurement.internal.reporting.v2.resultGroup as internalResultGroup
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.ResultGroup.MetricMetadata.ReportingUnitComponentSummary
import org.wfanet.measurement.reporting.v2alpha.reportingUnit

@RunWith(JUnit4::class)
class BasicReportProtoConversionsTest {
  @Test
  fun `toBasicReport populates reporting_set and event_group_summaries when flag enabled`() {
    val basicReport =
      INTERNAL_BASIC_REPORT.toBasicReport(populateDeprecatedReportingUnitEventGroupSummaries = true)

    val componentSummary = basicReport.onlyComponentSummary()
    assertThat(componentSummary.reportingSet)
      .isEqualTo(
        ReportingSetKey(CMMS_MEASUREMENT_CONSUMER_ID, COMPONENT_EXTERNAL_REPORTING_SET_ID).toName()
      )
    assertThat(componentSummary.eventGroupSummariesList.map { it.eventGroup })
      .containsExactly(
        MeasurementConsumerEventGroupKey(CMMS_MEASUREMENT_CONSUMER_ID, CMMS_EVENT_GROUP_ID).toName()
      )
  }

  @Test
  fun `toBasicReport omits event_group_summaries but keeps reporting_set when flag disabled`() {
    val basicReport =
      INTERNAL_BASIC_REPORT.toBasicReport(
        populateDeprecatedReportingUnitEventGroupSummaries = false
      )

    val componentSummary = basicReport.onlyComponentSummary()
    assertThat(componentSummary.reportingSet)
      .isEqualTo(
        ReportingSetKey(CMMS_MEASUREMENT_CONSUMER_ID, COMPONENT_EXTERNAL_REPORTING_SET_ID).toName()
      )
    assertThat(componentSummary.eventGroupSummariesList).isEmpty()
  }

  @Test
  fun `ReportingUnit toInternal encodes DataProvider components`() {
    val internalReportingUnit =
      reportingUnit { components += DataProviderKey(CMMS_DATA_PROVIDER_ID).toName() }.toInternal()

    assertThat(internalReportingUnit.dataProviderKeys.dataProviderKeysList.map { it.cmmsDataProviderId })
      .containsExactly(CMMS_DATA_PROVIDER_ID)
  }

  @Test
  fun `ReportingUnit toInternal encodes ReportingSet custom-group components`() {
    val publicReportingUnit =
      reportingUnit {
        components +=
          ReportingSetKey(CMMS_MEASUREMENT_CONSUMER_ID, COMPONENT_EXTERNAL_REPORTING_SET_ID).toName()
      }

    val internalReportingUnit = publicReportingUnit.toInternal()

    val reportingSetKey = internalReportingUnit.reportingSetKeys.reportingSetKeysList.single()
    assertThat(reportingSetKey.cmmsMeasurementConsumerId).isEqualTo(CMMS_MEASUREMENT_CONSUMER_ID)
    assertThat(reportingSetKey.externalReportingSetId).isEqualTo(COMPONENT_EXTERNAL_REPORTING_SET_ID)
    // Round-trips back to the same public resource names.
    assertThat(internalReportingUnit.toReportingUnit()).isEqualTo(publicReportingUnit)
  }

  @Test
  fun `toBasicReport sets campaign_group and effective_campaign_group when caller-supplied`() {
    val basicReport =
      INTERNAL_BASIC_REPORT.toBasicReport(populateDeprecatedReportingUnitEventGroupSummaries = false)

    val expectedName =
      ReportingSetKey(CMMS_MEASUREMENT_CONSUMER_ID, CAMPAIGN_GROUP_EXTERNAL_ID).toName()
    assertThat(basicReport.campaignGroup).isEqualTo(expectedName)
    assertThat(basicReport.effectiveCampaignGroup).isEqualTo(expectedName)
  }

  @Test
  fun `toBasicReport leaves campaign_group empty but sets effective_campaign_group when synthesized`() {
    val synthesizedReport =
      internalBasicReport {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalBasicReportId = "basic-report"
        externalCampaignGroupId = CAMPAIGN_GROUP_EXTERNAL_ID
        details = basicReportDetails { campaignGroupSynthesized = true }
      }

    val basicReport =
      synthesizedReport.toBasicReport(populateDeprecatedReportingUnitEventGroupSummaries = false)

    assertThat(basicReport.campaignGroup).isEmpty()
    assertThat(basicReport.effectiveCampaignGroup)
      .isEqualTo(ReportingSetKey(CMMS_MEASUREMENT_CONSUMER_ID, CAMPAIGN_GROUP_EXTERNAL_ID).toName())
  }

  private fun BasicReport.onlyComponentSummary(): ReportingUnitComponentSummary {
    return resultGroupsList
      .single()
      .resultsList
      .single()
      .metadata
      .reportingUnitSummary
      .reportingUnitComponentSummaryList
      .single()
  }

  companion object {
    private const val CMMS_MEASUREMENT_CONSUMER_ID = "mc"
    private const val CMMS_DATA_PROVIDER_ID = "dp"
    private const val CMMS_EVENT_GROUP_ID = "eg"
    private const val COMPONENT_EXTERNAL_REPORTING_SET_ID = "component-reporting-set"
    private const val CAMPAIGN_GROUP_EXTERNAL_ID = "campaign-group"

    private val INTERNAL_BASIC_REPORT: InternalBasicReport = internalBasicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = "basic-report"
      externalCampaignGroupId = CAMPAIGN_GROUP_EXTERNAL_ID
      resultDetails = basicReportResultDetails {
        resultGroups += internalResultGroup {
          title = "title"
          results +=
            InternalResultGroupKt.result {
              metadata =
                InternalResultGroupKt.metricMetadata {
                  reportingUnitSummary =
                    InternalResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                      reportingUnitComponentSummary +=
                        InternalResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = CMMS_DATA_PROVIDER_ID
                          cmmsDataProviderDisplayName = "display"
                          externalReportingSetId = COMPONENT_EXTERNAL_REPORTING_SET_ID
                          eventGroupSummaries +=
                            InternalResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = CMMS_EVENT_GROUP_ID
                              }
                        }
                    }
                }
              metricSet = InternalResultGroupKt.metricSet {}
            }
        }
      }
    }
  }
}
