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
import java.util.logging.Handler
import java.util.logging.Level
import java.util.logging.LogRecord
import java.util.logging.Logger
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
  fun `toBasicReport omits reporting_set when external_reporting_set_id not set`() {
    val basicReport =
      INTERNAL_BASIC_REPORT_WITHOUT_COMPONENT_REPORTING_SET_ID.toBasicReport(
        populateDeprecatedReportingUnitEventGroupSummaries = true
      )

    val componentSummary = basicReport.onlyComponentSummary()
    assertThat(componentSummary.reportingSet).isEmpty()
    assertThat(componentSummary.component)
      .isEqualTo(DataProviderKey(CMMS_DATA_PROVIDER_ID).toName())
    assertThat(componentSummary.eventGroupSummariesList.map { it.eventGroup })
      .containsExactly(
        MeasurementConsumerEventGroupKey(CMMS_MEASUREMENT_CONSUMER_ID, CMMS_EVENT_GROUP_ID).toName()
      )
  }

  @Test
  fun `toBasicReport omits reporting_set when id not set and deprecated flag disabled`() {
    val basicReport =
      INTERNAL_BASIC_REPORT_WITHOUT_COMPONENT_REPORTING_SET_ID.toBasicReport(
        populateDeprecatedReportingUnitEventGroupSummaries = false
      )

    val componentSummary = basicReport.onlyComponentSummary()
    assertThat(componentSummary.reportingSet).isEmpty()
    assertThat(componentSummary.component)
      .isEqualTo(DataProviderKey(CMMS_DATA_PROVIDER_ID).toName())
  }

  @Test
  fun `ReportingUnit toInternal encodes DataProvider components`() {
    val internalReportingUnit =
      reportingUnit { components += DataProviderKey(CMMS_DATA_PROVIDER_ID).toName() }.toInternal()

    assertThat(
        internalReportingUnit.dataProviderKeys.dataProviderKeysList.map { it.cmmsDataProviderId }
      )
      .containsExactly(CMMS_DATA_PROVIDER_ID)
  }

  @Test
  fun `ReportingUnit toInternal encodes ReportingSet custom-group components`() {
    val publicReportingUnit = reportingUnit {
      components +=
        ReportingSetKey(CMMS_MEASUREMENT_CONSUMER_ID, COMPONENT_EXTERNAL_REPORTING_SET_ID).toName()
    }

    val internalReportingUnit = publicReportingUnit.toInternal()

    val reportingSetKey = internalReportingUnit.reportingSetKeys.reportingSetKeysList.single()
    assertThat(reportingSetKey.cmmsMeasurementConsumerId).isEqualTo(CMMS_MEASUREMENT_CONSUMER_ID)
    assertThat(reportingSetKey.externalReportingSetId)
      .isEqualTo(COMPONENT_EXTERNAL_REPORTING_SET_ID)
    // Round-trips back to the same public resource names.
    assertThat(internalReportingUnit.toReportingUnit()).isEqualTo(publicReportingUnit)
  }

  @Test
  fun `toBasicReport sets campaign_group and effective_campaign_group when caller-supplied`() {
    val basicReport =
      INTERNAL_BASIC_REPORT.toBasicReport(
        populateDeprecatedReportingUnitEventGroupSummaries = false
      )

    val expectedName =
      ReportingSetKey(CMMS_MEASUREMENT_CONSUMER_ID, CAMPAIGN_GROUP_EXTERNAL_ID).toName()
    assertThat(basicReport.campaignGroup).isEqualTo(expectedName)
    assertThat(basicReport.effectiveCampaignGroup).isEqualTo(expectedName)
  }

  @Test
  fun `toBasicReport leaves campaign_group empty but sets effective_campaign_group when synthesized`() {
    val synthesizedReport = internalBasicReport {
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

  @Test
  fun `toBasicReport maps reporting_set_components to ReportingSet-keyed components`() {
    val customGroupId = "custom-group"
    val internalBasicReport = internalBasicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = "basic-report"
      externalCampaignGroupId = "campaign-group"
      resultDetails = basicReportResultDetails {
        resultGroups += internalResultGroup {
          title = "title"
          results +=
            InternalResultGroupKt.result {
              metadata = InternalResultGroupKt.metricMetadata {}
              metricSet =
                InternalResultGroupKt.metricSet {
                  reportingSetComponents +=
                    InternalResultGroupKt.MetricSetKt.reportingSetComponentMetricSetMapEntry {
                      externalReportingSetId = customGroupId
                      value =
                        InternalResultGroupKt.MetricSetKt.componentMetricSet {
                          cumulative =
                            InternalResultGroupKt.MetricSetKt.basicMetricSet { reach = 42 }
                        }
                    }
                }
            }
        }
      }
    }

    val basicReport =
      internalBasicReport.toBasicReport(populateDeprecatedReportingUnitEventGroupSummaries = false)

    val component =
      basicReport.resultGroupsList.single().resultsList.single().metricSet.componentsList.single()
    assertThat(component.key)
      .isEqualTo(ReportingSetKey(CMMS_MEASUREMENT_CONSUMER_ID, customGroupId).toName())
    assertThat(component.value.cumulative.reach).isEqualTo(42)
  }

  @Test
  fun `toBasicReport sets component to ReportingSet name for custom-group component summary`() {
    val customGroupId = "custom-group"
    val internalBasicReport = internalBasicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = "basic-report"
      externalCampaignGroupId = "campaign-group"
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
                          // No cmmsDataProviderId: this is a custom-group component.
                          externalReportingSetId = customGroupId
                        }
                    }
                }
              metricSet = InternalResultGroupKt.metricSet {}
            }
        }
      }
    }

    val basicReport =
      internalBasicReport.toBasicReport(populateDeprecatedReportingUnitEventGroupSummaries = false)

    val componentSummary = basicReport.onlyComponentSummary()
    assertThat(componentSummary.component)
      .isEqualTo(ReportingSetKey(CMMS_MEASUREMENT_CONSUMER_ID, customGroupId).toName())
  }

  @Test
  fun `toBasicReport warns once when a component summary has no external_reporting_set_id`() {
    val warnings = captureConversionWarnings {
      INTERNAL_BASIC_REPORT_WITHOUT_COMPONENT_REPORTING_SET_ID.toBasicReport(
        populateDeprecatedReportingUnitEventGroupSummaries = false
      )
    }

    assertThat(warnings).hasSize(1)
    assertThat(warnings.single()).contains("basic-report")
    assertThat(warnings.single()).contains("without external_reporting_set_id")
  }

  @Test
  fun `toBasicReport does not warn when every component summary is populated`() {
    val warnings = captureConversionWarnings {
      internalBasicReportWithComponentReportingSetId("reporting-set")
        .toBasicReport(populateDeprecatedReportingUnitEventGroupSummaries = false)
    }

    assertThat(warnings).isEmpty()
  }

  /**
   * Runs [block] and returns the WARNING messages the conversion logger emitted.
   *
   * The warning is the signal that gates removal of the tolerate-blank behavior, so it is asserted
   * on directly rather than inferred from the converted message.
   */
  private fun captureConversionWarnings(block: () -> Unit): List<String> {
    val logger = Logger.getLogger(CONVERSION_LOGGER_NAME)
    val warnings = mutableListOf<String>()
    val handler =
      object : Handler() {
        override fun publish(record: LogRecord) {
          if (record.level == Level.WARNING) {
            warnings.add(record.message)
          }
        }

        override fun flush() {}

        override fun close() {}
      }
    logger.addHandler(handler)
    return try {
      block()
      warnings
    } finally {
      logger.removeHandler(handler)
    }
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
    private const val CONVERSION_LOGGER_NAME = "BasicReportProtoConversions"

    private fun internalBasicReportWithComponentReportingSetId(
      componentExternalReportingSetId: String
    ): InternalBasicReport = internalBasicReport {
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
                          externalReportingSetId = componentExternalReportingSetId
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

    private val INTERNAL_BASIC_REPORT: InternalBasicReport =
      internalBasicReportWithComponentReportingSetId(COMPONENT_EXTERNAL_REPORTING_SET_ID)

    private val INTERNAL_BASIC_REPORT_WITHOUT_COMPONENT_REPORTING_SET_ID: InternalBasicReport =
      internalBasicReportWithComponentReportingSetId("")
  }
}
