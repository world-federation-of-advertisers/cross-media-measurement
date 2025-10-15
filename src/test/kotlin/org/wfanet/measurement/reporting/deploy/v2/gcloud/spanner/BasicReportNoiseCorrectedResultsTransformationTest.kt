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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.type.DayOfWeek
import com.google.type.copy
import com.google.type.date
import com.google.type.dateTime
import com.google.type.timeZone
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.toTimestamp
import org.wfanet.measurement.internal.reporting.v2.DimensionSpecKt
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.ReportResult
import org.wfanet.measurement.internal.reporting.v2.ReportResultKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingUnitKt
import org.wfanet.measurement.internal.reporting.v2.ResultGroupKt
import org.wfanet.measurement.internal.reporting.v2.ResultGroupMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.basicReport
import org.wfanet.measurement.internal.reporting.v2.basicReportDetails
import org.wfanet.measurement.internal.reporting.v2.dataProviderKey
import org.wfanet.measurement.internal.reporting.v2.dimensionSpec
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.metricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.reportResult
import org.wfanet.measurement.internal.reporting.v2.reportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.reportingInterval
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.internal.reporting.v2.reportingUnit
import org.wfanet.measurement.internal.reporting.v2.resultGroup
import org.wfanet.measurement.internal.reporting.v2.resultGroupMetricSpec
import org.wfanet.measurement.internal.reporting.v2.resultGroupSpec

@RunWith(JUnit4::class)
class BasicReportNoiseCorrectedResultsTransformationTest {
  @Test
  fun `buildResultGroups creates result groups correctly`() {
    val basicReport = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      details = basicReportDetails {
        impressionQualificationFilters += IMPRESSION_QUALIFICATION_FILTER_1
        reportingInterval = REPORTING_INTERVAL
        resultGroupSpecs += resultGroupSpec {
          title = "result-group-1"
          reportingUnit = reportingUnit {
            dataProviderKeys =
              ReportingUnitKt.dataProviderKeys {
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = DATA_PROVIDER_1_ID }
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = DATA_PROVIDER_2_ID }
              }
          }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.age_group" }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            populationSize = true
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                stackedIncrementalReach = true
              }
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { impressions = true }
                cumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              }
          }
        }
      }
    }

    val reportResult = reportResult {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportStart = REPORTING_INTERVAL.reportStart
      // Primitive 1
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = PRIMITIVE_REPORTING_SET_1_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.TOTAL
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults =
                    ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 10
                      impressions = 100
                    }
                }
            }
        }
      // Primitive 2
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = PRIMITIVE_REPORTING_SET_2_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.TOTAL
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults =
                    ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 20
                      impressions = 200
                    }
                }
            }
        }
      // Composite
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = COMPOSITE_REPORTING_SET_1_2_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.TOTAL
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults = ResultGroupKt.MetricSetKt.basicMetricSet { reach = 25 }
                }
            }
        }
    }

    val primitiveReportingSetByDataProviderId =
      mapOf(
        DATA_PROVIDER_1_ID to PRIMITIVE_REPORTING_SET_1,
        DATA_PROVIDER_2_ID to PRIMITIVE_REPORTING_SET_2,
      )

    val compositeReportingSetIdBySetExpression =
      mapOf(
        buildUnionSetExpression(
          listOf(PRIMITIVE_REPORTING_SET_1_ID, PRIMITIVE_REPORTING_SET_2_ID)
        ) to COMPOSITE_REPORTING_SET_1_2_ID
      )

    val resultGroups =
      buildResultGroups(
        basicReport,
        reportResult,
        primitiveReportingSetByDataProviderId,
        compositeReportingSetIdBySetExpression,
      )

    val expectedResultGroups =
      listOf(
        resultGroup {
          title = "result-group-1"
          results +=
            ResultGroupKt.result {
              metadata =
                ResultGroupKt.metricMetadata {
                  reportingUnitSummary =
                    ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_1_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg1"
                              }
                        }
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_2_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg2"
                              }
                        }
                    }
                  cumulativeMetricStartTime = REPORTING_INTERVAL.reportStart.toTimestamp()
                  metricEndTime =
                    REPORTING_INTERVAL.reportStart
                      .copy {
                        year = REPORTING_INTERVAL.reportEnd.year
                        month = REPORTING_INTERVAL.reportEnd.month
                        day = REPORTING_INTERVAL.reportEnd.day
                      }
                      .toTimestamp()
                  metricFrequencySpec = metricFrequencySpec { total = true }
                  dimensionSpecSummary =
                    ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                      groupings += eventTemplateField {
                        path = "person.age_group"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                      }
                    }
                  filter = IMPRESSION_QUALIFICATION_FILTER_1
                }
              metricSet =
                ResultGroupKt.metricSet {
                  populationSize = 100
                  reportingUnit =
                    ResultGroupKt.MetricSetKt.reportingUnitMetricSet {
                      cumulative = ResultGroupKt.MetricSetKt.basicMetricSet { reach = 25 }
                      stackedIncrementalReach += 10
                      stackedIncrementalReach += 15 // 25 - 10
                    }
                  components +=
                    ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = DATA_PROVIDER_1_ID
                      value =
                        ResultGroupKt.MetricSetKt.componentMetricSet {
                          cumulative =
                            ResultGroupKt.MetricSetKt.basicMetricSet { impressions = 100 }
                          cumulativeUnique =
                            ResultGroupKt.MetricSetKt.uniqueMetricSet {
                              reach = 5 // 25 - 20
                            }
                        }
                    }
                  components +=
                    ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = DATA_PROVIDER_2_ID
                      value =
                        ResultGroupKt.MetricSetKt.componentMetricSet {
                          cumulative =
                            ResultGroupKt.MetricSetKt.basicMetricSet { impressions = 200 }
                          cumulativeUnique =
                            ResultGroupKt.MetricSetKt.uniqueMetricSet {
                              reach = 15 // 25 - 10
                            }
                        }
                    }
                }
            }
        }
      )

    assertThat(resultGroups)
      .ignoringRepeatedFieldOrder()
      .containsExactlyElementsIn(expectedResultGroups)
  }

  @Test
  fun `buildResultGroups creates multiple result groups for multiple result group specs`() {
    val basicReport = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      details = basicReportDetails {
        impressionQualificationFilters += IMPRESSION_QUALIFICATION_FILTER_1
        reportingInterval = REPORTING_INTERVAL
        resultGroupSpecs += resultGroupSpec {
          title = "result-group-1"
          reportingUnit = reportingUnit {
            dataProviderKeys =
              ReportingUnitKt.dataProviderKeys {
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = DATA_PROVIDER_1_ID }
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = DATA_PROVIDER_2_ID }
              }
          }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.age_group" }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            populationSize = true
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                stackedIncrementalReach = true
              }
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { impressions = true }
                cumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              }
          }
        }
        resultGroupSpecs += resultGroupSpec {
          title = "result-group-2"
          reportingUnit = reportingUnit {
            dataProviderKeys =
              ReportingUnitKt.dataProviderKeys {
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = DATA_PROVIDER_1_ID }
              }
          }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.age_group" }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            populationSize = true
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                stackedIncrementalReach = true
              }
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { impressions = true }
                cumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              }
          }
        }
      }
    }

    val reportResult = reportResult {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportStart = REPORTING_INTERVAL.reportStart
      // Primitive 1
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = PRIMITIVE_REPORTING_SET_1_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.TOTAL
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults =
                    ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 10
                      impressions = 100
                    }
                }
            }
        }
      // Primitive 2
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = PRIMITIVE_REPORTING_SET_2_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.TOTAL
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults =
                    ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 20
                      impressions = 200
                    }
                }
            }
        }
      // Composite
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = COMPOSITE_REPORTING_SET_1_2_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.TOTAL
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults = ResultGroupKt.MetricSetKt.basicMetricSet { reach = 25 }
                }
            }
        }
    }

    val primitiveReportingSetByDataProviderId =
      mapOf(
        DATA_PROVIDER_1_ID to PRIMITIVE_REPORTING_SET_1,
        DATA_PROVIDER_2_ID to PRIMITIVE_REPORTING_SET_2,
      )

    val compositeReportingSetIdBySetExpression =
      mapOf(
        buildUnionSetExpression(
          listOf(PRIMITIVE_REPORTING_SET_1_ID, PRIMITIVE_REPORTING_SET_2_ID)
        ) to COMPOSITE_REPORTING_SET_1_2_ID
      )

    val resultGroups =
      buildResultGroups(
        basicReport,
        reportResult,
        primitiveReportingSetByDataProviderId,
        compositeReportingSetIdBySetExpression,
      )

    val expectedResultGroups =
      listOf(
        resultGroup {
          title = "result-group-1"
          results +=
            ResultGroupKt.result {
              metadata =
                ResultGroupKt.metricMetadata {
                  reportingUnitSummary =
                    ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_1_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg1"
                              }
                        }
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_2_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg2"
                              }
                        }
                    }
                  cumulativeMetricStartTime = REPORTING_INTERVAL.reportStart.toTimestamp()
                  metricEndTime =
                    REPORTING_INTERVAL.reportStart
                      .copy {
                        year = REPORTING_INTERVAL.reportEnd.year
                        month = REPORTING_INTERVAL.reportEnd.month
                        day = REPORTING_INTERVAL.reportEnd.day
                      }
                      .toTimestamp()
                  metricFrequencySpec = metricFrequencySpec { total = true }
                  dimensionSpecSummary =
                    ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                      groupings += eventTemplateField {
                        path = "person.age_group"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                      }
                    }
                  filter = IMPRESSION_QUALIFICATION_FILTER_1
                }
              metricSet =
                ResultGroupKt.metricSet {
                  populationSize = 100
                  reportingUnit =
                    ResultGroupKt.MetricSetKt.reportingUnitMetricSet {
                      cumulative = ResultGroupKt.MetricSetKt.basicMetricSet { reach = 25 }
                      stackedIncrementalReach += 10
                      stackedIncrementalReach += 15 // 25 - 10
                    }
                  components +=
                    ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = DATA_PROVIDER_1_ID
                      value =
                        ResultGroupKt.MetricSetKt.componentMetricSet {
                          cumulative =
                            ResultGroupKt.MetricSetKt.basicMetricSet { impressions = 100 }
                          cumulativeUnique =
                            ResultGroupKt.MetricSetKt.uniqueMetricSet {
                              reach = 5 // 25 - 20
                            }
                        }
                    }
                  components +=
                    ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = DATA_PROVIDER_2_ID
                      value =
                        ResultGroupKt.MetricSetKt.componentMetricSet {
                          cumulative =
                            ResultGroupKt.MetricSetKt.basicMetricSet { impressions = 200 }
                          cumulativeUnique =
                            ResultGroupKt.MetricSetKt.uniqueMetricSet {
                              reach = 15 // 25 - 10
                            }
                        }
                    }
                }
            }
        },
        resultGroup {
          title = "result-group-2"
          results +=
            ResultGroupKt.result {
              metadata =
                ResultGroupKt.metricMetadata {
                  reportingUnitSummary =
                    ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_1_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg1"
                              }
                        }
                    }
                  cumulativeMetricStartTime = REPORTING_INTERVAL.reportStart.toTimestamp()
                  metricEndTime =
                    REPORTING_INTERVAL.reportStart
                      .copy {
                        year = REPORTING_INTERVAL.reportEnd.year
                        month = REPORTING_INTERVAL.reportEnd.month
                        day = REPORTING_INTERVAL.reportEnd.day
                      }
                      .toTimestamp()
                  metricFrequencySpec = metricFrequencySpec { total = true }
                  dimensionSpecSummary =
                    ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                      groupings += eventTemplateField {
                        path = "person.age_group"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                      }
                    }
                  filter = IMPRESSION_QUALIFICATION_FILTER_1
                }
              metricSet =
                ResultGroupKt.metricSet {
                  populationSize = 100
                  reportingUnit =
                    ResultGroupKt.MetricSetKt.reportingUnitMetricSet {
                      cumulative = ResultGroupKt.MetricSetKt.basicMetricSet { reach = 10 }
                      stackedIncrementalReach += 10
                    }
                  components +=
                    ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = DATA_PROVIDER_1_ID
                      value =
                        ResultGroupKt.MetricSetKt.componentMetricSet {
                          cumulative =
                            ResultGroupKt.MetricSetKt.basicMetricSet { impressions = 100 }
                        }
                    }
                }
            }
        },
      )

    assertThat(resultGroups).hasSize(2)
    assertThat(resultGroups)
      .ignoringRepeatedFieldOrder()
      .containsExactlyElementsIn(expectedResultGroups)
  }

  @Test
  fun `buildResultGroups creates result groups correctly when windowStartDate is set`() {
    val basicReport = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      details = basicReportDetails {
        impressionQualificationFilters += IMPRESSION_QUALIFICATION_FILTER_1
        reportingInterval = REPORTING_INTERVAL
        resultGroupSpecs += resultGroupSpec {
          title = "result-group-1"
          reportingUnit = reportingUnit {
            dataProviderKeys =
              ReportingUnitKt.dataProviderKeys {
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = DATA_PROVIDER_1_ID }
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = DATA_PROVIDER_2_ID }
              }
          }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
          dimensionSpec = dimensionSpec {
            grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.age_group" }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            populationSize = true
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
          }
        }
      }
    }

    val reportResult = reportResult {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportStart = REPORTING_INTERVAL.reportStart
      // Composite
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = COMPOSITE_REPORTING_SET_1_2_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.WEEKLY
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowStartDate = date {
                day = REPORTING_INTERVAL.reportStart.day + 1
                month = REPORTING_INTERVAL.reportStart.month
                year = REPORTING_INTERVAL.reportStart.year
              }
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults = ResultGroupKt.MetricSetKt.basicMetricSet { reach = 25 }
                }
            }
        }
    }

    val primitiveReportingSetByDataProviderId =
      mapOf(
        DATA_PROVIDER_1_ID to PRIMITIVE_REPORTING_SET_1,
        DATA_PROVIDER_2_ID to PRIMITIVE_REPORTING_SET_2,
      )

    val compositeReportingSetIdBySetExpression =
      mapOf(
        buildUnionSetExpression(
          listOf(PRIMITIVE_REPORTING_SET_1_ID, PRIMITIVE_REPORTING_SET_2_ID)
        ) to COMPOSITE_REPORTING_SET_1_2_ID
      )

    val resultGroups =
      buildResultGroups(
        basicReport,
        reportResult,
        primitiveReportingSetByDataProviderId,
        compositeReportingSetIdBySetExpression,
      )

    val expectedResultGroups =
      listOf(
        resultGroup {
          title = "result-group-1"
          results +=
            ResultGroupKt.result {
              metadata =
                ResultGroupKt.metricMetadata {
                  reportingUnitSummary =
                    ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_1_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg1"
                              }
                        }
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_2_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg2"
                              }
                        }
                    }
                  nonCumulativeMetricStartTime =
                    REPORTING_INTERVAL.reportStart
                      .copy { day = REPORTING_INTERVAL.reportStart.day + 1 }
                      .toTimestamp()
                  cumulativeMetricStartTime = REPORTING_INTERVAL.reportStart.toTimestamp()
                  metricEndTime =
                    REPORTING_INTERVAL.reportStart
                      .copy {
                        year = REPORTING_INTERVAL.reportEnd.year
                        month = REPORTING_INTERVAL.reportEnd.month
                        day = REPORTING_INTERVAL.reportEnd.day
                      }
                      .toTimestamp()
                  metricFrequencySpec = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
                  dimensionSpecSummary =
                    ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                      groupings += eventTemplateField {
                        path = "person.age_group"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                      }
                    }
                  filter = IMPRESSION_QUALIFICATION_FILTER_1
                }
              metricSet =
                ResultGroupKt.metricSet {
                  populationSize = 100
                  reportingUnit =
                    ResultGroupKt.MetricSetKt.reportingUnitMetricSet {
                      cumulative = ResultGroupKt.MetricSetKt.basicMetricSet { reach = 25 }
                    }
                }
            }
        }
      )

    assertThat(resultGroups)
      .ignoringRepeatedFieldOrder()
      .containsExactlyElementsIn(expectedResultGroups)
  }

  @Test
  fun `buildResultGroups creates result groups correctly when reportingUnit has 1 component`() {
    val basicReport = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      details = basicReportDetails {
        impressionQualificationFilters += IMPRESSION_QUALIFICATION_FILTER_1
        reportingInterval = REPORTING_INTERVAL
        resultGroupSpecs += resultGroupSpec {
          title = "result-group-1"
          reportingUnit = reportingUnit {
            dataProviderKeys =
              ReportingUnitKt.dataProviderKeys {
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = DATA_PROVIDER_1_ID }
              }
          }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.age_group" }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            populationSize = true
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
          }
        }
      }
    }

    val reportResult = reportResult {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportStart = REPORTING_INTERVAL.reportStart
      // Primitive 1
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = PRIMITIVE_REPORTING_SET_1_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.TOTAL
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults =
                    ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 10
                      impressions = 100
                    }
                }
            }
        }
    }

    val primitiveReportingSetByDataProviderId =
      mapOf(
        DATA_PROVIDER_1_ID to PRIMITIVE_REPORTING_SET_1,
        DATA_PROVIDER_2_ID to PRIMITIVE_REPORTING_SET_2,
      )

    val resultGroups =
      buildResultGroups(basicReport, reportResult, primitiveReportingSetByDataProviderId, mapOf())

    val expectedResultGroups =
      listOf(
        resultGroup {
          title = "result-group-1"
          results +=
            ResultGroupKt.result {
              metadata =
                ResultGroupKt.metricMetadata {
                  reportingUnitSummary =
                    ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_1_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg1"
                              }
                        }
                    }
                  cumulativeMetricStartTime = REPORTING_INTERVAL.reportStart.toTimestamp()
                  metricEndTime =
                    REPORTING_INTERVAL.reportStart
                      .copy {
                        year = REPORTING_INTERVAL.reportEnd.year
                        month = REPORTING_INTERVAL.reportEnd.month
                        day = REPORTING_INTERVAL.reportEnd.day
                      }
                      .toTimestamp()
                  metricFrequencySpec = metricFrequencySpec { total = true }
                  dimensionSpecSummary =
                    ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                      groupings += eventTemplateField {
                        path = "person.age_group"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                      }
                    }
                  filter = IMPRESSION_QUALIFICATION_FILTER_1
                }
              metricSet =
                ResultGroupKt.metricSet {
                  populationSize = 100
                  reportingUnit =
                    ResultGroupKt.MetricSetKt.reportingUnitMetricSet {
                      cumulative = ResultGroupKt.MetricSetKt.basicMetricSet { reach = 10 }
                    }
                }
            }
        }
      )

    assertThat(resultGroups)
      .ignoringRepeatedFieldOrder()
      .containsExactlyElementsIn(expectedResultGroups)
  }

  @Test
  fun `buildResultGroups creates result groups correctly when weekly and noncumulative`() {
    val basicReport = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      details = basicReportDetails {
        impressionQualificationFilters += IMPRESSION_QUALIFICATION_FILTER_1
        reportingInterval = REPORTING_INTERVAL
        resultGroupSpecs += resultGroupSpec {
          title = "result-group-1"
          reportingUnit = reportingUnit {
            dataProviderKeys =
              ReportingUnitKt.dataProviderKeys {
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = DATA_PROVIDER_1_ID }
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = DATA_PROVIDER_2_ID }
              }
          }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
          dimensionSpec = dimensionSpec {
            grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.age_group" }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            populationSize = true
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { impressions = true }
                nonCumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              }
          }
        }
      }
    }

    val reportResult = reportResult {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportStart = REPORTING_INTERVAL.reportStart
      // Primitive 1
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = PRIMITIVE_REPORTING_SET_1_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.WEEKLY
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  nonCumulativeResults =
                    ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 10
                      impressions = 100
                    }
                }
            }
        }
      // Primitive 2
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = PRIMITIVE_REPORTING_SET_2_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.WEEKLY
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  nonCumulativeResults =
                    ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 20
                      impressions = 200
                    }
                }
            }
        }
      // Composite
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = COMPOSITE_REPORTING_SET_1_2_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.WEEKLY
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  nonCumulativeResults = ResultGroupKt.MetricSetKt.basicMetricSet { reach = 25 }
                }
            }
        }
    }

    val primitiveReportingSetByDataProviderId =
      mapOf(
        DATA_PROVIDER_1_ID to PRIMITIVE_REPORTING_SET_1,
        DATA_PROVIDER_2_ID to PRIMITIVE_REPORTING_SET_2,
      )

    val compositeReportingSetIdBySetExpression =
      mapOf(
        buildUnionSetExpression(
          listOf(PRIMITIVE_REPORTING_SET_1_ID, PRIMITIVE_REPORTING_SET_2_ID)
        ) to COMPOSITE_REPORTING_SET_1_2_ID
      )

    val resultGroups =
      buildResultGroups(
        basicReport,
        reportResult,
        primitiveReportingSetByDataProviderId,
        compositeReportingSetIdBySetExpression,
      )

    val expectedResultGroups =
      listOf(
        resultGroup {
          title = "result-group-1"
          results +=
            ResultGroupKt.result {
              metadata =
                ResultGroupKt.metricMetadata {
                  reportingUnitSummary =
                    ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_1_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg1"
                              }
                        }
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_2_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg2"
                              }
                        }
                    }
                  cumulativeMetricStartTime = REPORTING_INTERVAL.reportStart.toTimestamp()
                  metricEndTime =
                    REPORTING_INTERVAL.reportStart
                      .copy {
                        year = REPORTING_INTERVAL.reportEnd.year
                        month = REPORTING_INTERVAL.reportEnd.month
                        day = REPORTING_INTERVAL.reportEnd.day
                      }
                      .toTimestamp()
                  metricFrequencySpec = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
                  dimensionSpecSummary =
                    ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                      groupings += eventTemplateField {
                        path = "person.age_group"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                      }
                    }
                  filter = IMPRESSION_QUALIFICATION_FILTER_1
                }
              metricSet =
                ResultGroupKt.metricSet {
                  populationSize = 100
                  reportingUnit =
                    ResultGroupKt.MetricSetKt.reportingUnitMetricSet {
                      nonCumulative = ResultGroupKt.MetricSetKt.basicMetricSet { reach = 25 }
                    }
                  components +=
                    ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = DATA_PROVIDER_1_ID
                      value =
                        ResultGroupKt.MetricSetKt.componentMetricSet {
                          nonCumulative =
                            ResultGroupKt.MetricSetKt.basicMetricSet { impressions = 100 }
                          nonCumulativeUnique =
                            ResultGroupKt.MetricSetKt.uniqueMetricSet {
                              reach = 5 // 25 - 20
                            }
                        }
                    }
                  components +=
                    ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = DATA_PROVIDER_2_ID
                      value =
                        ResultGroupKt.MetricSetKt.componentMetricSet {
                          nonCumulative =
                            ResultGroupKt.MetricSetKt.basicMetricSet { impressions = 200 }
                          nonCumulativeUnique =
                            ResultGroupKt.MetricSetKt.uniqueMetricSet {
                              reach = 15 // 25 - 10
                            }
                        }
                    }
                }
            }
        }
      )

    assertThat(resultGroups)
      .ignoringRepeatedFieldOrder()
      .containsExactlyElementsIn(expectedResultGroups)
  }

  @Test
  fun `buildResultGroups creates result groups correctly when reporting unit with 3 components`() {
    val basicReport = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      details = basicReportDetails {
        impressionQualificationFilters += IMPRESSION_QUALIFICATION_FILTER_1
        reportingInterval = REPORTING_INTERVAL
        resultGroupSpecs += resultGroupSpec {
          title = "result-group-1"
          reportingUnit = reportingUnit {
            dataProviderKeys =
              ReportingUnitKt.dataProviderKeys {
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = DATA_PROVIDER_1_ID }
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = DATA_PROVIDER_2_ID }
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = DATA_PROVIDER_3_ID }
              }
          }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
          dimensionSpec = dimensionSpec {
            grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.age_group" }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            populationSize = true
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { impressions = true }
                nonCumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              }
          }
        }
      }
    }

    val reportResult = reportResult {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportStart = REPORTING_INTERVAL.reportStart
      // Primitive 1
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = PRIMITIVE_REPORTING_SET_1_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.WEEKLY
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  nonCumulativeResults =
                    ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 10
                      impressions = 100
                    }
                }
            }
        }
      // Primitive 2
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = PRIMITIVE_REPORTING_SET_2_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.WEEKLY
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  nonCumulativeResults =
                    ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 20
                      impressions = 200
                    }
                }
            }
        }
      // Primitive 3
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = PRIMITIVE_REPORTING_SET_3_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.WEEKLY
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  nonCumulativeResults =
                    ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 30
                      impressions = 300
                    }
                }
            }
        }
      // Composite
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = COMPOSITE_REPORTING_SET_1_2_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.WEEKLY
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  nonCumulativeResults = ResultGroupKt.MetricSetKt.basicMetricSet { reach = 25 }
                }
            }
        }
      // Composite 2
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = COMPOSITE_REPORTING_SET_2_3_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.WEEKLY
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  nonCumulativeResults = ResultGroupKt.MetricSetKt.basicMetricSet { reach = 30 }
                }
            }
        }
      // Composite 3
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = COMPOSITE_REPORTING_SET_1_3_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.WEEKLY
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  nonCumulativeResults = ResultGroupKt.MetricSetKt.basicMetricSet { reach = 45 }
                }
            }
        }
      // Composite 4
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = COMPOSITE_REPORTING_SET_1_2_3_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.WEEKLY
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  nonCumulativeResults = ResultGroupKt.MetricSetKt.basicMetricSet { reach = 55 }
                }
            }
        }
    }

    val primitiveReportingSetByDataProviderId =
      mapOf(
        DATA_PROVIDER_1_ID to PRIMITIVE_REPORTING_SET_1,
        DATA_PROVIDER_2_ID to PRIMITIVE_REPORTING_SET_2,
        DATA_PROVIDER_3_ID to PRIMITIVE_REPORTING_SET_3,
      )

    val compositeReportingSetIdBySetExpression =
      mapOf(
        buildUnionSetExpression(
          listOf(PRIMITIVE_REPORTING_SET_1_ID, PRIMITIVE_REPORTING_SET_2_ID)
        ) to COMPOSITE_REPORTING_SET_1_2_ID,
        buildUnionSetExpression(
          listOf(PRIMITIVE_REPORTING_SET_1_ID, PRIMITIVE_REPORTING_SET_3_ID)
        ) to COMPOSITE_REPORTING_SET_1_3_ID,
        buildUnionSetExpression(
          listOf(PRIMITIVE_REPORTING_SET_2_ID, PRIMITIVE_REPORTING_SET_3_ID)
        ) to COMPOSITE_REPORTING_SET_2_3_ID,
        buildUnionSetExpression(
          listOf(
            PRIMITIVE_REPORTING_SET_1_ID,
            PRIMITIVE_REPORTING_SET_2_ID,
            PRIMITIVE_REPORTING_SET_3_ID,
          )
        ) to COMPOSITE_REPORTING_SET_1_2_3_ID,
      )

    val resultGroups =
      buildResultGroups(
        basicReport,
        reportResult,
        primitiveReportingSetByDataProviderId,
        compositeReportingSetIdBySetExpression,
      )

    val expectedResultGroups =
      listOf(
        resultGroup {
          title = "result-group-1"
          results +=
            ResultGroupKt.result {
              metadata =
                ResultGroupKt.metricMetadata {
                  reportingUnitSummary =
                    ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_1_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg1"
                              }
                        }
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_2_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg2"
                              }
                        }
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_3_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg3"
                              }
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg4"
                              }
                        }
                    }
                  cumulativeMetricStartTime = REPORTING_INTERVAL.reportStart.toTimestamp()
                  metricEndTime =
                    REPORTING_INTERVAL.reportStart
                      .copy {
                        year = REPORTING_INTERVAL.reportEnd.year
                        month = REPORTING_INTERVAL.reportEnd.month
                        day = REPORTING_INTERVAL.reportEnd.day
                      }
                      .toTimestamp()
                  metricFrequencySpec = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
                  dimensionSpecSummary =
                    ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                      groupings += eventTemplateField {
                        path = "person.age_group"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                      }
                    }
                  filter = IMPRESSION_QUALIFICATION_FILTER_1
                }
              metricSet =
                ResultGroupKt.metricSet {
                  populationSize = 100
                  reportingUnit =
                    ResultGroupKt.MetricSetKt.reportingUnitMetricSet {
                      nonCumulative = ResultGroupKt.MetricSetKt.basicMetricSet { reach = 55 }
                    }
                  components +=
                    ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = DATA_PROVIDER_1_ID
                      value =
                        ResultGroupKt.MetricSetKt.componentMetricSet {
                          nonCumulative =
                            ResultGroupKt.MetricSetKt.basicMetricSet { impressions = 100 }
                          nonCumulativeUnique =
                            ResultGroupKt.MetricSetKt.uniqueMetricSet {
                              reach = 25 // 55 - 30
                            }
                        }
                    }
                  components +=
                    ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = DATA_PROVIDER_2_ID
                      value =
                        ResultGroupKt.MetricSetKt.componentMetricSet {
                          nonCumulative =
                            ResultGroupKt.MetricSetKt.basicMetricSet { impressions = 200 }
                          nonCumulativeUnique =
                            ResultGroupKt.MetricSetKt.uniqueMetricSet {
                              reach = 10 // 55 - 45
                            }
                        }
                    }
                  components +=
                    ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = DATA_PROVIDER_3_ID
                      value =
                        ResultGroupKt.MetricSetKt.componentMetricSet {
                          nonCumulative =
                            ResultGroupKt.MetricSetKt.basicMetricSet { impressions = 300 }
                          nonCumulativeUnique =
                            ResultGroupKt.MetricSetKt.uniqueMetricSet {
                              reach = 30 // 55 - 25
                            }
                        }
                    }
                }
            }
        }
      )

    assertThat(resultGroups)
      .ignoringRepeatedFieldOrder()
      .containsExactlyElementsIn(expectedResultGroups)
  }

  @Test
  fun `buildResultGroups creates different results for different IQFs`() {
    val basicReport = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      details = basicReportDetails {
        impressionQualificationFilters += IMPRESSION_QUALIFICATION_FILTER_1
        impressionQualificationFilters += CUSTOM_IMPRESSION_QUALIFICATION_FILTER
        reportingInterval = REPORTING_INTERVAL
        resultGroupSpecs += resultGroupSpec {
          title = "result-group-1"
          reportingUnit = reportingUnit {
            dataProviderKeys =
              ReportingUnitKt.dataProviderKeys {
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = DATA_PROVIDER_1_ID }
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = DATA_PROVIDER_2_ID }
              }
          }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.age_group" }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            populationSize = true
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { impressions = true }
              }
          }
        }
      }
    }

    val reportResult = reportResult {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportStart = REPORTING_INTERVAL.reportStart
      // Primitive 1
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = PRIMITIVE_REPORTING_SET_1_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.TOTAL
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults =
                    ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 10
                      impressions = 100
                    }
                }
            }
        }
      // Primitive 1 custom
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = PRIMITIVE_REPORTING_SET_1_ID
          custom = true
          metricFrequencyType = ReportResult.MetricFrequencyType.TOTAL
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults =
                    ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 20
                      impressions = 200
                    }
                }
            }
        }
      // Primitive 2
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = PRIMITIVE_REPORTING_SET_2_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.TOTAL
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults =
                    ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 30
                      impressions = 300
                    }
                }
            }
        }
      // Primitive 2 custom
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = PRIMITIVE_REPORTING_SET_2_ID
          custom = true
          metricFrequencyType = ReportResult.MetricFrequencyType.TOTAL
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults =
                    ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 40
                      impressions = 400
                    }
                }
            }
        }
    }

    val primitiveReportingSetByDataProviderId =
      mapOf(
        DATA_PROVIDER_1_ID to PRIMITIVE_REPORTING_SET_1,
        DATA_PROVIDER_2_ID to PRIMITIVE_REPORTING_SET_2,
      )

    val resultGroups =
      buildResultGroups(basicReport, reportResult, primitiveReportingSetByDataProviderId, mapOf())

    val expectedResultGroups =
      listOf(
        resultGroup {
          title = "result-group-1"
          results +=
            ResultGroupKt.result {
              metadata =
                ResultGroupKt.metricMetadata {
                  reportingUnitSummary =
                    ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_1_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg1"
                              }
                        }
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_2_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg2"
                              }
                        }
                    }
                  cumulativeMetricStartTime = REPORTING_INTERVAL.reportStart.toTimestamp()
                  metricEndTime =
                    REPORTING_INTERVAL.reportStart
                      .copy {
                        year = REPORTING_INTERVAL.reportEnd.year
                        month = REPORTING_INTERVAL.reportEnd.month
                        day = REPORTING_INTERVAL.reportEnd.day
                      }
                      .toTimestamp()
                  metricFrequencySpec = metricFrequencySpec { total = true }
                  dimensionSpecSummary =
                    ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                      groupings += eventTemplateField {
                        path = "person.age_group"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                      }
                    }
                  filter = IMPRESSION_QUALIFICATION_FILTER_1
                }
              metricSet =
                ResultGroupKt.metricSet {
                  populationSize = 100
                  components +=
                    ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = DATA_PROVIDER_1_ID
                      value =
                        ResultGroupKt.MetricSetKt.componentMetricSet {
                          cumulative =
                            ResultGroupKt.MetricSetKt.basicMetricSet { impressions = 100 }
                        }
                    }
                  components +=
                    ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = DATA_PROVIDER_2_ID
                      value =
                        ResultGroupKt.MetricSetKt.componentMetricSet {
                          cumulative =
                            ResultGroupKt.MetricSetKt.basicMetricSet { impressions = 300 }
                        }
                    }
                }
            }
          results +=
            ResultGroupKt.result {
              metadata =
                ResultGroupKt.metricMetadata {
                  reportingUnitSummary =
                    ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_1_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg1"
                              }
                        }
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_2_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg2"
                              }
                        }
                    }
                  cumulativeMetricStartTime = REPORTING_INTERVAL.reportStart.toTimestamp()
                  metricEndTime =
                    REPORTING_INTERVAL.reportStart
                      .copy {
                        year = REPORTING_INTERVAL.reportEnd.year
                        month = REPORTING_INTERVAL.reportEnd.month
                        day = REPORTING_INTERVAL.reportEnd.day
                      }
                      .toTimestamp()
                  metricFrequencySpec = metricFrequencySpec { total = true }
                  dimensionSpecSummary =
                    ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                      groupings += eventTemplateField {
                        path = "person.age_group"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                      }
                    }
                  filter = CUSTOM_IMPRESSION_QUALIFICATION_FILTER
                }
              metricSet =
                ResultGroupKt.metricSet {
                  populationSize = 100
                  components +=
                    ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = DATA_PROVIDER_1_ID
                      value =
                        ResultGroupKt.MetricSetKt.componentMetricSet {
                          cumulative =
                            ResultGroupKt.MetricSetKt.basicMetricSet { impressions = 200 }
                        }
                    }
                  components +=
                    ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = DATA_PROVIDER_2_ID
                      value =
                        ResultGroupKt.MetricSetKt.componentMetricSet {
                          cumulative =
                            ResultGroupKt.MetricSetKt.basicMetricSet { impressions = 400 }
                        }
                    }
                }
            }
        }
      )

    assertThat(resultGroups)
      .ignoringRepeatedFieldOrder()
      .containsExactlyElementsIn(expectedResultGroups)
  }

  @Test
  fun `buildResultGroups creates different results for different windows`() {
    val basicReport = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      details = basicReportDetails {
        impressionQualificationFilters += IMPRESSION_QUALIFICATION_FILTER_1
        reportingInterval = REPORTING_INTERVAL
        resultGroupSpecs += resultGroupSpec {
          title = "result-group-1"
          reportingUnit = reportingUnit {
            dataProviderKeys =
              ReportingUnitKt.dataProviderKeys {
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = DATA_PROVIDER_1_ID }
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = DATA_PROVIDER_2_ID }
              }
          }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
          dimensionSpec = dimensionSpec {
            grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.age_group" }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            populationSize = true
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { impressions = true }
              }
          }
        }
      }
    }

    val reportResult = reportResult {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportStart = REPORTING_INTERVAL.reportStart
      // Primitive 1
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = PRIMITIVE_REPORTING_SET_1_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.WEEKLY
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowStartDate =
                REPORTING_INTERVAL.reportEnd.copy {
                  day = REPORTING_INTERVAL.reportStart.day
                  month = REPORTING_INTERVAL.reportStart.month
                  year = REPORTING_INTERVAL.reportStart.year
                }
              windowEndDate =
                REPORTING_INTERVAL.reportEnd.copy {
                  day = REPORTING_INTERVAL.reportStart.day + 7
                  month = REPORTING_INTERVAL.reportStart.month
                  year = REPORTING_INTERVAL.reportStart.year
                }
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults =
                    ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 10
                      impressions = 100
                    }
                }
            }
        }
      // Primitive 1 week 2
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = PRIMITIVE_REPORTING_SET_1_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.WEEKLY
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowStartDate =
                REPORTING_INTERVAL.reportEnd.copy {
                  day = REPORTING_INTERVAL.reportStart.day + 7
                  month = REPORTING_INTERVAL.reportStart.month
                  year = REPORTING_INTERVAL.reportStart.year
                }
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults =
                    ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 20
                      impressions = 200
                    }
                }
            }
        }
      // Primitive 2
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = PRIMITIVE_REPORTING_SET_2_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.WEEKLY
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowStartDate =
                REPORTING_INTERVAL.reportEnd.copy {
                  day = REPORTING_INTERVAL.reportStart.day
                  month = REPORTING_INTERVAL.reportStart.month
                  year = REPORTING_INTERVAL.reportStart.year
                }
              windowEndDate =
                REPORTING_INTERVAL.reportEnd.copy {
                  day = REPORTING_INTERVAL.reportStart.day + 7
                  month = REPORTING_INTERVAL.reportStart.month
                  year = REPORTING_INTERVAL.reportStart.year
                }
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults =
                    ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 30
                      impressions = 300
                    }
                }
            }
        }
      // Primitive 2 week 2
      reportingSetResults +=
        ReportResultKt.reportingSetResult {
          externalReportingSetId = PRIMITIVE_REPORTING_SET_2_ID
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_1.externalImpressionQualificationFilterId
          metricFrequencyType = ReportResult.MetricFrequencyType.WEEKLY
          groupings += eventTemplateField {
            path = "person.age_group"
            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
          }
          populationSize = 100
          reportingWindowResults +=
            ReportResultKt.ReportingSetResultKt.reportingWindowResult {
              windowStartDate =
                REPORTING_INTERVAL.reportEnd.copy {
                  day = REPORTING_INTERVAL.reportStart.day + 7
                  month = REPORTING_INTERVAL.reportStart.month
                  year = REPORTING_INTERVAL.reportStart.year
                }
              windowEndDate = REPORTING_INTERVAL.reportEnd
              denoisedReportResultValues =
                ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults =
                    ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 40
                      impressions = 400
                    }
                }
            }
        }
    }

    val primitiveReportingSetByDataProviderId =
      mapOf(
        DATA_PROVIDER_1_ID to PRIMITIVE_REPORTING_SET_1,
        DATA_PROVIDER_2_ID to PRIMITIVE_REPORTING_SET_2,
      )

    val resultGroups =
      buildResultGroups(basicReport, reportResult, primitiveReportingSetByDataProviderId, mapOf())

    val expectedResultGroups =
      listOf(
        resultGroup {
          title = "result-group-1"
          results +=
            ResultGroupKt.result {
              metadata =
                ResultGroupKt.metricMetadata {
                  reportingUnitSummary =
                    ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_1_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg1"
                              }
                        }
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_2_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg2"
                              }
                        }
                    }
                  nonCumulativeMetricStartTime = REPORTING_INTERVAL.reportStart.toTimestamp()
                  cumulativeMetricStartTime = REPORTING_INTERVAL.reportStart.toTimestamp()
                  metricEndTime =
                    REPORTING_INTERVAL.reportStart
                      .copy { day = REPORTING_INTERVAL.reportStart.day + 7 }
                      .toTimestamp()
                  metricFrequencySpec = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
                  dimensionSpecSummary =
                    ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                      groupings += eventTemplateField {
                        path = "person.age_group"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                      }
                    }
                  filter = IMPRESSION_QUALIFICATION_FILTER_1
                }
              metricSet =
                ResultGroupKt.metricSet {
                  populationSize = 100
                  components +=
                    ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = DATA_PROVIDER_1_ID
                      value =
                        ResultGroupKt.MetricSetKt.componentMetricSet {
                          cumulative =
                            ResultGroupKt.MetricSetKt.basicMetricSet { impressions = 100 }
                        }
                    }
                  components +=
                    ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = DATA_PROVIDER_2_ID
                      value =
                        ResultGroupKt.MetricSetKt.componentMetricSet {
                          cumulative =
                            ResultGroupKt.MetricSetKt.basicMetricSet { impressions = 300 }
                        }
                    }
                }
            }
          results +=
            ResultGroupKt.result {
              metadata =
                ResultGroupKt.metricMetadata {
                  reportingUnitSummary =
                    ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_1_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg1"
                              }
                        }
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderId = DATA_PROVIDER_2_ID
                          eventGroupSummaries +=
                            ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                cmmsEventGroupId = "eg2"
                              }
                        }
                    }
                  nonCumulativeMetricStartTime =
                    REPORTING_INTERVAL.reportStart
                      .copy { day = REPORTING_INTERVAL.reportStart.day + 7 }
                      .toTimestamp()
                  cumulativeMetricStartTime = REPORTING_INTERVAL.reportStart.toTimestamp()
                  metricEndTime =
                    REPORTING_INTERVAL.reportStart
                      .copy {
                        year = REPORTING_INTERVAL.reportEnd.year
                        month = REPORTING_INTERVAL.reportEnd.month
                        day = REPORTING_INTERVAL.reportEnd.day
                      }
                      .toTimestamp()
                  metricFrequencySpec = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
                  dimensionSpecSummary =
                    ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                      groupings += eventTemplateField {
                        path = "person.age_group"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                      }
                    }
                  filter = IMPRESSION_QUALIFICATION_FILTER_1
                }
              metricSet =
                ResultGroupKt.metricSet {
                  populationSize = 100
                  components +=
                    ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = DATA_PROVIDER_1_ID
                      value =
                        ResultGroupKt.MetricSetKt.componentMetricSet {
                          cumulative =
                            ResultGroupKt.MetricSetKt.basicMetricSet { impressions = 200 }
                        }
                    }
                  components +=
                    ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = DATA_PROVIDER_2_ID
                      value =
                        ResultGroupKt.MetricSetKt.componentMetricSet {
                          cumulative =
                            ResultGroupKt.MetricSetKt.basicMetricSet { impressions = 400 }
                        }
                    }
                }
            }
        }
      )

    assertThat(resultGroups)
      .ignoringRepeatedFieldOrder()
      .containsExactlyElementsIn(expectedResultGroups)
  }

  private fun buildUnionSetExpression(
    externalReportingSetIds: List<String>
  ): ReportingSet.SetExpression {
    var setExpression =
      ReportingSetKt.setExpression {
        operation = ReportingSet.SetExpression.Operation.UNION
        lhs =
          ReportingSetKt.SetExpressionKt.operand {
            externalReportingSetId = externalReportingSetIds.first()
          }
      }

    for (externalReportingSetId in
      externalReportingSetIds.subList(1, externalReportingSetIds.size)) {
      setExpression =
        ReportingSetKt.setExpression {
          operation = ReportingSet.SetExpression.Operation.UNION
          lhs =
            ReportingSetKt.SetExpressionKt.operand {
              this.externalReportingSetId = externalReportingSetId
            }
          rhs = ReportingSetKt.SetExpressionKt.operand { expression = setExpression }
        }
    }

    return setExpression
  }

  companion object {
    private const val CMMS_MEASUREMENT_CONSUMER_ID = "mc1"
    private const val DATA_PROVIDER_1_ID = "dp1"
    private const val DATA_PROVIDER_2_ID = "dp2"
    private const val DATA_PROVIDER_3_ID = "dp3"

    private const val PRIMITIVE_REPORTING_SET_1_ID = "p1"
    private const val PRIMITIVE_REPORTING_SET_2_ID = "p2"
    private const val PRIMITIVE_REPORTING_SET_3_ID = "p3"
    private const val COMPOSITE_REPORTING_SET_1_2_ID = "c12"
    private const val COMPOSITE_REPORTING_SET_1_2_3_ID = "c123"
    private const val COMPOSITE_REPORTING_SET_1_3_ID = "c13"
    private const val COMPOSITE_REPORTING_SET_2_3_ID = "c23"

    private val CUSTOM_IMPRESSION_QUALIFICATION_FILTER = reportingImpressionQualificationFilter {
      filterSpecs += impressionQualificationFilterSpec {
        mediaType = ImpressionQualificationFilterSpec.MediaType.VIDEO
      }
    }

    private val IMPRESSION_QUALIFICATION_FILTER_1 = reportingImpressionQualificationFilter {
      externalImpressionQualificationFilterId = "iqf-1"
      filterSpecs += impressionQualificationFilterSpec {
        mediaType = ImpressionQualificationFilterSpec.MediaType.DISPLAY
      }
    }

    private val REPORTING_INTERVAL = reportingInterval {
      reportStart = dateTime {
        year = 2025
        month = 1
        day = 1
        timeZone = timeZone { id = "UTC" }
      }
      reportEnd = date {
        year = 2025
        month = 1
        day = 31
      }
    }

    private val PRIMITIVE_REPORTING_SET_1 = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = PRIMITIVE_REPORTING_SET_1_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = DATA_PROVIDER_1_ID
              cmmsEventGroupId = "eg1"
            }
        }
    }

    private val PRIMITIVE_REPORTING_SET_2 = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = PRIMITIVE_REPORTING_SET_2_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = DATA_PROVIDER_2_ID
              cmmsEventGroupId = "eg2"
            }
        }
    }

    private val PRIMITIVE_REPORTING_SET_3 = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = PRIMITIVE_REPORTING_SET_3_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = DATA_PROVIDER_3_ID
              cmmsEventGroupId = "eg3"
            }
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = DATA_PROVIDER_3_ID
              cmmsEventGroupId = "eg4"
            }
        }
    }
  }
}
