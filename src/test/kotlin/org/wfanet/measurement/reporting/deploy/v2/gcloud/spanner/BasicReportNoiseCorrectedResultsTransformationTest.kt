package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner

import com.google.common.truth.Truth
import com.google.type.DayOfWeek
import com.google.type.date
import com.google.type.dateTime
import com.google.type.timeZone
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.ReportResult
import org.wfanet.measurement.internal.reporting.v2.ReportResultKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ResultGroupKt
import org.wfanet.measurement.internal.reporting.v2.basicReport
import org.wfanet.measurement.internal.reporting.v2.basicReportDetails
import org.wfanet.measurement.internal.reporting.v2.dimensionSpec
import org.wfanet.measurement.internal.reporting.v2.eventFilter
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.reportResult
import org.wfanet.measurement.internal.reporting.v2.reportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.reportingInterval
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.internal.reporting.v2.resultGroup
import org.wfanet.measurement.internal.reporting.v2.resultGroupSpec

@RunWith(JUnit4::class)
class BasicReportNoiseCorrectedResultsTransformationTest {
  @Test
  fun `transform`(): Unit =
    runBlocking {
      val basicReport = basicReport {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportId = "1234"
        externalCampaignGroupId = CAMPAIGN_GROUP.externalReportingSetId
        details = basicReportDetails {
          impressionQualificationFilters += reportingImpressionQualificationFilter {
            externalImpressionQualificationFilterId = "IQF"
            filterSpecs += impressionQualificationFilterSpec {
              mediaType = ImpressionQualificationFilterSpec.MediaType.DISPLAY
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "banner_ad.viewable"
                  value = EventTemplateFieldKt.fieldValue { boolValue = true }
                }
              }
            }
          }
          impressionQualificationFilters += reportingImpressionQualificationFilter {
            filterSpecs += impressionQualificationFilterSpec {
              mediaType = ImpressionQualificationFilterSpec.MediaType.VIDEO
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "video_ad.viewed_fraction"
                  value = EventTemplateFieldKt.fieldValue { floatValue = 1.0f }
                }
              }
            }
            filterSpecs += impressionQualificationFilterSpec {
              mediaType = ImpressionQualificationFilterSpec.MediaType.DISPLAY
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "banner_ad.viewable"
                  value = EventTemplateFieldKt.fieldValue { boolValue = true }
                }
              }
            }
          }
          reportingInterval = reportingInterval {
            reportStart = dateTime {
              year = 2025
              month = 1
              day = 6
              timeZone = timeZone { id = "America/Los_Angeles" }
            }
            reportEnd = date {
              year = 2025
              month = 1
              day = 2
            }
          }
          resultGroupSpecs += resultGroupSpec {
            dimensionSpec = dimensionSpec {
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "person.age_group"
                  value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                }
              }
            }
          }
          resultGroupSpecs += resultGroupSpec {
            dimensionSpec = dimensionSpec {
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "person.age_group"
                  value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_35_TO_54" }
                }
              }
            }
          }
        }
      }

      val reportResult = reportResult {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        reportStart = basicReport.details.reportingInterval.reportStart

        reportingSetResults +=
          ReportResultKt.reportingSetResult {
            externalReportingSetId = PRIMITIVE_REPORTING_SET.externalReportingSetId
            vennDiagramRegionType = ReportResult.VennDiagramRegionType.PRIMITIVE
            custom = true
            metricFrequencyType = ReportResult.MetricFrequencyType.WEEKLY
            groupings += eventTemplateField {
              path = "person.age_group"
              value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
            }
            groupings += eventTemplateField {
              path = "person.gender"
              value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
            }
            eventFilters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            populationSize = 1000
            reportingWindowResults +=
              ReportResultKt.ReportingSetResultKt.reportingWindowResult {
                windowStartDate = date {
                  year = 2025
                  month = 1
                  day = 6
                }
                windowEndDate = date {
                  year = 2025
                  month = 1
                  day = 13
                }
                noisyReportResultValues =
                  ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt
                    .noisyReportResultValues {
                      cumulativeResults =
                        ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt
                          .NoisyReportResultValuesKt
                          .noisyMetricSet {
                            reach =
                              ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt
                                .NoisyReportResultValuesKt
                                .NoisyMetricSetKt
                                .reachResult { value = 1 }
                            impressionCount =
                              ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt
                                .NoisyReportResultValuesKt
                                .NoisyMetricSetKt
                                .impressionCountResult { value = 1 }
                          }
                      nonCumulativeResults =
                        ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt
                          .NoisyReportResultValuesKt
                          .noisyMetricSet {
                            reach =
                              ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt
                                .NoisyReportResultValuesKt
                                .NoisyMetricSetKt
                                .reachResult { value = 1 }
                            impressionCount =
                              ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt
                                .NoisyReportResultValuesKt
                                .NoisyMetricSetKt
                                .impressionCountResult { value = 1 }
                          }
                    }
                denoisedReportResultValues =
                  ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                    cumulativeResults = ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 1
                      impressions = 1
                    }
                    nonCumulativeResults = ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 1
                      impressions = 1
                    }
                  }
              }
            reportingWindowResults +=
              ReportResultKt.ReportingSetResultKt.reportingWindowResult {
                windowStartDate = date {
                  year = 2025
                  month = 1
                  day = 13
                }
                windowEndDate = date {
                  year = 2025
                  month = 1
                  day = 20
                }
                noisyReportResultValues =
                  ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt
                    .noisyReportResultValues {
                      nonCumulativeResults =
                        ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt
                          .NoisyReportResultValuesKt
                          .noisyMetricSet {
                            impressionCount =
                              ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt
                                .NoisyReportResultValuesKt
                                .NoisyMetricSetKt
                                .impressionCountResult { value = 1 }
                          }
                    }
                denoisedReportResultValues =
                  ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                    cumulativeResults = ResultGroupKt.MetricSetKt.basicMetricSet {
                      impressions = 1
                    }
                    nonCumulativeResults = ResultGroupKt.MetricSetKt.basicMetricSet {
                      impressions = 1
                    }
                  }
              }
          }

        reportingSetResults +=
          ReportResultKt.reportingSetResult {
            externalReportingSetId = PRIMITIVE_REPORTING_SET.externalReportingSetId
            vennDiagramRegionType = ReportResult.VennDiagramRegionType.PRIMITIVE
            custom = true
            metricFrequencyType = ReportResult.MetricFrequencyType.WEEKLY
            groupings += eventTemplateField {
              path = "person.age_group"
              value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_35_TO_54" }
            }
            groupings += eventTemplateField {
              path = "person.gender"
              value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
            }
            eventFilters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            reportingWindowResults +=
              ReportResultKt.ReportingSetResultKt.reportingWindowResult {
                windowStartDate = date {
                  year = 2025
                  month = 1
                  day = 6
                }
                windowEndDate = date {
                  year = 2025
                  month = 1
                  day = 13
                }
                noisyReportResultValues =
                  ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt
                    .noisyReportResultValues {
                      nonCumulativeResults =
                        ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt
                          .NoisyReportResultValuesKt
                          .noisyMetricSet {
                            impressionCount =
                              ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt
                                .NoisyReportResultValuesKt
                                .NoisyMetricSetKt
                                .impressionCountResult { value = 1 }
                          }
                    }
                denoisedReportResultValues =
                  ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                    cumulativeResults = ResultGroupKt.MetricSetKt.basicMetricSet {
                      impressions = 1
                    }
                    nonCumulativeResults = ResultGroupKt.MetricSetKt.basicMetricSet {
                      impressions = 1
                    }
                  }
              }
          }

        reportingSetResults +=
          ReportResultKt.reportingSetResult {
            externalReportingSetId = COMPOSITE_REPORTING_SET.externalReportingSetId
            vennDiagramRegionType = ReportResult.VennDiagramRegionType.UNION
            custom = true
            metricFrequencyType = ReportResult.MetricFrequencyType.TOTAL
            groupings += eventTemplateField {
              path = "person.age_group"
              value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
            }
            groupings += eventTemplateField {
              path = "person.gender"
              value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
            }
            eventFilters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_35_TO_54" }
              }
            }
            reportingWindowResults +=
              ReportResultKt.ReportingSetResultKt.reportingWindowResult {
                windowEndDate = date {
                  year = 2025
                  month = 1
                  day = 13
                }
                noisyReportResultValues =
                  ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt
                    .noisyReportResultValues {
                      cumulativeResults =
                        ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt
                          .NoisyReportResultValuesKt
                          .noisyMetricSet {
                            reach =
                              ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt
                                .NoisyReportResultValuesKt
                                .NoisyMetricSetKt
                                .reachResult { value = 1 }
                          }
                    }
                denoisedReportResultValues =
                  ReportResultKt.ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                    cumulativeResults = ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 1
                    }
                    nonCumulativeResults = ResultGroupKt.MetricSetKt.basicMetricSet {
                      reach = 1
                    }
                  }
              }
          }
      }

      val resultGroups = buildResultGroups(basicReport, reportResult, mapOf(), mapOf())
      Truth.assertThat(resultGroups)
        .containsExactly(
          resultGroup {

          },
          resultGroup {

          }
        )
    }

  companion object {
    private const val CMMS_MEASUREMENT_CONSUMER_ID = "A123"
    private const val MEASUREMENT_CONSUMER_NAME =
      "measurementConsumers/${CMMS_MEASUREMENT_CONSUMER_ID}"

    private const val COMPOSITE_REPORTING_SET_ID = "c124"
    private const val COMPOSITE_REPORTING_SET_NAME =
      "${MEASUREMENT_CONSUMER_NAME}/reportingSets/${COMPOSITE_REPORTING_SET_ID}"

    private val CAMPAIGN_GROUP = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = "1234"
      externalCampaignGroupId = "1234"
      displayName = "displayName"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = "1234"
              cmmsEventGroupId = "1234"
            }

          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = "1234"
              cmmsEventGroupId = "1235"
            }
        }
    }

    private val BASIC_REPORT = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportId = "1234"
      externalCampaignGroupId = CAMPAIGN_GROUP.externalReportingSetId
      details = basicReportDetails {
        impressionQualificationFilters += reportingImpressionQualificationFilter {
          filterSpecs += impressionQualificationFilterSpec {
            mediaType = ImpressionQualificationFilterSpec.MediaType.DISPLAY
            filters += eventFilter {
              terms += eventTemplateField {
                path = "banner_ad.viewable"
                value = EventTemplateFieldKt.fieldValue { boolValue = true }
              }
            }
          }
        }
        reportingInterval = reportingInterval {
          reportStart = dateTime {
            year = 2025
            month = 1
            day = 6
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2025
            month = 1
            day = 2
          }
        }
        resultGroupSpecs += resultGroupSpec { dimensionSpec = dimensionSpec {} }
      }
    }

    private val PRIMITIVE_REPORTING_SET = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = "p1234"
      externalCampaignGroupId = CAMPAIGN_GROUP.externalReportingSetId

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = "1234"
              cmmsEventGroupId = "1234"
            }
        }
    }

    private val PRIMITIVE_REPORTING_SET_2 = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = "p12342"
      externalCampaignGroupId = CAMPAIGN_GROUP.externalReportingSetId

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = "1234"
              cmmsEventGroupId = "1235"
            }
        }
    }

    private val COMPOSITE_REPORTING_SET = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = "c1234"
      externalCampaignGroupId = CAMPAIGN_GROUP.externalReportingSetId

      composite =
        ReportingSetKt.setExpression {
          lhs =
            ReportingSetKt.SetExpressionKt.operand {
              externalReportingSetId = PRIMITIVE_REPORTING_SET.externalReportingSetId
            }
          rhs =
            ReportingSetKt.SetExpressionKt.operand {
              externalReportingSetId = PRIMITIVE_REPORTING_SET_2.externalReportingSetId
            }
        }
    }
  }
}
