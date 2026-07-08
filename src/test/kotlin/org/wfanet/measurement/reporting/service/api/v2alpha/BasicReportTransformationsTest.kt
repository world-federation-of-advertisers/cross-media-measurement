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
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.type.DayOfWeek
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.EventMessageDescriptor
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.cel.CelPredicates
import org.wfanet.measurement.common.cel.CelValidationException
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.reporting.service.api.InvalidFieldValueException
import org.wfanet.measurement.reporting.v2alpha.DimensionSpecKt
import org.wfanet.measurement.reporting.v2alpha.EventTemplateFieldKt
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.dimensionSpec
import org.wfanet.measurement.reporting.v2alpha.eventFilter
import org.wfanet.measurement.reporting.v2alpha.eventTemplateField
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.reportingSet
import org.wfanet.measurement.reporting.v2alpha.reportingUnit
import org.wfanet.measurement.reporting.v2alpha.resultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.resultGroupSpec

@RunWith(JUnit4::class)
class BasicReportTransformationsTest {
  @Test
  fun `weekly resultGroupSpec with reportingUnitMetricSetSpec transforms into correct map`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit {
            components += DATA_PROVIDER_NAME_1
            components += DATA_PROVIDER_NAME_2
          }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.WEDNESDAY }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                nonCumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    averageFrequency = true
                    impressions = true
                  }
                cumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    averageFrequency = true
                    impressions = true
                  }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(2)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        listOf(
          MetricCalculationSpecKt.details {
            filter = "person.age_group == 1 && person.gender == 1"
            metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
          }
        ),
      )
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        reportingSet {
          campaignGroup = CAMPAIGN_GROUP_NAME
          composite =
            ReportingSetKt.composite {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.UNION
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = PRIMITIVE_REPORTING_SET_NAME_2
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      expression =
                        ReportingSetKt.setExpression {
                          operation = ReportingSet.SetExpression.Operation.UNION
                          lhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              reportingSet = PRIMITIVE_REPORTING_SET_NAME_1
                            }
                        }
                    }
                }
            }
        },
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  weekly =
                    MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                      dayOfWeek = DayOfWeek.WEDNESDAY
                    }
                }
              trailingWindow =
                MetricCalculationSpecKt.trailingWindow {
                  count = 1
                  increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
                }
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
              metricSpecs += metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  weekly =
                    MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                      dayOfWeek = DayOfWeek.WEDNESDAY
                    }
                }
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
              metricSpecs += metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `total resultGroupSpec with reportingUnitMetricSetSpec transforms into correct map`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit {
            components += DATA_PROVIDER_NAME_1
            components += DATA_PROVIDER_NAME_2
          }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    averageFrequency = true
                    impressions = true
                  }
                stackedIncrementalReach = true
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(2)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "person.age_group == 1 && person.gender == 1"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        reportingSet {
          campaignGroup = CAMPAIGN_GROUP_NAME
          composite =
            ReportingSetKt.composite {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.UNION
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = PRIMITIVE_REPORTING_SET_NAME_2
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      expression =
                        ReportingSetKt.setExpression {
                          operation = ReportingSet.SetExpression.Operation.UNION
                          lhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              reportingSet = PRIMITIVE_REPORTING_SET_NAME_1
                            }
                        }
                    }
                }
            }
        },
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
              metricSpecs += metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `weekly resultGroupSpec with componentMetricSetSpec transforms into correct map`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit {
            components += DATA_PROVIDER_NAME_1
            components += DATA_PROVIDER_NAME_2
          }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.WEDNESDAY }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                nonCumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    averageFrequency = true
                    impressions = true
                  }
                cumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    averageFrequency = true
                    impressions = true
                  }
                nonCumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
                cumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(3)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "person.age_group == 1 && person.gender == 1"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  weekly =
                    MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                      dayOfWeek = DayOfWeek.WEDNESDAY
                    }
                }
              trailingWindow =
                MetricCalculationSpecKt.trailingWindow {
                  count = 1
                  increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
                }
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
              metricSpecs += metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  weekly =
                    MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                      dayOfWeek = DayOfWeek.WEDNESDAY
                    }
                }
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
              metricSpecs += metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
            }
          )
        },
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_2,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  weekly =
                    MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                      dayOfWeek = DayOfWeek.WEDNESDAY
                    }
                }
              trailingWindow =
                MetricCalculationSpecKt.trailingWindow {
                  count = 1
                  increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
                }
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
              metricSpecs += metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  weekly =
                    MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                      dayOfWeek = DayOfWeek.WEDNESDAY
                    }
                }
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
              metricSpecs += metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
            }
          )
        },
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        reportingSet {
          campaignGroup = CAMPAIGN_GROUP_NAME
          composite =
            ReportingSetKt.composite {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.UNION
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = PRIMITIVE_REPORTING_SET_NAME_2
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      expression =
                        ReportingSetKt.setExpression {
                          operation = ReportingSet.SetExpression.Operation.UNION
                          lhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              reportingSet = PRIMITIVE_REPORTING_SET_NAME_1
                            }
                        }
                    }
                }
            }
        },
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  weekly =
                    MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                      dayOfWeek = DayOfWeek.WEDNESDAY
                    }
                }
              trailingWindow =
                MetricCalculationSpecKt.trailingWindow {
                  count = 1
                  increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
                }
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  weekly =
                    MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                      dayOfWeek = DayOfWeek.WEDNESDAY
                    }
                }
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `total resultGroupSpec with componentMetricSetSpec transforms into correct map`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit {
            components += DATA_PROVIDER_NAME_1
            components += DATA_PROVIDER_NAME_2
          }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                cumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    averageFrequency = true
                    impressions = true
                  }
                cumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(3)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "person.age_group == 1 && person.gender == 1"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
              metricSpecs += metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
            }
          )
        },
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_2,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
              metricSpecs += metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
            }
          )
        },
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        reportingSet {
          campaignGroup = CAMPAIGN_GROUP_NAME
          composite =
            ReportingSetKt.composite {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.UNION
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = PRIMITIVE_REPORTING_SET_NAME_2
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      expression =
                        ReportingSetKt.setExpression {
                          operation = ReportingSet.SetExpression.Operation.UNION
                          lhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              reportingSet = PRIMITIVE_REPORTING_SET_NAME_1
                            }
                        }
                    }
                }
            }
        },
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `componentMetricSetSpec without uniqueMetricSetSpec transforms into correct map`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit {
            components += DATA_PROVIDER_NAME_1
            components += DATA_PROVIDER_NAME_2
          }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                cumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    averageFrequency = true
                    impressions = true
                  }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(2)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "person.age_group == 1 && person.gender == 1"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
              metricSpecs += metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
            }
          )
        },
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_2,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
              metricSpecs += metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `noncumulative uniqueMetricSetSpec only transforms into correct map`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit {
            components += DATA_PROVIDER_NAME_1
            components += DATA_PROVIDER_NAME_2
          }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.WEDNESDAY }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                nonCumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(3)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "person.age_group == 1 && person.gender == 1"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  weekly =
                    MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                      dayOfWeek = DayOfWeek.WEDNESDAY
                    }
                }
              trailingWindow =
                MetricCalculationSpecKt.trailingWindow {
                  count = 1
                  increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
                }
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_2,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  weekly =
                    MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                      dayOfWeek = DayOfWeek.WEDNESDAY
                    }
                }
              trailingWindow =
                MetricCalculationSpecKt.trailingWindow {
                  count = 1
                  increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
                }
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        reportingSet {
          campaignGroup = CAMPAIGN_GROUP_NAME
          composite =
            ReportingSetKt.composite {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.UNION
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = PRIMITIVE_REPORTING_SET_NAME_2
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      expression =
                        ReportingSetKt.setExpression {
                          operation = ReportingSet.SetExpression.Operation.UNION
                          lhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              reportingSet = PRIMITIVE_REPORTING_SET_NAME_1
                            }
                        }
                    }
                }
            }
        },
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  weekly =
                    MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                      dayOfWeek = DayOfWeek.WEDNESDAY
                    }
                }
              trailingWindow =
                MetricCalculationSpecKt.trailingWindow {
                  count = 1
                  increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
                }
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `uniqueMetricSetSpec with 1 component transforms into map with only population`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.WEDNESDAY }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                nonCumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        listOf(
          MetricCalculationSpecKt.details {
            filter = "person.age_group == 1 && person.gender == 1"
            metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
          }
        ),
      )
  }

  @Test
  fun `noncumulative uniqueMetricSetSpec with 3 components transforms into correct map`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
      put(DATA_PROVIDER_NAME_3, PRIMITIVE_REPORTING_SET_3)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit {
            components += DATA_PROVIDER_NAME_1
            components += DATA_PROVIDER_NAME_2
            components += DATA_PROVIDER_NAME_3
          }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.WEDNESDAY }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                nonCumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(5)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        listOf(
          MetricCalculationSpecKt.details {
            filter = "person.age_group == 1 && person.gender == 1"
            metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
          }
        ),
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        reportingSet {
          campaignGroup = CAMPAIGN_GROUP_NAME
          composite =
            ReportingSetKt.composite {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.UNION
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = PRIMITIVE_REPORTING_SET_NAME_2
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      expression =
                        ReportingSetKt.setExpression {
                          operation = ReportingSet.SetExpression.Operation.UNION
                          lhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              reportingSet = PRIMITIVE_REPORTING_SET_NAME_1
                            }
                        }
                    }
                }
            }
        },
        buildList {
          add(
            MetricCalculationSpecKt.details {
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  weekly =
                    MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                      dayOfWeek = DayOfWeek.WEDNESDAY
                    }
                }
              trailingWindow =
                MetricCalculationSpecKt.trailingWindow {
                  count = 1
                  increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
                }
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        reportingSet {
          campaignGroup = CAMPAIGN_GROUP_NAME
          composite =
            ReportingSetKt.composite {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.UNION
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = PRIMITIVE_REPORTING_SET_NAME_3
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      expression =
                        ReportingSetKt.setExpression {
                          operation = ReportingSet.SetExpression.Operation.UNION
                          lhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              reportingSet = PRIMITIVE_REPORTING_SET_NAME_1
                            }
                        }
                    }
                }
            }
        },
        buildList {
          add(
            MetricCalculationSpecKt.details {
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  weekly =
                    MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                      dayOfWeek = DayOfWeek.WEDNESDAY
                    }
                }
              trailingWindow =
                MetricCalculationSpecKt.trailingWindow {
                  count = 1
                  increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
                }
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        reportingSet {
          campaignGroup = CAMPAIGN_GROUP_NAME
          composite =
            ReportingSetKt.composite {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.UNION
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = PRIMITIVE_REPORTING_SET_NAME_3
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      expression =
                        ReportingSetKt.setExpression {
                          operation = ReportingSet.SetExpression.Operation.UNION
                          lhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              reportingSet = PRIMITIVE_REPORTING_SET_NAME_2
                            }
                        }
                    }
                }
            }
        },
        buildList {
          add(
            MetricCalculationSpecKt.details {
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  weekly =
                    MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                      dayOfWeek = DayOfWeek.WEDNESDAY
                    }
                }
              trailingWindow =
                MetricCalculationSpecKt.trailingWindow {
                  count = 1
                  increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
                }
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        reportingSet {
          campaignGroup = CAMPAIGN_GROUP_NAME
          composite =
            ReportingSetKt.composite {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.UNION
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = PRIMITIVE_REPORTING_SET_NAME_3
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      expression =
                        ReportingSetKt.setExpression {
                          operation = ReportingSet.SetExpression.Operation.UNION
                          lhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              reportingSet = PRIMITIVE_REPORTING_SET_NAME_2
                            }
                          rhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              expression =
                                ReportingSetKt.setExpression {
                                  operation = ReportingSet.SetExpression.Operation.UNION
                                  lhs =
                                    ReportingSetKt.SetExpressionKt.operand {
                                      reportingSet = PRIMITIVE_REPORTING_SET_NAME_1
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        buildList {
          add(
            MetricCalculationSpecKt.details {
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  weekly =
                    MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                      dayOfWeek = DayOfWeek.WEDNESDAY
                    }
                }
              trailingWindow =
                MetricCalculationSpecKt.trailingWindow {
                  count = 1
                  increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
                }
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `cumulative uniqueMetricSetSpec with 3 components transforms into correct map`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
      put(DATA_PROVIDER_NAME_3, PRIMITIVE_REPORTING_SET_3)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit {
            components += DATA_PROVIDER_NAME_1
            components += DATA_PROVIDER_NAME_2
            components += DATA_PROVIDER_NAME_3
          }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                cumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(5)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        listOf(
          MetricCalculationSpecKt.details {
            filter = "person.age_group == 1 && person.gender == 1"
            metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
          }
        ),
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        reportingSet {
          campaignGroup = CAMPAIGN_GROUP_NAME
          composite =
            ReportingSetKt.composite {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.UNION
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = PRIMITIVE_REPORTING_SET_NAME_2
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      expression =
                        ReportingSetKt.setExpression {
                          operation = ReportingSet.SetExpression.Operation.UNION
                          lhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              reportingSet = PRIMITIVE_REPORTING_SET_NAME_1
                            }
                        }
                    }
                }
            }
        },
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        reportingSet {
          campaignGroup = CAMPAIGN_GROUP_NAME
          composite =
            ReportingSetKt.composite {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.UNION
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = PRIMITIVE_REPORTING_SET_NAME_3
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      expression =
                        ReportingSetKt.setExpression {
                          operation = ReportingSet.SetExpression.Operation.UNION
                          lhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              reportingSet = PRIMITIVE_REPORTING_SET_NAME_1
                            }
                        }
                    }
                }
            }
        },
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        reportingSet {
          campaignGroup = CAMPAIGN_GROUP_NAME
          composite =
            ReportingSetKt.composite {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.UNION
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = PRIMITIVE_REPORTING_SET_NAME_3
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      expression =
                        ReportingSetKt.setExpression {
                          operation = ReportingSet.SetExpression.Operation.UNION
                          lhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              reportingSet = PRIMITIVE_REPORTING_SET_NAME_2
                            }
                        }
                    }
                }
            }
        },
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        reportingSet {
          campaignGroup = CAMPAIGN_GROUP_NAME
          composite =
            ReportingSetKt.composite {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.UNION
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = PRIMITIVE_REPORTING_SET_NAME_3
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      expression =
                        ReportingSetKt.setExpression {
                          operation = ReportingSet.SetExpression.Operation.UNION
                          lhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              reportingSet = PRIMITIVE_REPORTING_SET_NAME_2
                            }
                          rhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              expression =
                                ReportingSetKt.setExpression {
                                  operation = ReportingSet.SetExpression.Operation.UNION
                                  lhs =
                                    ReportingSetKt.SetExpressionKt.operand {
                                      reportingSet = PRIMITIVE_REPORTING_SET_NAME_1
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `reportingUnitMetricSpec with just cumulative and 1 reportingUnit component no composite`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { averageFrequency = true }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "person.age_group == 1 && person.gender == 1"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
              metricSpecs += metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `stackedIncrementalReach with just 1 reportingUnit component has no composite`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec { stackedIncrementalReach = true }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "person.age_group == 1 && person.gender == 1"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `stackedIncrementalReach with 3 reportingUnit components transforms into correct map`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
      put(DATA_PROVIDER_NAME_3, PRIMITIVE_REPORTING_SET_3)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit {
            components += DATA_PROVIDER_NAME_1
            components += DATA_PROVIDER_NAME_2
            components += DATA_PROVIDER_NAME_3
          }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec { stackedIncrementalReach = true }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(3)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "person.age_group == 1 && person.gender == 1"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        reportingSet {
          campaignGroup = CAMPAIGN_GROUP_NAME
          composite =
            ReportingSetKt.composite {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.UNION
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = PRIMITIVE_REPORTING_SET_NAME_2
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      expression =
                        ReportingSetKt.setExpression {
                          operation = ReportingSet.SetExpression.Operation.UNION
                          lhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              reportingSet = PRIMITIVE_REPORTING_SET_NAME_1
                            }
                        }
                    }
                }
            }
        },
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        reportingSet {
          campaignGroup = CAMPAIGN_GROUP_NAME
          composite =
            ReportingSetKt.composite {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.UNION
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      reportingSet = PRIMITIVE_REPORTING_SET_NAME_3
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      expression =
                        ReportingSetKt.setExpression {
                          operation = ReportingSet.SetExpression.Operation.UNION
                          lhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              reportingSet = PRIMITIVE_REPORTING_SET_NAME_2
                            }
                          rhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              expression =
                                ReportingSetKt.setExpression {
                                  operation = ReportingSet.SetExpression.Operation.UNION
                                  lhs =
                                    ReportingSetKt.SetExpressionKt.operand {
                                      reportingSet = PRIMITIVE_REPORTING_SET_NAME_1
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `different values in dimensionSpec filter can be processed and sorted`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "banner_ad.viewable"
                value = EventTemplateFieldKt.fieldValue { boolValue = true }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "video_ad.viewed_fraction"
                value = EventTemplateFieldKt.fieldValue { floatValue = 0.5f }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec { stackedIncrementalReach = true }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap.keys)
      .containsExactly(PRIMITIVE_REPORTING_SET_1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap[PRIMITIVE_REPORTING_SET_1])
      .containsExactly(
        MetricCalculationSpecKt.details {
          filter =
            "banner_ad.viewable == true && person.age_group == 1 && video_ad.viewed_fraction == 0.5"
          metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
        },
        MetricCalculationSpecKt.details {
          filter =
            "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (banner_ad.viewable == true && person.age_group == 1 && video_ad.viewed_fraction == 0.5)"
          metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
        },
      )
  }

  @Test
  fun `dimensionSpec filter missing value throws IllegalArgumentException`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter { terms += eventTemplateField { path = "person.age_group" } }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec { stackedIncrementalReach = true }
          }
        }
      )

    assertFailsWith<IllegalArgumentException> {
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )
    }
  }

  @Test
  fun `duplicate resultGroupSpecs does not duplicate the entries in the map`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit {
            components += DATA_PROVIDER_NAME_1
            components += DATA_PROVIDER_NAME_2
          }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.WEDNESDAY }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                nonCumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    averageFrequency = true
                    impressions = true
                  }
                cumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    averageFrequency = true
                    impressions = true
                  }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    val resultGroupSpecsWithDuplicates = resultGroupSpecs + resultGroupSpecs
    val secondReportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecsWithDuplicates,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .isEqualTo(secondReportingSetMetricCalculationSpecDetailsMap)
  }

  @Test
  fun `reach transforms into reach MetricSpec`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "person.age_group == 1 && person.gender == 1"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `percent_reach transforms into reach MetricSpec and population MetricSpec`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { percentReach = true }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "person.age_group == 1 && person.gender == 1"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `k_plus_reach transforms into reachAndFrequency MetricSpec`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { kPlusReach = 3 }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "person.age_group == 1 && person.gender == 1"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec {
                reachAndFrequency = MetricSpecKt.reachAndFrequencyParams {}
              }
            }
          )
        },
      )
  }

  @Test
  fun `percent_k_plus_reach transforms into rf MetricSpec and population MetricSpec`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { percentKPlusReach = true }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "person.age_group == 1 && person.gender == 1"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec {
                reachAndFrequency = MetricSpecKt.reachAndFrequencyParams {}
              }
            }
          )
        },
      )
  }

  @Test
  fun `averageFrequency transforms into reach MetricSpec and impression MetricSpec`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { averageFrequency = true }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "person.age_group == 1 && person.gender == 1"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
              metricSpecs += metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `impressions transforms into impression MetricSpec`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { impressions = true }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "person.age_group == 1 && person.gender == 1"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `grps transforms into impression MetricSpec and population MetricSpec`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { grps = true }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "person.age_group == 1 && person.gender == 1"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `dimensionSpec Grouping with single field results in a single grouping`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.gender" }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { grps = true }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = emptyList(),
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              groupings +=
                MetricCalculationSpecKt.grouping {
                  predicates += "person.gender == 1"
                  predicates += "person.gender == 2"
                }
              filter = "person.age_group == 1"
              metricSpecs += metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `dimensionSpec Grouping with 2 fields results in 2 groupings`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
      put(DATA_PROVIDER_NAME_2, PRIMITIVE_REPORTING_SET_2)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            grouping =
              DimensionSpecKt.grouping {
                eventTemplateFields += "person.gender"
                eventTemplateFields += "person.age_group"
              }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { grps = true }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = emptyList(),
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              groupings +=
                MetricCalculationSpecKt.grouping {
                  predicates += "person.gender == 1"
                  predicates += "person.gender == 2"
                }
              groupings +=
                MetricCalculationSpecKt.grouping {
                  predicates += "person.age_group == 1"
                  predicates += "person.age_group == 2"
                  predicates += "person.age_group == 3"
                }
              filter = "person.age_group == 1"
              metricSpecs += metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `resultGroupSpec with no IQFs transforms into correct map`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = emptyList(),
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "person.age_group == 1 && person.gender == 1"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `resultGroupSpec with 2 IQFs transforms into correct map`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
          }
        }
      )

    val impressionQualificationFilterSpecList = buildList {
      add(
        listOf(
          impressionQualificationFilterSpec {
            mediaType = MediaType.VIDEO
            filters += eventFilter {
              terms += eventTemplateField {
                path = "video_ad.viewed_fraction"
                value = EventTemplateFieldKt.fieldValue { floatValue = 1.0f }
              }
            }
          }
        )
      )
      add(
        listOf(
          impressionQualificationFilterSpec {
            mediaType = MediaType.DISPLAY
            filters += eventFilter {
              terms += eventTemplateField {
                path = "banner_ad.viewable"
                value = EventTemplateFieldKt.fieldValue { boolValue = true }
              }
            }
          }
        )
      )
    }

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs =
          impressionQualificationFilterSpecList.map {
            SourcedImpressionQualificationFilterSpecs(
              it,
              ImpressionQualificationFilterSpecsSource.Base("test"),
            )
          },
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "person.age_group == 1 && person.gender == 1"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )
          add(
            MetricCalculationSpecKt.details {
              filter =
                "(video_ad != null && video_ad.viewed_fraction == 1.0) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
          add(
            MetricCalculationSpecKt.details {
              filter =
                "(banner_ad != null && banner_ad.viewable == true) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `resultGroupSpec with 2 specs for IQF transforms into correct map`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
          }
        }
      )

    val impressionQualificationFilterSpecList = buildList {
      add(
        listOf(
          impressionQualificationFilterSpec {
            mediaType = MediaType.DISPLAY
            filters += eventFilter {
              terms += eventTemplateField {
                path = "banner_ad.viewable"
                value = EventTemplateFieldKt.fieldValue { boolValue = true }
              }
            }
          },
          impressionQualificationFilterSpec {
            mediaType = MediaType.VIDEO
            filters += eventFilter {
              terms += eventTemplateField {
                path = "video_ad.viewed_fraction"
                value = EventTemplateFieldKt.fieldValue { floatValue = 0.5f }
              }
            }
          },
        )
      )
    }

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs =
          impressionQualificationFilterSpecList.map {
            SourcedImpressionQualificationFilterSpecs(
              it,
              ImpressionQualificationFilterSpecsSource.Base("test"),
            )
          },
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "person.age_group == 1 && person.gender == 1"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 0.5)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `resultGroupSpec with IQF and no dimensionSpec transforms into correct map`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "(banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `resultGroupSpec with no IQF and no dimensionSpec transforms into correct map`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
          }
        }
      )

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs = emptyList(),
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `resultGroupSpec with IQF with only MediaType OTHER transforms into correct map`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
          }
        }
      )

    val impressionQualificationFilterSpecList: List<List<ImpressionQualificationFilterSpec>> =
      buildList {
        add(listOf(impressionQualificationFilterSpec { mediaType = MediaType.OTHER }))
      }

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs =
          impressionQualificationFilterSpecList.map {
            SourcedImpressionQualificationFilterSpecs(
              it,
              ImpressionQualificationFilterSpecsSource.Base("test"),
            )
          },
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap.keys)
      .containsExactly(PRIMITIVE_REPORTING_SET_1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap[PRIMITIVE_REPORTING_SET_1])
      .containsExactly(
        MetricCalculationSpecKt.details {
          filter = "person.age_group == 1 && person.gender == 1"
          metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
        },
        MetricCalculationSpecKt.details {
          filter = "(testing_only != null) && (person.age_group == 1 && person.gender == 1)"
          metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
        },
      )
  }

  @Test
  fun `resultGroupSpec with IQF MediaType OtHER plus another transforms into correct map`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
          }
        }
      )

    val impressionQualificationFilterSpecList: List<List<ImpressionQualificationFilterSpec>> =
      buildList {
        add(
          listOf(
            impressionQualificationFilterSpec { mediaType = MediaType.VIDEO },
            impressionQualificationFilterSpec { mediaType = MediaType.OTHER },
          )
        )
      }

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs =
          impressionQualificationFilterSpecList.map {
            SourcedImpressionQualificationFilterSpecs(
              it,
              ImpressionQualificationFilterSpecsSource.Base("test"),
            )
          },
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap.keys)
      .containsExactly(PRIMITIVE_REPORTING_SET_1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap[PRIMITIVE_REPORTING_SET_1])
      .containsExactly(
        MetricCalculationSpecKt.details {
          filter = "person.age_group == 1 && person.gender == 1"
          metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
        },
        MetricCalculationSpecKt.details {
          filter =
            "((testing_only != null) || (video_ad != null)) && (person.age_group == 1 && person.gender == 1)"
          metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
        },
      )
  }

  @Test
  fun `resultGroupSpec with IQF with no filters transforms into correct map`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
          }
        }
      )

    val impressionQualificationFilterSpecList: List<List<ImpressionQualificationFilterSpec>> =
      buildList {
        add(
          listOf(
            impressionQualificationFilterSpec { mediaType = MediaType.VIDEO },
            impressionQualificationFilterSpec { mediaType = MediaType.DISPLAY },
            impressionQualificationFilterSpec { mediaType = MediaType.OTHER },
          )
        )
      }

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs =
          impressionQualificationFilterSpecList.map {
            SourcedImpressionQualificationFilterSpecs(
              it,
              ImpressionQualificationFilterSpecsSource.Base("test"),
            )
          },
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap.keys)
      .containsExactly(PRIMITIVE_REPORTING_SET_1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap[PRIMITIVE_REPORTING_SET_1])
      .containsExactly(
        MetricCalculationSpecKt.details {
          filter = "person.age_group == 1 && person.gender == 1"
          metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
        },
        MetricCalculationSpecKt.details {
          filter =
            "((banner_ad != null) || (testing_only != null) || (video_ad != null)) && (person.age_group == 1 && person.gender == 1)"
          metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
        },
      )
  }

  @Test
  fun `resultGroupSpec with IQF with no specs transforms into correct map`() {
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
          }
        }
      )

    val impressionQualificationFilterSpecList: List<List<ImpressionQualificationFilterSpec>> =
      buildList {
        add(emptyList())
      }

    val reportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecs =
          impressionQualificationFilterSpecList.map {
            SourcedImpressionQualificationFilterSpecs(
              it,
              ImpressionQualificationFilterSpecsSource.Base("test"),
            )
          },
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        env = TEST_CEL_ENV,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap.keys)
      .containsExactly(PRIMITIVE_REPORTING_SET_1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap[PRIMITIVE_REPORTING_SET_1])
      .containsExactly(
        MetricCalculationSpecKt.details {
          filter = "person.age_group == 1 && person.gender == 1"
          metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
          metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
        }
      )
  }

  @Test
  fun `Custom IQF with bad CEL throws InvalidFieldValueException pointing at request index`() {
    for (case in BAD_CEL_CASES) {
      val exception =
        assertFailsWith<InvalidFieldValueException>("case: ${case.label} filter='${case.filter}'") {
          validateImpressionQualificationFilterCel(
            env = TEST_CEL_ENV,
            filter = case.filter,
            source = ImpressionQualificationFilterSpecsSource.Custom(requestIndex = 2),
          )
        }
      assertThat(exception.message)
        .contains("basic_report.impression_qualification_filters[2].custom")
      assertThat(exception.message).contains(case.fieldSuffix)
    }
  }

  @Test
  fun `Base IQF with bad CEL throws IllegalStateException naming the IQF id`() {
    for (case in BAD_CEL_CASES) {
      val exception =
        assertFailsWith<IllegalStateException>(
          "case: ${case.label} filter='${case.filter}'"
        ) {
          validateImpressionQualificationFilterCel(
            env = TEST_CEL_ENV,
            filter = case.filter,
            source = ImpressionQualificationFilterSpecsSource.Base("misconfigured-iqf"),
          )
        }
      assertThat(exception.message).contains("misconfigured-iqf")
      assertThat(exception.message).contains(case.diagnostic)
    }
  }

  @Test
  fun `Named IQF with bad CEL throws IllegalStateException naming the IQF resource and request index`() {
    for (case in BAD_CEL_CASES) {
      val exception =
        assertFailsWith<IllegalStateException>(
          "case: ${case.label} filter='${case.filter}'"
        ) {
          validateImpressionQualificationFilterCel(
            env = TEST_CEL_ENV,
            filter = case.filter,
            source =
              ImpressionQualificationFilterSpecsSource.Named(
                requestIndex = 1,
                impressionQualificationFilterName = "impressionQualificationFilters/iqf-1",
              ),
          )
        }
      assertThat(exception.message).contains("impressionQualificationFilters/iqf-1")
      assertThat(exception.message)
        .contains(
          "basic_report.impression_qualification_filters[1].impression_qualification_filter"
        )
      assertThat(exception.message).contains(case.diagnostic)
    }
  }

  @Test
  fun `validateImpressionQualificationFilterCel accepts an empty filter for any source`() {
    // Smoke test: empty filter is the dedup-collapsed case and must not throw regardless of
    // provenance.
    validateImpressionQualificationFilterCel(
      env = TEST_CEL_ENV,
      filter = "",
      source = ImpressionQualificationFilterSpecsSource.Custom(requestIndex = 0),
    )
    validateImpressionQualificationFilterCel(
      env = TEST_CEL_ENV,
      filter = "",
      source = ImpressionQualificationFilterSpecsSource.Base("anything"),
    )
    validateImpressionQualificationFilterCel(
      env = TEST_CEL_ENV,
      filter = "",
      source =
        ImpressionQualificationFilterSpecsSource.Named(
          requestIndex = 0,
          impressionQualificationFilterName = "impressionQualificationFilters/x",
        ),
    )
  }

  @Test
  fun `validateImpressionQualificationFilterCel accepts a well-formed boolean for any source`() {
    val valid = "banner_ad.viewable == true"
    validateImpressionQualificationFilterCel(
      env = TEST_CEL_ENV,
      filter = valid,
      source = ImpressionQualificationFilterSpecsSource.Custom(requestIndex = 0),
    )
    validateImpressionQualificationFilterCel(
      env = TEST_CEL_ENV,
      filter = valid,
      source = ImpressionQualificationFilterSpecsSource.Base("anything"),
    )
    validateImpressionQualificationFilterCel(
      env = TEST_CEL_ENV,
      filter = valid,
      source =
        ImpressionQualificationFilterSpecsSource.Named(
          requestIndex = 0,
          impressionQualificationFilterName = "impressionQualificationFilters/x",
        ),
    )
  }

  companion object {
    /**
     * One-liner CEL filter strings that must be rejected by
     * [validateImpressionQualificationFilterCel], each carrying the substring expected in the
     * resulting exception message so we know we have not silently regressed the phrasing.
     */
    private data class BadCelCase(
      val label: String,
      val filter: String,
      val diagnostic: String,
      val fieldSuffix: String,
    )

    private val BAD_CEL_CASES =
      listOf(
        BadCelCase(
          label = "trailing operator (parse error)",
          filter = "banner_ad.viewable ==",
          diagnostic = "not a valid CEL expression",
          fieldSuffix = "is not a valid CEL expression",
        ),
        BadCelCase(
          label = "unknown top-level identifier",
          filter = "nonexistent_field == true",
          diagnostic = "not a valid CEL expression",
          fieldSuffix = "is not a valid CEL expression",
        ),
        BadCelCase(
          label = "unknown nested field",
          filter = "banner_ad.bogus_field == true",
          diagnostic = "not a valid CEL expression",
          fieldSuffix = "is not a valid CEL expression",
        ),
        BadCelCase(
          label = "type-mismatched operand (bool + int)",
          filter = "banner_ad.viewable + 1 == 2",
          diagnostic = "not a valid CEL expression",
          fieldSuffix = "is not a valid CEL expression",
        ),
        BadCelCase(
          label = "non-boolean result (float field on its own)",
          filter = "video_ad.viewed_fraction",
          diagnostic = "does not evaluate to a boolean",
          fieldSuffix = "does not evaluate to a boolean",
        ),
        BadCelCase(
          label = "non-boolean result (string literal)",
          filter = "'just a string'",
          diagnostic = "does not evaluate to a boolean",
          fieldSuffix = "does not evaluate to a boolean",
        ),
        BadCelCase(
          label = "non-boolean result (int literal)",
          filter = "42",
          diagnostic = "does not evaluate to a boolean",
          fieldSuffix = "does not evaluate to a boolean",
        ),
        BadCelCase(
          label = "non-boolean result (message field access)",
          filter = "banner_ad",
          diagnostic = "does not evaluate to a boolean",
          fieldSuffix = "does not evaluate to a boolean",
        ),
      )

    private const val MEASUREMENT_CONSUMER_ID = "AAAAAAAAAHs"
    private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/$MEASUREMENT_CONSUMER_ID"
    private const val CAMPAIGN_GROUP_NAME =
      "$MEASUREMENT_CONSUMER_NAME/reportingSets/campaign-group-1"
    private const val DATA_PROVIDER_NAME_1 = "$MEASUREMENT_CONSUMER_NAME/dataProviders/AAAAAAAAAHs"
    private const val DATA_PROVIDER_NAME_2 = "$MEASUREMENT_CONSUMER_NAME/dataProviders/BBBBBBBBBHs"
    private const val DATA_PROVIDER_NAME_3 = "$MEASUREMENT_CONSUMER_NAME/dataProviders/CCCCCCCCCHs"

    private const val PRIMITIVE_REPORTING_SET_NAME_1 =
      "$MEASUREMENT_CONSUMER_NAME/reportingSets/primitive-reporting-set-1"
    private const val PRIMITIVE_REPORTING_SET_NAME_2 =
      "$MEASUREMENT_CONSUMER_NAME/reportingSets/primitive-reporting-set-2"
    private const val PRIMITIVE_REPORTING_SET_NAME_3 =
      "$MEASUREMENT_CONSUMER_NAME/reportingSets/primitive-reporting-set-3"
    private val PRIMITIVE_REPORTING_SET_1 = reportingSet { name = PRIMITIVE_REPORTING_SET_NAME_1 }
    private val PRIMITIVE_REPORTING_SET_2 = reportingSet { name = PRIMITIVE_REPORTING_SET_NAME_2 }
    private val PRIMITIVE_REPORTING_SET_3 = reportingSet { name = PRIMITIVE_REPORTING_SET_NAME_3 }

    private val SOURCED_IMPRESSION_QUALIFICATION_FILTER_SPECS:
      List<SourcedImpressionQualificationFilterSpecs> =
      listOf(
        SourcedImpressionQualificationFilterSpecs(
          specs =
            listOf(
              impressionQualificationFilterSpec {
                mediaType = MediaType.VIDEO
                filters += eventFilter {
                  terms += eventTemplateField {
                    path = "video_ad.viewed_fraction"
                    value = EventTemplateFieldKt.fieldValue { floatValue = 1.0f }
                  }
                }
              },
              impressionQualificationFilterSpec {
                mediaType = MediaType.DISPLAY
                filters += eventFilter {
                  terms += eventTemplateField {
                    path = "banner_ad.viewable"
                    value = EventTemplateFieldKt.fieldValue { boolValue = true }
                  }
                }
              },
            ),
          source = ImpressionQualificationFilterSpecsSource.Base("test"),
        )
      )

    private val TEST_EVENT_DESCRIPTOR = EventMessageDescriptor(TestEvent.getDescriptor())
    private val TEST_CEL_ENV = CelPredicates.buildEnvironment(TestEvent.getDescriptor())
  }

  //
  // These probes bypass CreateBasicReportRequestValidation entirely to see what
  // toCelValue emits for extreme user values. If the CEL block would need to
  // catch these when upstream validation is loosened / a new selectorCase is
  // added, that's evidence the block is defense-in-depth against real bugs.

  // ============================================================================
  // REACHABILITY-BOUNDARY TESTS
  //
  // What these prove:
  //   - Every filter shape a caller can construct today (given the upstream
  //     validators) produces well-formed CEL. The CEL block is a NO-OP for
  //     currently-reachable user input.
  //   - IF a future change opens up a new selectorCase, weakens
  //     validateEventTemplateFieldValue, or adds a STRING/FLOAT-typed
  //     IMPRESSION_QUALIFICATION field, the CEL block would catch the
  //     resulting malformed CEL. These tests hand-craft the CEL that would
  //     result and confirm the validator rejects it.
  //
  // Bottom line: the CEL block is defense-in-depth against future
  // regressions in upstream validators and against toCelValue's raw
  // STRING_VALUE / FLOAT_VALUE emissions (which would produce bad CEL if
  // ever reached).
  // ============================================================================

  @Test
  fun `every user-reachable enum value produces well-formed CEL`() {
    // All valid enum names for person.gender. Each generates `person.gender == <ordinal>`
    // which CEL parses as bool.
    for (enumName in listOf("GENDER_UNSPECIFIED", "MALE", "FEMALE")) {
      val filter =
        buildCelExpression(
          dimensionSpecFilters =
            listOf(
              eventFilter {
                terms += eventTemplateField {
                  path = "person.gender"
                  value = EventTemplateFieldKt.fieldValue { enumValue = enumName }
                }
              }
            ),
          eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        )
      // Should not throw for any valid enum name.
      CelPredicates.validate(TEST_CEL_ENV, filter)
    }
  }

  @Test
  fun `every user-reachable bool IQF produces well-formed CEL`() {
    for (v in listOf(true, false)) {
      val filter =
        buildCelExpression(
          impressionQualificationFilterSpecs =
            listOf(
              impressionQualificationFilterSpec {
                mediaType = MediaType.DISPLAY
                filters += eventFilter {
                  terms += eventTemplateField {
                    path = "banner_ad.viewable"
                    value = EventTemplateFieldKt.fieldValue { boolValue = v }
                  }
                }
              }
            ),
          eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        )
      CelPredicates.validate(TEST_CEL_ENV, filter)
    }
  }

  @Test
  fun `dimension_spec with multiple filters joins with && and validates`() {
    val filter =
      buildCelExpression(
        dimensionSpecFilters =
          listOf(
            eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            },
            eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            },
          ),
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )
    // Locks in the && join shape. If a future refactor changes to || (which
    // has different bool semantics but would still validate), the shape here
    // will surface it.
    assertThat(filter).contains(" && ")
    CelPredicates.validate(TEST_CEL_ENV, filter)
  }

  @Test
  fun `custom IQF with two mediaTypes joins with disjunction and validates`() {
    val filter =
      buildCelExpression(
        impressionQualificationFilterSpecs =
          listOf(
            impressionQualificationFilterSpec {
              mediaType = MediaType.DISPLAY
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "banner_ad.viewable"
                  value = EventTemplateFieldKt.fieldValue { boolValue = false }
                }
              }
            },
            impressionQualificationFilterSpec {
              mediaType = MediaType.VIDEO
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "video_ad.viewed_fraction"
                  value = EventTemplateFieldKt.fieldValue { floatValue = 0.5f }
                }
              }
            },
          ),
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )
    // Locks in the || join shape across mediaTypes.
    assertThat(filter).contains(" || ")
    CelPredicates.validate(TEST_CEL_ENV, filter)
  }

  // ---- Backstop tests: shapes that would result from a future regression or a
  // STRING/FLOAT-typed IMPRESSION_QUALIFICATION field being added. CEL block
  // catches each.

  @Test
  fun `backstop - rejects field == NaN which would result from a Float NaN on a FLOAT IQF field`() {
    // If validateEventTemplateFieldValue is ever weakened to accept FLOAT_VALUE from
    // users, and a caller sends floatValue = Float.NaN on a FLOAT IQF field,
    // toCelValue would emit "NaN" (unquoted) and CEL would see this exact string.
    // No FLOAT IMPRESSION_QUALIFICATION field exists in the test event, so we
    // hand-craft the CEL string to prove the backstop.
    val exception =
      assertFailsWith<CelValidationException> {
        CelPredicates.validate(TEST_CEL_ENV, "video_ad.viewed_fraction == NaN")
      }
    assertThat(exception.message).contains("not a valid CEL expression")
    assertThat(exception.message).contains("NaN")
  }

  @Test
  fun `backstop - rejects field == Infinity which would result from a Float Infinity on a FLOAT IQF field`() {
    val exception =
      assertFailsWith<CelValidationException> {
        CelPredicates.validate(TEST_CEL_ENV, "video_ad.viewed_fraction == Infinity")
      }
    assertThat(exception.message).contains("not a valid CEL expression")
    assertThat(exception.message).contains("Infinity")
  }

  @Test
  fun `backstop - rejects field == unquoted_string which would result from raw stringValue on a STRING IQF field`() {
    // toCelValue's STRING_VALUE branch is `stringValue` (unquoted). If a STRING
    // IMPRESSION_QUALIFICATION field is ever added and a user (or base IQF)
    // supplies stringValue = "abc" on it, the generated CEL is
    // `<path> == abc` which CEL parses as identifier comparison. No STRING IQF
    // field exists today; hand-craft the CEL to prove the backstop.
    val exception =
      assertFailsWith<CelValidationException> {
        CelPredicates.validate(TEST_CEL_ENV, "banner_ad.viewable == abc")
      }
    assertThat(exception.message).contains("not a valid CEL expression")
    assertThat(exception.message).contains("abc")
  }
}
