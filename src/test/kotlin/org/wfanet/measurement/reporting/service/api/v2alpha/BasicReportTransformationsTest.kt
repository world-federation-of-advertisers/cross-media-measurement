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
import com.google.type.DayOfWeek
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.EventMessageDescriptor
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.reporting.v2alpha.DimensionSpecKt
import org.wfanet.measurement.reporting.v2alpha.EventTemplateFieldKt
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(2)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        listOf(
          MetricCalculationSpecKt.details {
            filter = "(person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(2)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "(person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(3)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "(person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(3)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "(person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(2)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "(person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(3)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "(person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        listOf(
          MetricCalculationSpecKt.details {
            filter = "(person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(5)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        listOf(
          MetricCalculationSpecKt.details {
            filter = "(person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(5)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        listOf(
          MetricCalculationSpecKt.details {
            filter = "(person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "(person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "(person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(3)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "(person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
        },
      )
  }

  @Test
  fun `differnt values in dimensionSpec filter can be processed and sorted`() {
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter =
                "(banner_ad.viewable == true && person.age_group == 1 && video_ad.viewed_fraction == 0.5)"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (banner_ad.viewable == true && person.age_group == 1 && video_ad.viewed_fraction == 0.5)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    val resultGroupSpecsWithDuplicates = resultGroupSpecs + resultGroupSpecs
    val secondReportingSetMetricCalculationSpecDetailsMap =
      buildReportingSetMetricCalculationSpecDetailsMap(
        campaignGroupName = CAMPAIGN_GROUP_NAME,
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecsWithDuplicates,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "(person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "(person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "(person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "(person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "(person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "(person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "(person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = emptyList(),
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
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
              filter = "(person.age_group == 1)"
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
        impressionQualificationFilterSpecsLists = emptyList(),
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
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
              filter = "(person.age_group == 1)"
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
        impressionQualificationFilterSpecsLists = emptyList(),
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "(person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = impressionQualificationFilterSpecList,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "(person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }
          )
          add(
            MetricCalculationSpecKt.details {
              filter =
                "((has(banner_ad.viewable) && banner_ad.viewable == true)) && (person.age_group == 1 && person.gender == 1)"
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
            filters += eventFilter {
              terms += eventTemplateField {
                path = "banner_ad.viewable"
                value = EventTemplateFieldKt.fieldValue { boolValue = true }
              }
            }
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
        impressionQualificationFilterSpecsLists = impressionQualificationFilterSpecList,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    assertThat(reportingSetMetricCalculationSpecDetailsMap).hasSize(1)
    assertThat(reportingSetMetricCalculationSpecDetailsMap)
      .containsEntry(
        PRIMITIVE_REPORTING_SET_1,
        buildList {
          add(
            MetricCalculationSpecKt.details {
              filter = "(person.age_group == 1 && person.gender == 1)"
              metricSpecs += metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
            }
          )

          add(
            MetricCalculationSpecKt.details {
              filter =
                "((has(banner_ad.viewable) && banner_ad.viewable == true && has(banner_ad.viewable) && banner_ad.viewable == true)) && (person.age_group == 1 && person.gender == 1)"
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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
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
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0))"
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
        impressionQualificationFilterSpecsLists = emptyList(),
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
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

  companion object {
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

    private val IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS = buildList {
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
        )
      )
    }

    private val TEST_EVENT_DESCRIPTOR = EventMessageDescriptor(TestEvent.getDescriptor())
  }
}
