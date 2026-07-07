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
import kotlin.test.fail
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.projectnessie.cel.Env
import org.projectnessie.cel.EnvOption
import org.projectnessie.cel.checker.Decls
import org.projectnessie.cel.common.types.pb.ProtoTypeRegistry
import org.wfanet.measurement.api.v2alpha.EventMessageDescriptor
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestingOnly
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.metricSpec
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
  private fun MetricCalculationSpec.Details.hasReachMetric(): Boolean =
    metricSpecsList.any { it.hasReach() }

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
        impressionQualificationFilterSpecsLists = IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
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
  fun `match-all IQF combined with non-empty IQF still produces a spec without dimension filter`() {
    // Regression test for issue #4109: a match-all ImpressionQualificationFilter (empty filter
    // specs, e.g. AMI) was silently dropped when combined with a non-empty IQF, so no
    // MetricCalculationSpec was created for it and its results were never computed.
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {}
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
        // A match-all IQF (empty filter specs) plus the standard non-empty IQF.
        impressionQualificationFilterSpecsLists =
          listOf(emptyList<ImpressionQualificationFilterSpec>()) +
            IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    val nonEmptyIqfFilter =
      "(banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)"
    val allDetails = reportingSetMetricCalculationSpecDetailsMap.values.flatten()

    // The match-all IQF must still produce a reach metric under the empty (match-all) filter.
    assertThat(allDetails.any { it.filter.isEmpty() && it.hasReachMetric() }).isTrue()
    // The non-empty IQF reach metric is present and not combined with any dimension filter.
    assertThat(allDetails.any { it.filter == nonEmptyIqfFilter && it.hasReachMetric() }).isTrue()
  }

  @Test
  fun `match-all IQF combined with dimension filter yields dimension filter alone`() {
    // Regression test for issue #4109: with a DimensionSpec filter present, a match-all IQF must
    // resolve to the DimensionSpec filter alone, not a malformed "() && (...)" expression.
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
        impressionQualificationFilterSpecsLists =
          listOf(emptyList<ImpressionQualificationFilterSpec>()) +
            IMPRESSION_QUALIFICATION_FILTER_SPECS_LISTS,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    val dimensionFilter = "person.age_group == 1 && person.gender == 1"
    val combinedFilter =
      "((banner_ad != null && banner_ad.viewable == true) || (video_ad != null && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1 && person.gender == 1)"
    val allDetails = reportingSetMetricCalculationSpecDetailsMap.values.flatten()

    // The match-all IQF reach metric uses the DimensionSpec filter alone (not "() && (...)").
    assertThat(allDetails.any { it.filter == dimensionFilter && it.hasReachMetric() }).isTrue()
    // No malformed expression is produced.
    assertThat(allDetails.none { it.filter.contains("()") }).isTrue()
    // The non-empty IQF reach metric is still correctly combined with the dimension filter.
    assertThat(allDetails.any { it.filter == combinedFilter && it.hasReachMetric() }).isTrue()
  }

  @Test
  fun `single match-all IQF with dimension filter resolves to the dimension filter`() {
    // Regression test for issue #4109: a single match-all IQF combined with a DimensionSpec filter
    // must continue to resolve to the DimensionSpec filter alone. After the fix this flows through
    // the empty-ImpressionQualificationFilter branch of buildCelExpressions rather than the
    // pre-fix empty-list path, so pin the single-match-all contract explicitly.
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
        impressionQualificationFilterSpecsLists =
          listOf(emptyList<ImpressionQualificationFilterSpec>()),
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )

    val dimensionFilter = "person.age_group == 1 && person.gender == 1"
    val allDetails = reportingSetMetricCalculationSpecDetailsMap.values.flatten()

    // The reach metric uses the DimensionSpec filter alone (not "() && (...)").
    assertThat(allDetails.any { it.filter == dimensionFilter && it.hasReachMetric() }).isTrue()
    // No malformed expression is produced.
    assertThat(allDetails.none { it.filter.contains("()") }).isTrue()
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
        impressionQualificationFilterSpecsLists = impressionQualificationFilterSpecList,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
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
        impressionQualificationFilterSpecsLists = impressionQualificationFilterSpecList,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
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
        impressionQualificationFilterSpecsLists = impressionQualificationFilterSpecList,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
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
        impressionQualificationFilterSpecsLists = impressionQualificationFilterSpecList,
        dataProviderPrimitiveReportingSetMap = dataProviderPrimitiveReportingSetMap,
        resultGroupSpecs = resultGroupSpecs,
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
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

  // ---- toCelValue quoting / rejection tests (issue #4148) ----

  // STRING_VALUE branch: assert the transformer emits a properly-quoted CEL string
  // literal for a base IQF whose stringValue lands on a STRING IMPRESSION_QUALIFICATION
  // field. Uses TestingOnly.testing_string (see event_templates/testing/testing_only.proto).

  @Test
  fun `toCelValue emits properly quoted STRING literal end to end`() {
    val filter =
      buildCelExpression(
        impressionQualificationFilterSpecs =
          listOf(
            impressionQualificationFilterSpec {
              mediaType = MediaType.OTHER
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "testing_only.testing_string"
                  value = EventTemplateFieldKt.fieldValue { stringValue = "abc" }
                }
              }
            }
          ),
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )
    assertThat(filter).contains("testing_only.testing_string == \"abc\"")
    assertCompilesCleanly(filter)
  }

  @Test
  fun `toCelValue escapes embedded quote and backslash in STRING literal`() {
    val filter =
      buildCelExpression(
        impressionQualificationFilterSpecs =
          listOf(
            impressionQualificationFilterSpec {
              mediaType = MediaType.OTHER
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "testing_only.testing_string"
                  value = EventTemplateFieldKt.fieldValue { stringValue = "he said \"hi\\bye\"" }
                }
              }
            }
          ),
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )
    assertThat(filter).contains("testing_only.testing_string == \"he said \\\"hi\\\\bye\\\"\"")
    assertCompilesCleanly(filter)
  }

  @Test
  fun `toCelValue escapes control characters in STRING literal`() {
    val filter =
      buildCelExpression(
        impressionQualificationFilterSpecs =
          listOf(
            impressionQualificationFilterSpec {
              mediaType = MediaType.OTHER
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "testing_only.testing_string"
                  value =
                    EventTemplateFieldKt.fieldValue {
                      stringValue = "line1\nline2\ttab\rreturnbell"
                    }
                }
              }
            }
          ),
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )
    assertThat(filter)
      .contains("testing_only.testing_string == \"line1\\nline2\\ttab\\rreturn\\u0001bell\"")
    assertCompilesCleanly(filter)
  }

  @Test
  fun `toCelValue escapes DEL (0x7F) in STRING literal`() {
    // DEL has its own branch in toCelStringLiteral (mapped to \u007f) distinct from the
    // generic control-char < 0x20 branch. Pin it directly so a future refactor can't
    // silently collapse the branches and end up emitting DEL as a raw byte.
    val filter =
      buildCelExpression(
        impressionQualificationFilterSpecs =
          listOf(
            impressionQualificationFilterSpec {
              mediaType = MediaType.OTHER
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "testing_only.testing_string"
                  value = EventTemplateFieldKt.fieldValue { stringValue = "before\u007Fafter" }
                }
              }
            }
          ),
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )
    assertThat(filter).contains("testing_only.testing_string == \"before\\u007fafter\"")
    assertCompilesCleanly(filter)
  }

  @Test
  fun `toCelValue passes through printable non-ASCII in STRING literal`() {
    val filter =
      buildCelExpression(
        impressionQualificationFilterSpecs =
          listOf(
            impressionQualificationFilterSpec {
              mediaType = MediaType.OTHER
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "testing_only.testing_string"
                  value = EventTemplateFieldKt.fieldValue { stringValue = "café" }
                }
              }
            }
          ),
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )
    assertThat(filter).contains("testing_only.testing_string == \"café\"")
    assertCompilesCleanly(filter)
  }

  @Test
  fun `toCelValue handles empty STRING as empty quoted literal`() {
    val filter =
      buildCelExpression(
        impressionQualificationFilterSpecs =
          listOf(
            impressionQualificationFilterSpec {
              mediaType = MediaType.OTHER
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "testing_only.testing_string"
                  value = EventTemplateFieldKt.fieldValue { stringValue = "" }
                }
              }
            }
          ),
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )
    assertThat(filter).contains("testing_only.testing_string == \"\"")
    assertCompilesCleanly(filter)
  }

  @Test
  fun `toCelValue emits properly quoted STRING literal via DimensionSpec filter end to end`() {
    // Coverage that the STRING_VALUE fix works via the DimensionSpec (dimension_spec.filters)
    // path as well as the IQF path. Same underlying toCelValue branch; different upstream caller
    // in BasicReportTransformations.buildCelExpression. Uses testing_only.testing_string_filterable
    // which is FILTERABLE + POPULATION_ATTRIBUTE (rather than IMPRESSION_QUALIFICATION like
    // testing_string) so it flows through the dimension-spec CEL builder.
    val filter =
      buildCelExpression(
        dimensionSpecFilters =
          listOf(
            eventFilter {
              terms += eventTemplateField {
                path = "testing_only.testing_string_filterable"
                value = EventTemplateFieldKt.fieldValue { stringValue = "abc" }
              }
            }
          ),
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )
    assertThat(filter).contains("testing_only.testing_string_filterable == \"abc\"")
    assertCompilesCleanly(filter)
  }

  @Test
  fun `toCelValue escapes quotes in STRING literal via DimensionSpec filter`() {
    val filter =
      buildCelExpression(
        dimensionSpecFilters =
          listOf(
            eventFilter {
              terms += eventTemplateField {
                path = "testing_only.testing_string_filterable"
                value = EventTemplateFieldKt.fieldValue { stringValue = "he said \"hi\"" }
              }
            }
          ),
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )
    assertThat(filter).contains("testing_only.testing_string_filterable == \"he said \\\"hi\\\"\"")
    assertCompilesCleanly(filter)
  }

  // FLOAT_VALUE branch: finite values pass through; NaN / infinities throw.

  @Test
  fun `toCelValue emits finite FLOAT literal end to end`() {
    val filter =
      buildCelExpression(
        impressionQualificationFilterSpecs =
          listOf(
            impressionQualificationFilterSpec {
              mediaType = MediaType.OTHER
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "testing_only.testing_float"
                  value = EventTemplateFieldKt.fieldValue { floatValue = 0.5f }
                }
              }
            }
          ),
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )
    assertThat(filter).contains("testing_only.testing_float == 0.5")
    assertCompilesCleanly(filter)
  }

  @Test
  fun `toCelValue emits subnormal FLOAT literal (Float MIN_VALUE) without rejection`() {
    // Pin subnormals as accepted -- isFinite() is true for them, and a future overzealous
    // tightening of the finite-value guard (e.g. adding an isNormal() check) would silently
    // reject legitimate values. Float.MIN_VALUE = 1.4e-45f, the smallest positive subnormal.
    val filter =
      buildCelExpression(
        impressionQualificationFilterSpecs =
          listOf(
            impressionQualificationFilterSpec {
              mediaType = MediaType.OTHER
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "testing_only.testing_float"
                  value = EventTemplateFieldKt.fieldValue { floatValue = Float.MIN_VALUE }
                }
              }
            }
          ),
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )
    assertThat(filter).contains("testing_only.testing_float ==")
    assertCompilesCleanly(filter)
  }

  @Test
  fun `toCelValue rejects NaN FLOAT with IllegalArgumentException`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        buildCelExpression(
          impressionQualificationFilterSpecs =
            listOf(
              impressionQualificationFilterSpec {
                mediaType = MediaType.OTHER
                filters += eventFilter {
                  terms += eventTemplateField {
                    path = "testing_only.testing_float"
                    value = EventTemplateFieldKt.fieldValue { floatValue = Float.NaN }
                  }
                }
              }
            ),
          eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        )
      }
    assertThat(exception.message).contains("non-finite float")
  }

  @Test
  fun `toCelValue rejects positive Infinity FLOAT with IllegalArgumentException`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        buildCelExpression(
          impressionQualificationFilterSpecs =
            listOf(
              impressionQualificationFilterSpec {
                mediaType = MediaType.OTHER
                filters += eventFilter {
                  terms += eventTemplateField {
                    path = "testing_only.testing_float"
                    value = EventTemplateFieldKt.fieldValue { floatValue = Float.POSITIVE_INFINITY }
                  }
                }
              }
            ),
          eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        )
      }
    assertThat(exception.message).contains("non-finite float")
  }

  @Test
  fun `toCelValue rejects negative Infinity FLOAT with IllegalArgumentException`() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        buildCelExpression(
          impressionQualificationFilterSpecs =
            listOf(
              impressionQualificationFilterSpec {
                mediaType = MediaType.OTHER
                filters += eventFilter {
                  terms += eventTemplateField {
                    path = "testing_only.testing_float"
                    value = EventTemplateFieldKt.fieldValue { floatValue = Float.NEGATIVE_INFINITY }
                  }
                }
              }
            ),
          eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        )
      }
    assertThat(exception.message).contains("non-finite float")
  }

  /**
   * Asserts that [filter] compiles cleanly against a CEL environment built from `TestEvent`. Fails
   * the enclosing test with the CEL diagnostic if compilation fails.
   */
  private fun assertCompilesCleanly(filter: String) {
    // buildCelEnvironment(Message) registers only the top-level Message and its directly-nested
    // types. TestingOnly lives in a separate proto file (testing_only.proto) so we register it
    // explicitly here so CEL can resolve `testing_only.*` field references. See #4148.
    val celTypeRegistry = ProtoTypeRegistry.newRegistry()
    celTypeRegistry.registerMessage(TestEvent.getDefaultInstance())
    celTypeRegistry.registerMessage(Person.getDefaultInstance())
    celTypeRegistry.registerMessage(TestingOnly.getDefaultInstance())
    celTypeRegistry.registerMessage(TestingOnly.TestingArmIqf.getDefaultInstance())
    celTypeRegistry.registerMessage(TestingOnly.TestingArmFilterable.getDefaultInstance())
    celTypeRegistry.registerMessage(TestingOnly.TestingArmGroupable.getDefaultInstance())
    val descriptor = TestEvent.getDescriptor()
    val env =
      Env.newEnv(
        EnvOption.container(descriptor.fullName),
        EnvOption.customTypeProvider(celTypeRegistry),
        EnvOption.customTypeAdapter(celTypeRegistry),
        EnvOption.declarations(
          descriptor.fields.map {
            Decls.newVar(it.name, celTypeRegistry.findFieldType(descriptor.fullName, it.name).type)
          }
        ),
      )
    val astAndIssues = env.compile(filter)
    if (astAndIssues.hasIssues()) {
      fail("CEL failed to compile: $filter\nIssues: ${astAndIssues.issues}")
    }
  }

  // ---- oneof / nested-member handling tests (issue #4147) ----

  @Test
  fun `IQF-side buildCelExpression emits nested-member null guard for oneof member path`() {
    // testing_only.testing_arm_iqf.testing_arm_iqf_enum is IMPRESSION_QUALIFICATION on a field
    // nested inside the oneof member TestingArmIqf. Because oneof members can be unset (unlike
    // top-level template fields whose proto3 default is a zero-instance message), the CEL
    // needs a `<template>.<member> != null` guard before dereferencing the nested field.
    val filter =
      buildCelExpression(
        impressionQualificationFilterSpecs =
          listOf(
            impressionQualificationFilterSpec {
              mediaType = MediaType.OTHER
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "testing_only.testing_arm_iqf.testing_arm_iqf_enum"
                  value = EventTemplateFieldKt.fieldValue { enumValue = "ARM_IQF_1" }
                }
              }
            }
          ),
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        emitCelNullGuardsForNestedMembers = true,
      )
    assertThat(filter)
      .contains(
        "testing_only.testing_arm_iqf != null && " +
          "testing_only.testing_arm_iqf.testing_arm_iqf_enum == 1"
      )
    assertCompilesCleanly(filter)
  }

  @Test
  fun `IQF-side buildCelExpression omits nested-member null guard when flag is off (default)`() {
    // Default (flag off): nested-member paths emit the bare `path == value` form. Matches the
    // current Origin/Aquila workaround for EDPs that reject CEL filters containing `!= null`
    // clauses. The correctness trade-off (unset oneof members return proto defaults) is
    // documented on the flag.
    val filter =
      buildCelExpression(
        impressionQualificationFilterSpecs =
          listOf(
            impressionQualificationFilterSpec {
              mediaType = MediaType.OTHER
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "testing_only.testing_arm_iqf.testing_arm_iqf_enum"
                  value = EventTemplateFieldKt.fieldValue { enumValue = "ARM_IQF_1" }
                }
              }
            }
          ),
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )
    assertThat(filter).doesNotContain("testing_arm_iqf != null")
    assertThat(filter).contains("testing_only.testing_arm_iqf.testing_arm_iqf_enum == 1")
    assertCompilesCleanly(filter)
  }

  @Test
  fun `DimensionSpec-side buildCelExpression emits nested-member null guard for oneof member path`() {
    // testing_only.testing_arm_filterable.testing_arm_filterable_enum is
    // FILTERABLE + POPULATION_ATTRIBUTE. Same nested-member null-guard semantics apply on the
    // DimensionSpec code path — unlike the IQF-side, this path emits no template-level
    // null-guard, so the member-null-guard is the only guard on the emitted term.
    val filter =
      buildCelExpression(
        dimensionSpecFilters =
          listOf(
            eventFilter {
              terms += eventTemplateField {
                path = "testing_only.testing_arm_filterable.testing_arm_filterable_enum"
                value = EventTemplateFieldKt.fieldValue { enumValue = "ARM_FILT_1" }
              }
            }
          ),
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
        emitCelNullGuardsForNestedMembers = true,
      )
    assertThat(filter)
      .contains(
        "testing_only.testing_arm_filterable != null && " +
          "testing_only.testing_arm_filterable.testing_arm_filterable_enum == 1"
      )
    assertCompilesCleanly(filter)
  }

  @Test
  fun `DimensionSpec-side buildCelExpression omits nested-member null guard when flag is off (default)`() {
    // Default (flag off): same bare form as the IQF-side default. See flag KDoc for trade-off.
    val filter =
      buildCelExpression(
        dimensionSpecFilters =
          listOf(
            eventFilter {
              terms += eventTemplateField {
                path = "testing_only.testing_arm_filterable.testing_arm_filterable_enum"
                value = EventTemplateFieldKt.fieldValue { enumValue = "ARM_FILT_1" }
              }
            }
          ),
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )
    assertThat(filter).doesNotContain("!= null")
    assertThat(filter)
      .contains("testing_only.testing_arm_filterable.testing_arm_filterable_enum == 1")
    assertCompilesCleanly(filter)
  }

  @Test
  fun `top-level template paths still emit without a nested-member guard`() {
    // Regression: single-dot paths (`<template>.<field>`) do NOT get an member-null-guard because
    // the proto3 default for an unset top-level message is a zero-instance, not null. If
    // buildCelTerm ever over-triggers and wraps top-level terms, the emitted CEL would grow a
    // spurious `testing_only != null && ...` clause and break existing test assertions.
    val filter =
      buildCelExpression(
        dimensionSpecFilters =
          listOf(
            eventFilter {
              terms += eventTemplateField {
                path = "person.gender"
                value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          ),
        eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      )
    // Exactly the bare term, no null-guard clause.
    assertThat(filter).isEqualTo("person.gender == 1")
    assertCompilesCleanly(filter)
  }

  @Test
  fun `DimensionSpec Grouping predicates emit nested-member null guard for oneof member path`() {
    // testing_only.testing_arm_groupable.testing_arm_groupable_enum is GROUPABLE +
    // POPULATION_ATTRIBUTE inside a oneof member. Grouping predicates go through the same
    // buildCelTerm helper as filter emission, so nested-member paths get an member-null guard per
    // emitted predicate (one per non-zero enum ordinal). Top-level template paths (single-dot)
    // still emit bare `field == N` predicates without a null-guard -- see
    // `top-level template paths still emit without a nested-member guard` for that regression.
    val dataProviderPrimitiveReportingSetMap = buildMap {
      put(DATA_PROVIDER_NAME_1, PRIMITIVE_REPORTING_SET_1)
    }
    val resultGroupSpecs =
      listOf(
        resultGroupSpec {
          reportingUnit = reportingUnit { components += DATA_PROVIDER_NAME_1 }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            grouping =
              DimensionSpecKt.grouping {
                eventTemplateFields +=
                  "testing_only.testing_arm_groupable.testing_arm_groupable_enum"
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
        emitCelNullGuardsForNestedMembers = true,
      )

    val detailsList =
      reportingSetMetricCalculationSpecDetailsMap.getValue(PRIMITIVE_REPORTING_SET_1)
    val grouping = detailsList.single().groupingsList.single()
    // Two enum ordinals > 0 (ARM_GRP_1 = 1, ARM_GRP_2 = 2), so two predicates, each with the
    // member-null guard.
    assertThat(grouping.predicatesList)
      .containsExactly(
        "testing_only.testing_arm_groupable != null && " +
          "testing_only.testing_arm_groupable.testing_arm_groupable_enum == 1",
        "testing_only.testing_arm_groupable != null && " +
          "testing_only.testing_arm_groupable.testing_arm_groupable_enum == 2",
      )
      .inOrder()
  }
}
