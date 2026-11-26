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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.type.Date
import com.google.type.DayOfWeek
import com.google.type.date
import com.google.type.dateTime
import com.google.type.interval
import com.google.type.timeZone
import java.time.Instant
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.EventMessageDescriptor
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.Query
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.populationAttributeValue
import org.wfanet.measurement.api.v2alpha.query
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.vectorSet
import org.wfanet.measurement.api.v2alpha.vidFilter
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.DimensionSpec
import org.wfanet.measurement.internal.reporting.v2.DimensionSpecKt
import org.wfanet.measurement.internal.reporting.v2.EventFilter
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.MetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.ReportComputation
import org.wfanet.measurement.internal.reporting.v2.ReportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.ReportingUnitKt
import org.wfanet.measurement.internal.reporting.v2.ResultGroupMetricSpec
import org.wfanet.measurement.internal.reporting.v2.ResultGroupMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.ResultGroupQueryMapping
import org.wfanet.measurement.internal.reporting.v2.ResultGroupQueryMappingKt
import org.wfanet.measurement.internal.reporting.v2.ResultGroupSpec
import org.wfanet.measurement.internal.reporting.v2.basicReport
import org.wfanet.measurement.internal.reporting.v2.basicReportDetails
import org.wfanet.measurement.internal.reporting.v2.dataProviderKey
import org.wfanet.measurement.internal.reporting.v2.dimensionSpec
import org.wfanet.measurement.internal.reporting.v2.eventFilter
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.metricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.reportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.reportingInterval
import org.wfanet.measurement.internal.reporting.v2.reportingUnit
import org.wfanet.measurement.internal.reporting.v2.resultGroupMetricSpec
import org.wfanet.measurement.internal.reporting.v2.resultGroupQueryMapping
import org.wfanet.measurement.internal.reporting.v2.resultGroupSpec

@RunWith(JUnit4::class)
class MultimeasurementCompilerTest {
  private val compiler =
    MultimeasurementCompiler(
      eventTemplateFieldsByPath = TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
      emitCelNullGuardsForNestedMembers = false,
    )

  private fun compile(basicReport: BasicReport): List<Compiled> =
    compiler
      .compile(
        basicReport,
        COMPONENTS,
        List(basicReport.details.resultGroupSpecsCount) { REPORTING_UNIT_REPORTING_SET_ID },
      )
      .map { Compiled(it) }

  private fun compileSingle(vararg resultGroupSpecs: ResultGroupSpec) =
    compile(buildBasicReport(listOf(AMI_IQF), resultGroupSpecs.toList())).single()

  @Test
  fun `compile returns one multimeasurement per impression qualification filter`() {
    val compiled =
      compile(
        buildBasicReport(
          listOf(AMI_IQF, MRC_IQF, CUSTOM_IQF),
          listOf(buildSpec(BOTH_COMPONENTS, cumulativeReportingUnit())),
        )
      )

    assertThat(compiled.map { it.reportComputation.externalImpressionQualificationFilterId })
      .containsExactly("ami", "mrc", "")
      .inOrder()
    assertThat(compiled.map { it.multimeasurement.requisitionSpecs().first().filterExpression() })
      .containsExactly(
        "",
        "banner_ad != null && banner_ad.viewable == true",
        "banner_ad != null && banner_ad.viewable == false",
      )
      .inOrder()
  }

  @Test
  fun `compile shares identical vectors across slots and specs`() {
    val multimeasurement =
      compileSingle(
          buildSpec(
            BOTH_COMPONENTS,
            resultGroupMetricSpec {
              reportingUnit =
                ResultGroupMetricSpecKt.reportingUnitMetricSetSpec { cumulative = REACH }
              component = ResultGroupMetricSpecKt.componentMetricSetSpec { cumulative = REACH }
            },
          ),
          buildSpec(listOf(DATA_PROVIDER_ID_2), cumulativeReportingUnit()),
        )
        .multimeasurement

    assertThat(multimeasurement.dataProviderVectorsList.map { it.dataProvider })
      .containsExactly(DATA_PROVIDER_NAME_1, DATA_PROVIDER_NAME_2)
    assertThat(multimeasurement.dataProviderVectorsList.flatMap { it.vectorsList }.map { it.index })
      .containsExactly(0, 1)
    assertThat(multimeasurement.queriesList).hasSize(4)
  }

  @Test
  fun `compile builds requisition specs over the reporting interval`() {
    val multimeasurement =
      compileSingle(buildSpec(listOf(DATA_PROVIDER_ID_1), cumulativeReportingUnit()))
        .multimeasurement

    assertThat(multimeasurement.requisitionSpecs())
      .containsExactly(
        buildRequisitionSpec(EVENT_GROUP_NAME_1, REPORT_START_INSTANT, REPORT_END_INSTANT)
      )
  }

  @Test
  fun `compile sums component vectors for reporting unit reach`() {
    val multimeasurement =
      compileSingle(buildSpec(BOTH_COMPONENTS, cumulativeReportingUnit())).multimeasurement

    assertThat(multimeasurement.queriesList)
      .containsExactly(
        query {
          id = "0/c/2026-01-05/2026-01-19/ru"
          summands = vectorSet { baseVectors += listOf(0, 1) }
          maxFrequency = 1
        }
      )
  }

  @Test
  fun `compile sets max frequency from k plus reach`() {
    val query: Query =
      compileSingle(
          buildSpec(
            BOTH_COMPONENTS,
            resultGroupMetricSpec {
              reportingUnit =
                ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                  cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { kPlusReach = 5 }
                }
            },
          )
        )
        .multimeasurement
        .queriesList
        .single()

    assertThat(query.maxFrequency).isEqualTo(5)
  }

  @Test
  fun `compile maps a demographic filter to a vid filter attribute`() {
    val multimeasurement =
      compileSingle(
          buildSpec(
            BOTH_COMPONENTS,
            cumulativeReportingUnit(),
            dimensionSpec { filters += AGE_GROUP_18_TO_34_FILTER },
          )
        )
        .multimeasurement

    assertThat(multimeasurement.queriesList.single().vidFilter)
      .isEqualTo(
        vidFilter {
          attributes.put(
            "person.age_group",
            populationAttributeValue { enumValue = "YEARS_18_TO_34" },
          )
        }
      )
    assertThat(multimeasurement.requisitionSpecs().map { it.filterExpression() })
      .containsExactly("", "")
  }

  @Test
  fun `compile keeps a disjunctive demographic filter as an event filter`() {
    val multimeasurement =
      compileSingle(
          buildSpec(
            BOTH_COMPONENTS,
            cumulativeReportingUnit(),
            dimensionSpec {
              filters += eventFilter {
                terms += AGE_GROUP_18_TO_34_FILTER.termsList
                terms += eventTemplateField {
                  path = "person.age_group"
                  value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_35_TO_54" }
                }
              }
            },
          )
        )
        .multimeasurement

    assertThat(multimeasurement.queriesList.single().hasVidFilter()).isFalse()
    for (requisitionSpec in multimeasurement.requisitionSpecs()) {
      assertThat(requisitionSpec.filterExpression()).contains("person.age_group == 1")
      assertThat(requisitionSpec.filterExpression()).contains("person.age_group == 2")
    }
  }

  @Test
  fun `compile maps a demographic breakdown to grouping paths without adding vectors`() {
    val multimeasurement =
      compileSingle(
          buildSpec(
            BOTH_COMPONENTS,
            cumulativeReportingUnit(),
            dimensionSpec {
              grouping =
                DimensionSpecKt.grouping {
                  eventTemplateFields += "person.gender"
                  eventTemplateFields += "person.age_group"
                }
            },
          )
        )
        .multimeasurement

    assertThat(multimeasurement.queriesList.single().groupingPathsList)
      .containsExactly("person.gender", "person.age_group")
      .inOrder()
    assertThat(multimeasurement.requisitionSpecs()).hasSize(2)
  }

  @Test
  fun `compile throws when grouping by a non-population field`() {
    assertFailsWith<IllegalArgumentException> {
      compileSingle(
        buildSpec(
          BOTH_COMPONENTS,
          cumulativeReportingUnit(),
          dimensionSpec {
            grouping = DimensionSpecKt.grouping { eventTemplateFields += "banner_ad.viewable" }
          },
        )
      )
    }
  }

  @Test
  fun `compile sums weekly period vectors for cumulative windows`() {
    val multimeasurement =
      compileSingle(
          buildSpec(
            listOf(DATA_PROVIDER_ID_1),
            resultGroupMetricSpec {
              reportingUnit =
                ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                  nonCumulative = REACH
                  cumulative = REACH
                }
            },
            metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY },
          )
        )
        .multimeasurement

    assertThat(multimeasurement.dataProviderVectorsList.single().vectorsList.map { it.index })
      .containsExactly(0, 1)
    assertThat(multimeasurement.requisitionSpecs())
      .containsExactly(
        buildRequisitionSpec(EVENT_GROUP_NAME_1, REPORT_START_INSTANT, WEEK_1_END_INSTANT),
        buildRequisitionSpec(EVENT_GROUP_NAME_1, WEEK_1_END_INSTANT, REPORT_END_INSTANT),
      )
      .inOrder()
    assertThat(multimeasurement.queriesList.associate { it.id to it.summands.baseVectorsList })
      .containsExactly(
        "0/n/2026-01-05/2026-01-12/ru",
        listOf(0),
        "0/n/2026-01-12/2026-01-19/ru",
        listOf(1),
        "0/c/2026-01-05/2026-01-12/ru",
        listOf(0),
        "0/c/2026-01-05/2026-01-19/ru",
        listOf(0, 1),
      )
  }

  @Test
  fun `compile splits periods at the window boundaries of every spec`() {
    val multimeasurement =
      compileSingle(
          buildSpec(listOf(DATA_PROVIDER_ID_1), cumulativeReportingUnit()),
          buildSpec(
            listOf(DATA_PROVIDER_ID_1),
            cumulativeReportingUnit(),
            metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY },
          ),
        )
        .multimeasurement

    assertThat(multimeasurement.queriesList.associate { it.id to it.summands.baseVectorsList })
      .containsExactly(
        "0/c/2026-01-05/2026-01-19/ru",
        listOf(0, 1),
        "1/c/2026-01-05/2026-01-12/ru",
        listOf(0),
        "1/c/2026-01-05/2026-01-19/ru",
        listOf(0, 1),
      )
  }

  @Test
  fun `compile returns the same query IDs when recompiled`() {
    val basicReport =
      buildBasicReport(
        listOf(AMI_IQF, MRC_IQF),
        listOf(
          buildSpec(
            BOTH_COMPONENTS,
            resultGroupMetricSpec {
              reportingUnit =
                ResultGroupMetricSpecKt.reportingUnitMetricSetSpec { nonCumulative = REACH }
              component = ResultGroupMetricSpecKt.componentMetricSetSpec { cumulative = REACH }
            },
            metricFrequency = metricFrequencySpec { weekly = DayOfWeek.WEDNESDAY },
          )
        ),
      )

    val first = compile(basicReport)
    val second = compile(basicReport)

    assertThat(second).isEqualTo(first)
    assertThat(first[0].multimeasurement.queriesList.map { it.id })
      .containsExactly(
        "0/n/2026-01-05/2026-01-07/ru",
        "0/n/2026-01-07/2026-01-14/ru",
        "0/n/2026-01-14/2026-01-19/ru",
        "0/c/2026-01-05/2026-01-07/c0",
        "0/c/2026-01-05/2026-01-07/c1",
        "0/c/2026-01-05/2026-01-14/c0",
        "0/c/2026-01-05/2026-01-14/c1",
        "0/c/2026-01-05/2026-01-19/c0",
        "0/c/2026-01-05/2026-01-19/c1",
      )
    assertThat(first[1].multimeasurement.queriesList.map { it.id })
      .containsExactlyElementsIn(first[0].multimeasurement.queriesList.map { it.id })
  }

  @Test
  fun `compile maps every query to its result group slot`() {
    val compiled =
      compileSingle(
        buildSpec(
          BOTH_COMPONENTS,
          resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec { cumulative = REACH }
            component = ResultGroupMetricSpecKt.componentMetricSetSpec { nonCumulative = REACH }
          },
        )
      )
    val window = WindowDates(REPORT_START_DATE, REPORT_END_DATE)

    assertThat(compiled.reportComputation.mappingsList)
      .containsExactly(
        buildMapping("0/c/2026-01-05/2026-01-19/ru", window, cumulative = true) {
          reportingUnit =
            ResultGroupQueryMappingKt.reportingSet {
              externalReportingSetId = REPORTING_UNIT_REPORTING_SET_ID
            }
        },
        buildMapping("0/n/2026-01-05/2026-01-19/c0", window, cumulative = false) {
          component =
            ResultGroupQueryMappingKt.reportingSet { externalReportingSetId = REPORTING_SET_ID_1 }
        },
        buildMapping("0/n/2026-01-05/2026-01-19/c1", window, cumulative = false) {
          component =
            ResultGroupQueryMappingKt.reportingSet { externalReportingSetId = REPORTING_SET_ID_2 }
        },
      )
      .inOrder()
    assertThat(compiled.multimeasurement.queriesList.map { it.id })
      .containsExactlyElementsIn(compiled.reportComputation.mappingsList.map { it.queryId })
      .inOrder()
  }

  @Test
  fun `compile slot map survives serialization`() {
    val compiled =
      compileSingle(
        buildSpec(
          BOTH_COMPONENTS,
          resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec { cumulative = REACH }
            component = ResultGroupMetricSpecKt.componentMetricSetSpec { cumulative = REACH }
          },
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY },
        )
      )

    val parsed = ReportComputation.parseFrom(compiled.reportComputation.toByteString())

    assertThat(parsed).isEqualTo(compiled.reportComputation)
    val mappingsByQueryId: Map<String, ResultGroupQueryMapping> =
      parsed.mappingsList.associateBy { it.queryId }
    assertThat(mappingsByQueryId.keys)
      .containsExactlyElementsIn(compiled.multimeasurement.queriesList.map { it.id })
  }

  @Test
  fun `compile shares one query between slots with the same cells`() {
    val compiled =
      compileSingle(
        buildSpec(
          BOTH_COMPONENTS,
          resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec { cumulative = REACH }
          },
        ),
        buildSpec(
          BOTH_COMPONENTS,
          resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    reach = true
                    kPlusReach = 5
                  }
              }
          },
        ),
      )

    val query: Query = compiled.multimeasurement.queriesList.single()
    assertThat(query.maxFrequency).isEqualTo(5)
    assertThat(compiled.reportComputation.mappingsList.map { it.queryId })
      .containsExactly(query.id, query.id)
  }

  @Test
  fun `compile throws for component unique reach`() {
    assertFailsWith<IllegalArgumentException> {
      compileSingle(
        buildSpec(
          BOTH_COMPONENTS,
          resultGroupMetricSpec {
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                cumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              }
          },
        )
      )
    }
  }

  @Test
  fun `compile throws for component intersection`() {
    assertFailsWith<IllegalArgumentException> {
      compileSingle(
        buildSpec(
          BOTH_COMPONENTS,
          resultGroupMetricSpec {
            componentIntersection =
              ResultGroupMetricSpecKt.componentIntersectionMetricSetSpec {
                contributorCount += 2
                cumulative = REACH
              }
          },
        )
      )
    }
  }

  @Test
  fun `compile throws for stacked incremental reach`() {
    assertFailsWith<IllegalArgumentException> {
      compileSingle(
        buildSpec(
          BOTH_COMPONENTS,
          resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec { stackedIncrementalReach = true }
          },
        )
      )
    }
  }

  @Test
  fun `compile throws for an unresolved component`() {
    assertFailsWith<IllegalArgumentException> {
      compileSingle(buildSpec(listOf("unknown"), cumulativeReportingUnit()))
    }
  }

  @Test
  fun `compile throws for an unknown impression qualification filter`() {
    assertFailsWith<IllegalArgumentException> {
      compile(
        buildBasicReport(
          listOf(
            reportingImpressionQualificationFilter {
              externalImpressionQualificationFilterId = "unknown"
            }
          ),
          listOf(buildSpec(BOTH_COMPONENTS, cumulativeReportingUnit())),
        )
      )
    }
  }

  /** A compiled [ReportComputation] with its multimeasurement parsed. */
  private class Compiled(val reportComputation: ReportComputation) {
    val multimeasurement: Measurement.Multimeasurement =
      Measurement.Multimeasurement.parseFrom(reportComputation.multimeasurement)
  }

  companion object {
    private data class WindowDates(val start: Date, val end: Date)

    private val TEST_EVENT_DESCRIPTOR = EventMessageDescriptor(TestEvent.getDescriptor())

    private const val MEASUREMENT_CONSUMER_ID = "mc"
    private const val BASIC_REPORT_ID = "basic-report"
    private const val DATA_PROVIDER_ID_1 = "dp1"
    private const val DATA_PROVIDER_ID_2 = "dp2"
    private const val DATA_PROVIDER_NAME_1 = "dataProviders/$DATA_PROVIDER_ID_1"
    private const val DATA_PROVIDER_NAME_2 = "dataProviders/$DATA_PROVIDER_ID_2"
    private const val EVENT_GROUP_NAME_1 = "$DATA_PROVIDER_NAME_1/eventGroups/eg1"
    private const val EVENT_GROUP_NAME_2 = "$DATA_PROVIDER_NAME_2/eventGroups/eg2"
    private const val REPORTING_SET_ID_1 = "rs-1"
    private const val REPORTING_SET_ID_2 = "rs-2"
    private val BOTH_COMPONENTS = listOf(DATA_PROVIDER_ID_1, DATA_PROVIDER_ID_2)

    private val COMPONENTS =
      mapOf(
        DATA_PROVIDER_NAME_1 to
          MultimeasurementCompiler.ReportingUnitComponent(
            REPORTING_SET_ID_1,
            listOf(EVENT_GROUP_NAME_1),
          ),
        DATA_PROVIDER_NAME_2 to
          MultimeasurementCompiler.ReportingUnitComponent(
            REPORTING_SET_ID_2,
            listOf(EVENT_GROUP_NAME_2),
          ),
      )

    private const val REPORTING_UNIT_REPORTING_SET_ID = "reporting-unit"

    private val AMI_IQF: ReportingImpressionQualificationFilter =
      reportingImpressionQualificationFilter {
        externalImpressionQualificationFilterId = "ami"
      }
    private val MRC_IQF: ReportingImpressionQualificationFilter =
      reportingImpressionQualificationFilter {
        externalImpressionQualificationFilterId = "mrc"
        filterSpecs += buildViewableFilterSpec(true)
      }
    private val CUSTOM_IQF: ReportingImpressionQualificationFilter =
      reportingImpressionQualificationFilter {
        filterSpecs += buildViewableFilterSpec(false)
      }

    private val AGE_GROUP_18_TO_34_FILTER: EventFilter = eventFilter {
      terms += eventTemplateField {
        path = "person.age_group"
        value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
      }
    }

    private val REACH: ResultGroupMetricSpec.BasicMetricSetSpec =
      ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }

    private val REPORT_START_DATE = date {
      year = 2026
      month = 1
      day = 5
    }
    private val REPORT_END_DATE = date {
      year = 2026
      month = 1
      day = 19
    }
    private val REPORT_START_INSTANT = Instant.parse("2026-01-05T00:00:00Z")
    private val WEEK_1_END_INSTANT = Instant.parse("2026-01-12T00:00:00Z")
    private val REPORT_END_INSTANT = Instant.parse("2026-01-19T00:00:00Z")

    private fun buildViewableFilterSpec(viewable: Boolean): ImpressionQualificationFilterSpec =
      impressionQualificationFilterSpec {
        mediaType = ImpressionQualificationFilterSpec.MediaType.DISPLAY
        filters += eventFilter {
          terms += eventTemplateField {
            path = "banner_ad.viewable"
            value = EventTemplateFieldKt.fieldValue { boolValue = viewable }
          }
        }
      }

    private fun cumulativeReportingUnit(): ResultGroupMetricSpec = resultGroupMetricSpec {
      reportingUnit = ResultGroupMetricSpecKt.reportingUnitMetricSetSpec { cumulative = REACH }
    }

    private fun buildSpec(
      dataProviderIds: List<String>,
      metricSpec: ResultGroupMetricSpec,
      dimension: DimensionSpec = DimensionSpec.getDefaultInstance(),
      metricFrequency: MetricFrequencySpec = metricFrequencySpec { total = true },
    ): ResultGroupSpec = resultGroupSpec {
      reportingUnit = reportingUnit {
        dataProviderKeys =
          ReportingUnitKt.dataProviderKeys {
            for (dataProviderId in dataProviderIds) {
              dataProviderKeys += dataProviderKey { cmmsDataProviderId = dataProviderId }
            }
          }
      }
      this.metricFrequency = metricFrequency
      dimensionSpec = dimension
      resultGroupMetricSpec = metricSpec
    }

    private fun buildBasicReport(
      impressionQualificationFilters: List<ReportingImpressionQualificationFilter>,
      resultGroupSpecs: List<ResultGroupSpec>,
    ): BasicReport = basicReport {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
      externalBasicReportId = BASIC_REPORT_ID
      details = basicReportDetails {
        effectiveImpressionQualificationFilters += impressionQualificationFilters
        reportingInterval = reportingInterval {
          reportStartDate = REPORT_START_DATE
          effectiveReportStart = dateTime {
            year = 2026
            month = 1
            day = 5
            timeZone = timeZone { id = "UTC" }
          }
          reportEnd = REPORT_END_DATE
        }
        this.resultGroupSpecs += resultGroupSpecs
      }
    }

    private fun buildRequisitionSpec(
      eventGroupName: String,
      start: Instant,
      end: Instant,
    ): RequisitionSpec {
      return requisitionSpec {
        events =
          RequisitionSpecKt.events {
            eventGroups +=
              RequisitionSpecKt.eventGroupEntry {
                key = eventGroupName
                value =
                  RequisitionSpecKt.EventGroupEntryKt.value {
                    collectionInterval = interval {
                      startTime = start.toProtoTime()
                      endTime = end.toProtoTime()
                    }
                  }
              }
          }
      }
    }

    private fun buildMapping(
      queryId: String,
      window: WindowDates,
      cumulative: Boolean,
      fill: ResultGroupQueryMappingKt.Dsl.() -> Unit,
    ): ResultGroupQueryMapping = resultGroupQueryMapping {
      this.queryId = queryId
      resultGroupSpecIndex = 0
      windowStart = window.start
      windowEnd = window.end
      this.cumulative = cumulative
      fill()
    }

    private fun Measurement.Multimeasurement.requisitionSpecs(): List<RequisitionSpec> =
      dataProviderVectorsList
        .flatMap { it.vectorsList }
        .sortedBy { it.index }
        .map { it.requisitionSpec }

    private fun RequisitionSpec.filterExpression(): String =
      events.eventGroupsList.single().value.filter.expression
  }
}
