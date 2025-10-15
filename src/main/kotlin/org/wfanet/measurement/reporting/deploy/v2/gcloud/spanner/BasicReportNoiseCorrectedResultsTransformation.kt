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

import com.google.protobuf.Timestamp
import com.google.type.Date
import com.google.type.copy
import org.wfanet.measurement.common.toTimestamp
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.EventFilter
import org.wfanet.measurement.internal.reporting.v2.EventTemplateField
import org.wfanet.measurement.internal.reporting.v2.MetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.ReportResult
import org.wfanet.measurement.internal.reporting.v2.ReportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ResultGroup
import org.wfanet.measurement.internal.reporting.v2.ResultGroupKt
import org.wfanet.measurement.internal.reporting.v2.ResultGroupMetricSpec
import org.wfanet.measurement.internal.reporting.v2.ResultGroupSpec
import org.wfanet.measurement.internal.reporting.v2.metricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.reportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.resultGroup

/**
 * Builds a list of [ResultGroup]s for the [BasicReport] given the [ReportResult]
 *
 * @param basicReport [BasicReport] to create [ResultGroup]s for
 * @param reportResult [ReportResult] to get results from
 * @param primitiveReportingSetByDataProviderId Map that contains the Primitive [ReportingSet] for
 *   each DataProviderId represented by the CampaignGroup [ReportingSet]
 * @param compositeReportingSetIdBySetExpression Map that contains ExternalReportingSetIDs for each
 *   [ReportingSet.SetExpression] represented by the CampaignGroup [ReportingSet]
 */
fun buildResultGroups(
  basicReport: BasicReport,
  reportResult: ReportResult,
  primitiveReportingSetByDataProviderId: Map<String, ReportingSet>,
  compositeReportingSetIdBySetExpression: Map<ReportingSet.SetExpression, String>,
): List<ResultGroup> {
  val reportingWindowResultValuesByResultGroupSpecKey:
    Map<ResultGroupSpecKey, Map<ReportingWindowResultKey, ReportingWindowResultValues>> =
    buildReportingWindowResultValuesByResultGroupSpecKey(basicReport, reportResult)

  // If there is no custom ReportingImpressionQualificationFilter, this will be unused so the value
  // doesn't matter.
  var customReportingImpressionQualificationFilter: ReportingImpressionQualificationFilter =
    reportingImpressionQualificationFilter {}

  val reportingImpressionQualificationFilterByExternalId:
    Map<String, ReportingImpressionQualificationFilter> =
    buildMap {
      for (impressionQualificationFilter in
        basicReport.details.impressionQualificationFiltersList) {
        if (impressionQualificationFilter.externalImpressionQualificationFilterId.isEmpty()) {
          customReportingImpressionQualificationFilter = impressionQualificationFilter
        } else {
          put(
            impressionQualificationFilter.externalImpressionQualificationFilterId,
            impressionQualificationFilter,
          )
        }
      }
    }

  val reportStartTimestamp: Timestamp =
    basicReport.details.reportingInterval.reportStart.toTimestamp()

  return buildList {
    for (resultGroupSpec in basicReport.details.resultGroupSpecsList) {
      val reportingUnitDataProviderIds =
        resultGroupSpec.reportingUnit.dataProviderKeys.dataProviderKeysList.map {
          it.cmmsDataProviderId
        }

      // ReportingUnitSummary will be the same for every Result in the ResultGroup.
      val reportingUnitSummary =
        ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
          for (dataProviderId in reportingUnitDataProviderIds) {
            reportingUnitComponentSummary +=
              ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                cmmsDataProviderId = dataProviderId
                val primitiveReportingSet =
                  primitiveReportingSetByDataProviderId.getValue(dataProviderId)
                for (eventGroupKey in primitiveReportingSet.primitive.eventGroupKeysList) {
                  eventGroupSummaries +=
                    ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                      .eventGroupSummary {
                        cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
                        cmmsEventGroupId = eventGroupKey.cmmsEventGroupId
                      }
                }
              }
          }
        }

      // If the value is blank, it wouldn't be used
      val reportingUnitReportingSetId =
        if (reportingUnitDataProviderIds.size == 1) {
          primitiveReportingSetByDataProviderId
            .getValue(reportingUnitDataProviderIds.first())
            .externalReportingSetId
        } else if (
          resultGroupSpec.resultGroupMetricSpec.hasReportingUnit() &&
            (resultGroupSpec.resultGroupMetricSpec.reportingUnit.hasNonCumulative() ||
              resultGroupSpec.resultGroupMetricSpec.reportingUnit.hasCumulative())
        ) {
          compositeReportingSetIdBySetExpression.getValue(
            buildUnionSetExpression(
              reportingUnitDataProviderIds.map {
                primitiveReportingSetByDataProviderId.getValue(it).externalReportingSetId
              }
            )
          )
        } else {
          ""
        }

      // ExternalReportingSetIDs for StackedIncrementalReach.
      val incrementalReportingSetIds: List<String> = buildList {
        if (
          resultGroupSpec.resultGroupMetricSpec.hasReportingUnit() &&
            resultGroupSpec.resultGroupMetricSpec.reportingUnit.stackedIncrementalReach
        ) {
          val firstReportingSetId =
            primitiveReportingSetByDataProviderId
              .getValue(reportingUnitDataProviderIds.first())
              .externalReportingSetId

          add(firstReportingSetId)

          val primitiveReportingSetIds = mutableListOf<String>()
          primitiveReportingSetIds.add(firstReportingSetId)
          for (dataProviderId in
            reportingUnitDataProviderIds.subList(1, reportingUnitDataProviderIds.size)) {
            primitiveReportingSetIds.add(
              primitiveReportingSetByDataProviderId.getValue(dataProviderId).externalReportingSetId
            )
            val setExpression = buildUnionSetExpression(primitiveReportingSetIds)
            add(compositeReportingSetIdBySetExpression.getValue(setExpression))
          }
        }
      }

      val componentReportingSetIdsByDataProviderId: Map<String, ComponentReportingSetIds> =
        buildMap {
          val hasUniqueMetricSet =
            resultGroupSpec.resultGroupMetricSpec.hasComponent() &&
              (resultGroupSpec.resultGroupMetricSpec.component.hasNonCumulativeUnique() ||
                resultGroupSpec.resultGroupMetricSpec.component.hasCumulativeUnique())

          val allComponentsReportingSetId =
            if (reportingUnitDataProviderIds.size == 1) {
              null
            } else if (hasUniqueMetricSet) {
              compositeReportingSetIdBySetExpression.getValue(
                buildUnionSetExpression(
                  reportingUnitDataProviderIds.map {
                    primitiveReportingSetByDataProviderId.getValue(it).externalReportingSetId
                  }
                )
              )
            } else {
              null
            }

          for (dataProviderId in reportingUnitDataProviderIds) {
            val primitiveReportingSet =
              primitiveReportingSetByDataProviderId.getValue(dataProviderId)
            val componentReportingSetId = primitiveReportingSet.externalReportingSetId
            val allComponentsWithoutCurrentComponentReportingSetId =
              if (reportingUnitDataProviderIds.size == 2) {
                primitiveReportingSetByDataProviderId
                  .getValue(reportingUnitDataProviderIds.first { it != dataProviderId })
                  .externalReportingSetId
                // If there is no UniqueMetricSet requested, the externalReportingSetId may not
                // exist
              } else if (reportingUnitDataProviderIds.size == 1) {
                null
              } else if (hasUniqueMetricSet) {
                compositeReportingSetIdBySetExpression.getValue(
                  buildUnionSetExpression(
                    reportingUnitDataProviderIds
                      .filter { it != dataProviderId }
                      .map {
                        primitiveReportingSetByDataProviderId.getValue(it).externalReportingSetId
                      }
                  )
                )
              } else {
                null
              }
            put(
              dataProviderId,
              ComponentReportingSetIds(
                componentReportingSetId = componentReportingSetId,
                reportingUnitReportingSetId = allComponentsReportingSetId,
                reportingUnitWithoutComponentReportingSetId =
                  allComponentsWithoutCurrentComponentReportingSetId,
              ),
            )
          }
        }

      val resultGroup = resultGroup {
        title = resultGroupSpec.title

        val reportingWindowResultValuesMap:
          Map<ReportingWindowResultKey, ReportingWindowResultValues> =
          reportingWindowResultValuesByResultGroupSpecKey.getValue(
            ResultGroupSpecKey(
              metricFrequencySpec = resultGroupSpec.metricFrequency,
              groupingFields = resultGroupSpec.dimensionSpec.grouping.eventTemplateFieldsList,
              eventFilters = resultGroupSpec.dimensionSpec.filtersList,
            )
          )

        for (reportingWindowResults in reportingWindowResultValuesMap.entries) {
          results +=
            ResultGroupKt.result {
              metadata =
                ResultGroupKt.metricMetadata {
                  this.reportingUnitSummary = reportingUnitSummary

                  // If the windowStartDate is empty, then there are no non-cumulative metrics.
                  if (reportingWindowResults.key.windowStartDate.day != 0) {
                    nonCumulativeMetricStartTime =
                      basicReport.details.reportingInterval.reportStart
                        .copy {
                          day = reportingWindowResults.key.windowStartDate.day
                          month = reportingWindowResults.key.windowStartDate.month
                          year = reportingWindowResults.key.windowStartDate.year
                        }
                        .toTimestamp()
                  }

                  cumulativeMetricStartTime = reportStartTimestamp

                  metricEndTime =
                    basicReport.details.reportingInterval.reportStart
                      .copy {
                        day = reportingWindowResults.key.windowEndDate.day
                        month = reportingWindowResults.key.windowEndDate.month
                        year = reportingWindowResults.key.windowEndDate.year
                      }
                      .toTimestamp()

                  metricFrequencySpec = resultGroupSpec.metricFrequency

                  dimensionSpecSummary =
                    ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                      groupings += reportingWindowResults.key.groupings
                      filters += resultGroupSpec.dimensionSpec.filtersList
                    }

                  filter =
                    if (
                      reportingWindowResults.key.externalImpressionQualificationFilterId != null
                    ) {
                      reportingImpressionQualificationFilterByExternalId.getValue(
                        reportingWindowResults.key.externalImpressionQualificationFilterId!!
                      )
                    } else {
                      customReportingImpressionQualificationFilter
                    }
                }

              metricSet =
                ResultGroupKt.metricSet {
                  if (resultGroupSpec.resultGroupMetricSpec.populationSize) {
                    populationSize = reportingWindowResults.value.populationSize
                  }

                  if (resultGroupSpec.resultGroupMetricSpec.hasReportingUnit()) {
                    reportingUnit =
                      buildReportingUnitMetricSet(
                        resultGroupSpec.resultGroupMetricSpec.reportingUnit,
                        reportingUnitReportingSetId,
                        incrementalReportingSetIds,
                        reportingWindowResults.value.reportResultValuesByExternalReportingSetId,
                      )
                  }

                  if (resultGroupSpec.resultGroupMetricSpec.hasComponent()) {
                    for (dataProviderId in reportingUnitDataProviderIds) {
                      components +=
                        ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                          key = dataProviderId
                          value =
                            buildComponentMetricSet(
                              resultGroupSpec.resultGroupMetricSpec.component,
                              componentReportingSetIdsByDataProviderId.getValue(dataProviderId),
                              reportingWindowResults.value
                                .reportResultValuesByExternalReportingSetId,
                            )
                        }
                    }
                  }
                }
            }
        }
      }

      add(resultGroup)
    }
  }
}

/**
 * Builds a Map for finding all results for each [ResultGroupSpec] in the [BasicReport]
 *
 * @param basicReport [BasicReport] to get [ResultGroupSpec]s from
 * @param reportResult [ReportResult] to get [ReportResult.ReportingSetResult]s from
 */
private fun buildReportingWindowResultValuesByResultGroupSpecKey(
  basicReport: BasicReport,
  reportResult: ReportResult,
): Map<ResultGroupSpecKey, Map<ReportingWindowResultKey, ReportingWindowResultValues>> {
  val totalMetricFrequencySpec: MetricFrequencySpec = metricFrequencySpec { total = true }

  // ReportResult only has a weekly enum and not the DayOfWeek. Without that information, the
  // assumption is that the DayOfWeek is the same for all weekly frequencies.
  var weeklyMetricFrequencySpec: MetricFrequencySpec = metricFrequencySpec {}
  for (resultGroupSpec in basicReport.details.resultGroupSpecsList) {
    if (resultGroupSpec.metricFrequency.hasWeekly()) {
      weeklyMetricFrequencySpec = resultGroupSpec.metricFrequency
    }
  }

  return buildMap<
    ResultGroupSpecKey,
    MutableMap<ReportingWindowResultKey, ReportingWindowResultValues>,
  > {
    for (reportingSetResult in reportResult.reportingSetResultsList) {
      for (reportingWindowResult in reportingSetResult.reportingWindowResultsList) {
        val externalImpressionQualificationFilterId: String? =
          if (reportingSetResult.custom) {
            null
          } else {
            reportingSetResult.externalImpressionQualificationFilterId
          }

        val metricFrequencySpec: MetricFrequencySpec =
          when (reportingSetResult.metricFrequencyType) {
            ReportResult.MetricFrequencyType.TOTAL -> {
              totalMetricFrequencySpec
            }
            ReportResult.MetricFrequencyType.WEEKLY -> {
              weeklyMetricFrequencySpec
            }
            else -> {
              throw IllegalStateException(
                "Unknown metric frequency type: ${reportingSetResult.metricFrequencyType}"
              )
            }
          }

        // This isn't unique to a ResultGroupSpec because it doesn't include the ReportingUnit,
        // but it is unique enough to get the results for every ResultGroupSpec that matches this.
        val resultGroupSpecKey =
          ResultGroupSpecKey(
            metricFrequencySpec = metricFrequencySpec,
            groupingFields = reportingSetResult.groupingsList.map { it.path },
            eventFilters = reportingSetResult.eventFiltersList,
          )

        val reportingWindowResultMap:
          MutableMap<ReportingWindowResultKey, ReportingWindowResultValues> =
          getOrPut(resultGroupSpecKey) { mutableMapOf() }

        val key =
          ReportingWindowResultKey(
            groupings = reportingSetResult.groupingsList,
            windowStartDate = reportingWindowResult.windowStartDate,
            windowEndDate = reportingWindowResult.windowEndDate,
            externalImpressionQualificationFilterId = externalImpressionQualificationFilterId,
          )

        val value =
          ReportingWindowResultValues(
            populationSize = reportingSetResult.populationSize,
            reportResultValuesByExternalReportingSetId = mutableMapOf(),
          )

        val reportingWindowResultValues: ReportingWindowResultValues =
          reportingWindowResultMap.getOrPut(key) { value }

        if (
          reportingWindowResultValues.populationSize == 0 && reportingSetResult.populationSize > 0
        ) {
          reportingWindowResultValues.populationSize = reportingSetResult.populationSize
        }

        reportingWindowResultValues.reportResultValuesByExternalReportingSetId[
            reportingSetResult.externalReportingSetId] =
          reportingWindowResult.denoisedReportResultValues
      }
    }
  }
}

/**
 * Builds a [ResultGroup.MetricSet.ReportingUnitMetricSet] from a
 * [ResultGroupMetricSpec.ReportingUnitMetricSetSpec] and denoised values
 *
 * @param reportingUnitMetricSetSpec [ResultGroupMetricSpec.ReportingUnitMetricSetSpec] specifies
 *   which fields to set.
 * @param reportingUnitReportingSetId String representing ExternalReportingSetID for the
 *   ReportingUnit. Is empty if it isn't used.
 * @param incrementalReportingSetIds List of Strings representing ExternalReportingSetIDs starting
 *   with the ID of the Primitive ReportingSet for the first component of the ReportingUnit, then
 *   the ID of the Composite ReportingSet for the first two components of the ReportingUnit, etc.,
 *   until the ID of the Composite ReportingSet for the entire ReportingUnit. Is empty if it isn't
 *   used.
 * @param reportResultValuesByExternalReportingSetId Map containing denoised values.
 */
private fun buildReportingUnitMetricSet(
  reportingUnitMetricSetSpec: ResultGroupMetricSpec.ReportingUnitMetricSetSpec,
  reportingUnitReportingSetId: String,
  incrementalReportingSetIds: List<String>,
  reportResultValuesByExternalReportingSetId:
    Map<String, ReportResult.ReportingSetResult.ReportingWindowResult.ReportResultValues>,
): ResultGroup.MetricSet.ReportingUnitMetricSet {
  return ResultGroupKt.MetricSetKt.reportingUnitMetricSet {
    val reportingUnitDenoisedResultValues =
      reportResultValuesByExternalReportingSetId.getValue(reportingUnitReportingSetId)

    if (reportingUnitMetricSetSpec.hasNonCumulative()) {
      nonCumulative =
        buildBasicMetricSet(
          reportingUnitMetricSetSpec.nonCumulative,
          reportingUnitDenoisedResultValues.nonCumulativeResults,
        )
    }

    if (reportingUnitMetricSetSpec.hasCumulative()) {
      cumulative =
        buildBasicMetricSet(
          reportingUnitMetricSetSpec.cumulative,
          reportingUnitDenoisedResultValues.cumulativeResults,
        )
    }

    if (reportingUnitMetricSetSpec.stackedIncrementalReach) {
      var prevReach = 0
      // Subtracts the previous reach to get the difference in reach.
      for (reportingSetId in incrementalReportingSetIds) {
        val denoisedValues = reportResultValuesByExternalReportingSetId.getValue(reportingSetId)
        stackedIncrementalReach += denoisedValues.cumulativeResults.reach - prevReach
        prevReach = denoisedValues.cumulativeResults.reach
      }
    }
  }
}

private fun buildComponentMetricSet(
  componentMetricSetSpec: ResultGroupMetricSpec.ComponentMetricSetSpec,
  componentReportingSetIds: ComponentReportingSetIds,
  reportResultValuesByExternalReportingSetId:
    Map<String, ReportResult.ReportingSetResult.ReportingWindowResult.ReportResultValues>,
): ResultGroup.MetricSet.ComponentMetricSet {
  val componentDenoisedResultValues =
    reportResultValuesByExternalReportingSetId.getValue(
      componentReportingSetIds.componentReportingSetId
    )

  return ResultGroupKt.MetricSetKt.componentMetricSet {
    if (componentMetricSetSpec.hasNonCumulative()) {
      nonCumulative =
        buildBasicMetricSet(
          componentMetricSetSpec.nonCumulative,
          componentDenoisedResultValues.nonCumulativeResults,
        )
    }

    if (componentMetricSetSpec.hasCumulative()) {
      cumulative =
        buildBasicMetricSet(
          componentMetricSetSpec.cumulative,
          componentDenoisedResultValues.cumulativeResults,
        )
    }

    if (
      componentMetricSetSpec.hasNonCumulativeUnique() &&
        componentReportingSetIds.reportingUnitReportingSetId != null &&
        componentReportingSetIds.reportingUnitWithoutComponentReportingSetId != null
    ) {
      nonCumulativeUnique =
        ResultGroupKt.MetricSetKt.uniqueMetricSet {
          reach =
            reportResultValuesByExternalReportingSetId
              .getValue(componentReportingSetIds.reportingUnitReportingSetId)
              .nonCumulativeResults
              .reach -
              reportResultValuesByExternalReportingSetId
                .getValue(componentReportingSetIds.reportingUnitWithoutComponentReportingSetId)
                .nonCumulativeResults
                .reach
        }
    }

    if (
      componentMetricSetSpec.hasCumulativeUnique() &&
        componentReportingSetIds.reportingUnitReportingSetId != null &&
        componentReportingSetIds.reportingUnitWithoutComponentReportingSetId != null
    ) {
      cumulativeUnique =
        ResultGroupKt.MetricSetKt.uniqueMetricSet {
          reach =
            reportResultValuesByExternalReportingSetId
              .getValue(componentReportingSetIds.reportingUnitReportingSetId)
              .cumulativeResults
              .reach -
              reportResultValuesByExternalReportingSetId
                .getValue(componentReportingSetIds.reportingUnitWithoutComponentReportingSetId)
                .cumulativeResults
                .reach
        }
    }
  }
}

/**
 * Builds a [ResultGroup.MetricSet.BasicMetricSet] from an existing one and a
 * [ResultGroupMetricSpec.BasicMetricSetSpec] detailing which fields to set.
 *
 * @param basicMetricSetSpec [ResultGroupMetricSpec.BasicMetricSetSpec] specifies which fields to
 *   set.
 * @param denoisedValues [ResultGroup.MetricSet.BasicMetricSet] contains all the denoised values,
 *   which may have fields set that the basicMetricSetSpec doesn't ask for.
 */
private fun buildBasicMetricSet(
  basicMetricSetSpec: ResultGroupMetricSpec.BasicMetricSetSpec,
  denoisedValues: ResultGroup.MetricSet.BasicMetricSet,
): ResultGroup.MetricSet.BasicMetricSet {
  return ResultGroupKt.MetricSetKt.basicMetricSet {
    if (basicMetricSetSpec.reach) {
      reach = denoisedValues.reach
    }
    if (basicMetricSetSpec.percentReach) {
      percentReach = denoisedValues.percentReach
    }
    if (basicMetricSetSpec.kPlusReach > 0) {
      kPlusReach += denoisedValues.kPlusReachList
    }
    if (basicMetricSetSpec.percentKPlusReach) {
      percentKPlusReach += denoisedValues.percentKPlusReachList
    }
    if (basicMetricSetSpec.averageFrequency) {
      averageFrequency = denoisedValues.averageFrequency
    }
    if (basicMetricSetSpec.impressions) {
      impressions = denoisedValues.impressions
    }
    if (basicMetricSetSpec.grps) {
      grps = denoisedValues.grps
    }
  }
}

/**
 * Builds a union-only [ReportingSet.SetExpression] from a List of ExternalReportingSetIDs.
 *
 * @param externalReportingSetIds List of Strings representing ExternalReportingSetIDs.
 * @return [ReportingSet.SetExpression]
 */
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

  for (externalReportingSetId in externalReportingSetIds.subList(1, externalReportingSetIds.size)) {
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

private data class ResultGroupSpecKey(
  val metricFrequencySpec: MetricFrequencySpec,
  val eventFilters: List<EventFilter>,
  val groupingFields: List<String>,
)

private data class ReportingWindowResultKey(
  val groupings: List<EventTemplateField>,
  val windowStartDate: Date,
  val windowEndDate: Date,
  val externalImpressionQualificationFilterId: String? = null,
)

private data class ReportingWindowResultValues(
  var populationSize: Int,
  val reportResultValuesByExternalReportingSetId:
    MutableMap<String, ReportResult.ReportingSetResult.ReportingWindowResult.ReportResultValues>,
)

private data class ComponentReportingSetIds(
  val componentReportingSetId: String,
  val reportingUnitReportingSetId: String?,
  val reportingUnitWithoutComponentReportingSetId: String?,
)
