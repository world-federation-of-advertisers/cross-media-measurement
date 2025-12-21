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
import com.google.type.DateTime
import com.google.type.copy
import org.wfanet.measurement.common.toTimestamp
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.EventFilter
import org.wfanet.measurement.internal.reporting.v2.MetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.ReportResult
import org.wfanet.measurement.internal.reporting.v2.ReportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult
import org.wfanet.measurement.internal.reporting.v2.ResultGroup
import org.wfanet.measurement.internal.reporting.v2.ResultGroupKt
import org.wfanet.measurement.internal.reporting.v2.ResultGroupMetricSpec
import org.wfanet.measurement.internal.reporting.v2.ResultGroupSpec
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField
import org.wfanet.measurement.internal.reporting.v2.resultGroup

object BasicReportProcessedResultsTransformation {
  /**
   * Builds a list of [ResultGroup]s for the [BasicReport] given the [ReportResult]
   *
   * @param basicReport [BasicReport] to create [ResultGroup]s for
   * @param reportingSetResults [ReportingSetResult]s for a [ReportResult]
   * @param primitiveInfoByDataProviderId Map that contains [PrimitiveInfo] for each DataProviderId
   *   represented by the CampaignGroup [ReportingSet]
   * @param compositeReportingSetIdBySetExpression Map that contains ExternalReportingSetIDs for
   *   each [ReportingSet.SetExpression] represented by the CampaignGroup [ReportingSet]
   */
  fun buildResultGroups(
    basicReport: BasicReport,
    reportingSetResults: Iterable<ReportingSetResult>,
    primitiveInfoByDataProviderId: Map<String, PrimitiveInfo>,
    compositeReportingSetIdBySetExpression: Map<ReportingSet.SetExpression, String>,
  ): List<ResultGroup> {
    val reportingWindowResultValuesByResultGroupSpecKey:
      Map<ResultGroupSpecKey, Map<ReportingWindowResultKey, ReportingWindowResultValues>> =
      buildReportingWindowResultValuesByResultGroupSpecKey(reportingSetResults)

    // If there is no custom ReportingImpressionQualificationFilter, this will be unused so the
    // value
    // doesn't matter.
    var customReportingImpressionQualificationFilter: ReportingImpressionQualificationFilter =
      ReportingImpressionQualificationFilter.getDefaultInstance()

    val reportingImpressionQualificationFilterByExternalId:
      Map<String, ReportingImpressionQualificationFilter> =
      buildMap {
        val impressionQualificationFilters =
          basicReport.details.effectiveImpressionQualificationFiltersList

        for (impressionQualificationFilter in impressionQualificationFilters) {
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
        val reportingUnitSummary: ResultGroup.MetricMetadata.ReportingUnitSummary =
          buildReportingUnitSummary(
            basicReport,
            reportingUnitDataProviderIds,
            primitiveInfoByDataProviderId,
          )

        // If the value is blank, it wouldn't be used
        val reportingUnitReportingSetId: String =
          getReportingUnitReportingSetId(
            reportingUnitDataProviderIds,
            primitiveInfoByDataProviderId,
            compositeReportingSetIdBySetExpression,
            resultGroupSpec,
          )

        // ExternalReportingSetIDs for StackedIncrementalReach.
        val incrementalReportingSetIds: List<String> =
          buildIncrementalReportingSetIdList(
            reportingUnitDataProviderIds,
            primitiveInfoByDataProviderId,
            compositeReportingSetIdBySetExpression,
            resultGroupSpec,
          )

        val componentReportingSetIdsByDataProviderId: Map<String, ComponentReportingSetIds> =
          buildComponentReportingSetIdsByDataProviderIdMap(
            reportingUnitDataProviderIds,
            primitiveInfoByDataProviderId,
            compositeReportingSetIdBySetExpression,
            resultGroupSpec,
          )

        val resultGroup = resultGroup {
          title = resultGroupSpec.title
          results +=
            buildResults(
              reportStart = basicReport.details.reportingInterval.reportStart,
              resultGroupSpec = resultGroupSpec,
              reportStartTimestamp = reportStartTimestamp,
              reportingUnitSummary = reportingUnitSummary,
              reportingWindowResultValuesByResultGroupSpecKey =
                reportingWindowResultValuesByResultGroupSpecKey,
              reportingImpressionQualificationFilterByExternalId =
                reportingImpressionQualificationFilterByExternalId,
              customReportingImpressionQualificationFilter =
                customReportingImpressionQualificationFilter,
              reportingUnitDataProviderIds = reportingUnitDataProviderIds,
              componentReportingSetIdsByDataProviderId = componentReportingSetIdsByDataProviderId,
              incrementalReportingSetIds = incrementalReportingSetIds,
              reportingUnitReportingSetId = reportingUnitReportingSetId,
            )
        }

        add(resultGroup)
      }
    }
  }

  private fun buildReportingUnitSummary(
    basicReport: BasicReport,
    reportingUnitDataProviderIds: List<String>,
    primitiveInfoByDataProviderId: Map<String, PrimitiveInfo>,
  ): ResultGroup.MetricMetadata.ReportingUnitSummary {
    return ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
      for (dataProviderId in reportingUnitDataProviderIds) {
        reportingUnitComponentSummary +=
          ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
            cmmsDataProviderId = dataProviderId
            val primitiveInfo = primitiveInfoByDataProviderId.getValue(dataProviderId)
            for (eventGroupKey in primitiveInfo.eventGroupKeys) {
              eventGroupSummaries +=
                ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt.eventGroupSummary {
                  cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
                  cmmsEventGroupId = eventGroupKey.cmmsEventGroupId
                }
            }
          }
      }
    }
  }

  private fun getReportingUnitReportingSetId(
    reportingUnitDataProviderIds: List<String>,
    primitiveInfoByDataProviderId: Map<String, PrimitiveInfo>,
    compositeReportingSetIdBySetExpression: Map<ReportingSet.SetExpression, String>,
    resultGroupSpec: ResultGroupSpec,
  ): String {
    return if (reportingUnitDataProviderIds.size == 1) {
      primitiveInfoByDataProviderId
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
            primitiveInfoByDataProviderId.getValue(it).externalReportingSetId
          }
        )
      )
    } else {
      ""
    }
  }

  private fun buildIncrementalReportingSetIdList(
    reportingUnitDataProviderIds: List<String>,
    primitiveInfoByDataProviderId: Map<String, PrimitiveInfo>,
    compositeReportingSetIdBySetExpression: Map<ReportingSet.SetExpression, String>,
    resultGroupSpec: ResultGroupSpec,
  ): List<String> {
    return buildList {
      if (
        resultGroupSpec.resultGroupMetricSpec.hasReportingUnit() &&
          resultGroupSpec.resultGroupMetricSpec.reportingUnit.stackedIncrementalReach
      ) {
        val firstReportingSetId =
          primitiveInfoByDataProviderId
            .getValue(reportingUnitDataProviderIds.first())
            .externalReportingSetId

        add(firstReportingSetId)

        val primitiveReportingSetIds = mutableListOf<String>()
        primitiveReportingSetIds.add(firstReportingSetId)
        for (dataProviderId in
          reportingUnitDataProviderIds.subList(1, reportingUnitDataProviderIds.size)) {
          primitiveReportingSetIds.add(
            primitiveInfoByDataProviderId.getValue(dataProviderId).externalReportingSetId
          )
          val setExpression = buildUnionSetExpression(primitiveReportingSetIds)
          add(compositeReportingSetIdBySetExpression.getValue(setExpression))
        }
      }
    }
  }

  private fun buildComponentReportingSetIdsByDataProviderIdMap(
    reportingUnitDataProviderIds: List<String>,
    primitiveInfoByDataProviderId: Map<String, PrimitiveInfo>,
    compositeReportingSetIdBySetExpression: Map<ReportingSet.SetExpression, String>,
    resultGroupSpec: ResultGroupSpec,
  ): Map<String, ComponentReportingSetIds> {
    return buildMap {
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
                primitiveInfoByDataProviderId.getValue(it).externalReportingSetId
              }
            )
          )
        } else {
          null
        }

      for (dataProviderId in reportingUnitDataProviderIds) {
        val primitiveReportingSet = primitiveInfoByDataProviderId.getValue(dataProviderId)
        val componentReportingSetId = primitiveReportingSet.externalReportingSetId
        val allComponentsWithoutCurrentComponentReportingSetId =
          if (reportingUnitDataProviderIds.size == 2) {
            primitiveInfoByDataProviderId
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
                  .map { primitiveInfoByDataProviderId.getValue(it).externalReportingSetId }
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
  }

  /**
   * Builds a List of [ResultGroup.Result]s for a [ResultGroup]
   *
   * @param reportStart [DateTime] for the start of the report
   * @param resultGroupSpec [ResultGroupSpec] for the [ResultGroup]
   * @param reportStartTimestamp [Timestamp] for the start of the report
   * @param reportingUnitSummary [ResultGroup.MetricMetadata.ReportingUnitSummary]
   * @param reportingWindowResultValuesByResultGroupSpecKey
   * @param reportingImpressionQualificationFilterByExternalId
   * @param customReportingImpressionQualificationFilter
   * @param reportingUnitDataProviderIds
   * @param componentReportingSetIdsByDataProviderId
   * @param incrementalReportingSetIds List of Strings representing ExternalReportingSetIDs starting
   *   with the ID of the Primitive ReportingSet for the first component of the ReportingUnit, then
   *   the ID of the Composite ReportingSet for the first two components of the ReportingUnit, etc.,
   *   until the ID of the Composite ReportingSet for the entire ReportingUnit. Is empty if it isn't
   *   used.
   * @param reportingUnitReportingSetId String representing ExternalReportingSetID for the
   *   ReportingUnit. Is empty if it isn't used.
   */
  private fun buildResults(
    reportStart: DateTime,
    resultGroupSpec: ResultGroupSpec,
    reportStartTimestamp: Timestamp,
    reportingUnitSummary: ResultGroup.MetricMetadata.ReportingUnitSummary,
    reportingWindowResultValuesByResultGroupSpecKey:
      Map<ResultGroupSpecKey, Map<ReportingWindowResultKey, ReportingWindowResultValues>>,
    reportingImpressionQualificationFilterByExternalId:
      Map<String, ReportingImpressionQualificationFilter>,
    customReportingImpressionQualificationFilter: ReportingImpressionQualificationFilter,
    reportingUnitDataProviderIds: List<String>,
    componentReportingSetIdsByDataProviderId: Map<String, ComponentReportingSetIds>,
    incrementalReportingSetIds: List<String>,
    reportingUnitReportingSetId: String,
  ): List<ResultGroup.Result> {
    val reportingWindowResultValuesMap: Map<ReportingWindowResultKey, ReportingWindowResultValues> =
      reportingWindowResultValuesByResultGroupSpecKey.getValue(
        ResultGroupSpecKey(
          metricFrequencySpec = resultGroupSpec.metricFrequency,
          groupingFields = resultGroupSpec.dimensionSpec.grouping.eventTemplateFieldsList.toSet(),
          eventFilters = resultGroupSpec.dimensionSpec.filtersList.toSet(),
        )
      )

    return reportingWindowResultValuesMap.entries.map { reportingWindowResults ->
      ResultGroupKt.result {
        metadata =
          ResultGroupKt.metricMetadata {
            this.reportingUnitSummary = reportingUnitSummary

            val nonCumulativeWindowStartDate =
              reportingWindowResults.key.nonCumulativeWindowStartDate
            if (nonCumulativeWindowStartDate != null) {
              nonCumulativeMetricStartTime =
                reportStart
                  .copy {
                    day = nonCumulativeWindowStartDate.day
                    month = nonCumulativeWindowStartDate.month
                    year = nonCumulativeWindowStartDate.year
                  }
                  .toTimestamp()
            }

            cumulativeMetricStartTime = reportStartTimestamp

            metricEndTime =
              reportStart
                .copy {
                  day = reportingWindowResults.key.windowEndDate.day
                  month = reportingWindowResults.key.windowEndDate.month
                  year = reportingWindowResults.key.windowEndDate.year
                }
                .toTimestamp()

            metricFrequencySpec = resultGroupSpec.metricFrequency

            dimensionSpecSummary =
              ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                groupings +=
                  reportingWindowResults.key.grouping.valueByPathMap.toSortedMap().map {
                    eventTemplateField {
                      path = it.key
                      value = it.value
                    }
                  }
                filters += resultGroupSpec.dimensionSpec.filtersList
              }

            filter =
              if (reportingWindowResults.key.externalImpressionQualificationFilterId != null) {
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
                        reportingWindowResults.value.reportResultValuesByExternalReportingSetId,
                      )
                  }
              }
            }
          }
      }
    }
  }

  /** Builds a Map for finding all results for each [ResultGroupSpec] in the [BasicReport] */
  private fun buildReportingWindowResultValuesByResultGroupSpecKey(
    reportingSetResults: Iterable<ReportingSetResult>
  ): Map<ResultGroupSpecKey, Map<ReportingWindowResultKey, ReportingWindowResultValues>> {
    return buildMap<
      ResultGroupSpecKey,
      MutableMap<ReportingWindowResultKey, ReportingWindowResultValues>,
    > {
      for (reportingSetResult in reportingSetResults) {
        for (reportingWindowEntry in reportingSetResult.reportingWindowResultsList) {
          val externalImpressionQualificationFilterId: String? =
            if (reportingSetResult.dimension.custom) {
              null
            } else {
              reportingSetResult.dimension.externalImpressionQualificationFilterId
            }

          // This isn't unique to a ResultGroupSpec because it doesn't include the ReportingUnit,
          // but it is unique enough to get the results for every ResultGroupSpec that matches this.
          val resultGroupSpecKey =
            ResultGroupSpecKey(
              metricFrequencySpec = reportingSetResult.dimension.metricFrequencySpec,
              groupingFields = reportingSetResult.dimension.grouping.valueByPathMap.keys,
              eventFilters = reportingSetResult.dimension.eventFiltersList.toSet(),
            )

          val reportingWindowResultMap:
            MutableMap<ReportingWindowResultKey, ReportingWindowResultValues> =
            getOrPut(resultGroupSpecKey) { mutableMapOf() }

          val key =
            ReportingWindowResultKey(
              grouping = reportingSetResult.dimension.grouping,
              nonCumulativeWindowStartDate =
                if (
                  reportingSetResult.dimension.metricFrequencySpec.selectorCase ==
                    MetricFrequencySpec.SelectorCase.WEEKLY &&
                    reportingWindowEntry.key.hasNonCumulativeStart()
                ) {
                  reportingWindowEntry.key.nonCumulativeStart
                } else {
                  null
                },
              windowEndDate = reportingWindowEntry.key.end,
              externalImpressionQualificationFilterId = externalImpressionQualificationFilterId,
            )

          val value =
            ReportingWindowResultValues(
              populationSize = reportingSetResult.populationSize,
              reportResultValuesByExternalReportingSetId = mutableMapOf(),
            )

          val reportingWindowResultValues: ReportingWindowResultValues =
            reportingWindowResultMap.getOrPut(key) { value }

          reportingWindowResultValues.reportResultValuesByExternalReportingSetId[
              reportingSetResult.dimension.externalReportingSetId] =
            reportingWindowEntry.value.processedReportResultValues
        }
      }
    }
  }

  /**
   * Builds a [ResultGroup.MetricSet.ReportingUnitMetricSet] from a
   * [ResultGroupMetricSpec.ReportingUnitMetricSetSpec] and processed values
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
   * @param reportResultValuesByExternalReportingSetId Map containing processed values.
   */
  private fun buildReportingUnitMetricSet(
    reportingUnitMetricSetSpec: ResultGroupMetricSpec.ReportingUnitMetricSetSpec,
    reportingUnitReportingSetId: String,
    incrementalReportingSetIds: List<String>,
    reportResultValuesByExternalReportingSetId:
      Map<String, ReportingSetResult.ReportingWindowResult.ReportResultValues>,
  ): ResultGroup.MetricSet.ReportingUnitMetricSet {
    return ResultGroupKt.MetricSetKt.reportingUnitMetricSet {
      val reportingUnitProcessedResultValues =
        reportResultValuesByExternalReportingSetId.getValue(reportingUnitReportingSetId)

      if (reportingUnitMetricSetSpec.hasNonCumulative()) {
        nonCumulative =
          buildBasicMetricSet(
            reportingUnitMetricSetSpec.nonCumulative,
            reportingUnitProcessedResultValues.nonCumulativeResults,
          )
      }

      if (reportingUnitMetricSetSpec.hasCumulative()) {
        cumulative =
          buildBasicMetricSet(
            reportingUnitMetricSetSpec.cumulative,
            reportingUnitProcessedResultValues.cumulativeResults,
          )
      }

      if (reportingUnitMetricSetSpec.stackedIncrementalReach) {
        var prevReach = 0
        // Subtracts the previous reach to get the difference in reach.
        for (reportingSetId in incrementalReportingSetIds) {
          val processedValues = reportResultValuesByExternalReportingSetId.getValue(reportingSetId)
          stackedIncrementalReach += processedValues.cumulativeResults.reach - prevReach
          prevReach = processedValues.cumulativeResults.reach
        }
      }
    }
  }

  private fun buildComponentMetricSet(
    componentMetricSetSpec: ResultGroupMetricSpec.ComponentMetricSetSpec,
    componentReportingSetIds: ComponentReportingSetIds,
    reportResultValuesByExternalReportingSetId:
      Map<String, ReportingSetResult.ReportingWindowResult.ReportResultValues>,
  ): ResultGroup.MetricSet.ComponentMetricSet {
    val componentProcessedResultValues =
      reportResultValuesByExternalReportingSetId.getValue(
        componentReportingSetIds.componentReportingSetId
      )

    return ResultGroupKt.MetricSetKt.componentMetricSet {
      if (componentMetricSetSpec.hasNonCumulative()) {
        nonCumulative =
          buildBasicMetricSet(
            componentMetricSetSpec.nonCumulative,
            componentProcessedResultValues.nonCumulativeResults,
          )
      }

      if (componentMetricSetSpec.hasCumulative()) {
        cumulative =
          buildBasicMetricSet(
            componentMetricSetSpec.cumulative,
            componentProcessedResultValues.cumulativeResults,
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
   * @param processedValues [ResultGroup.MetricSet.BasicMetricSet] contains all the processed
   *   values, which may have fields set that the basicMetricSetSpec doesn't ask for.
   */
  private fun buildBasicMetricSet(
    basicMetricSetSpec: ResultGroupMetricSpec.BasicMetricSetSpec,
    processedValues: ResultGroup.MetricSet.BasicMetricSet,
  ): ResultGroup.MetricSet.BasicMetricSet {
    return ResultGroupKt.MetricSetKt.basicMetricSet {
      if (basicMetricSetSpec.reach) {
        reach = processedValues.reach
      }
      if (basicMetricSetSpec.percentReach) {
        percentReach = processedValues.percentReach
      }
      if (basicMetricSetSpec.kPlusReach > 0) {
        kPlusReach += processedValues.kPlusReachList.take(basicMetricSetSpec.kPlusReach)
      }
      if (basicMetricSetSpec.percentKPlusReach) {
        percentKPlusReach +=
          processedValues.percentKPlusReachList.take(basicMetricSetSpec.kPlusReach)
      }
      if (basicMetricSetSpec.averageFrequency) {
        averageFrequency = processedValues.averageFrequency
      }
      if (basicMetricSetSpec.impressions) {
        impressions = processedValues.impressions
      }
      if (basicMetricSetSpec.grps) {
        grps = processedValues.grps
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

  private data class ResultGroupSpecKey(
    val metricFrequencySpec: MetricFrequencySpec,
    val eventFilters: Set<EventFilter>,
    val groupingFields: Set<String>,
  )

  private data class ReportingWindowResultKey(
    val grouping: ReportingSetResult.Dimension.Grouping,
    val nonCumulativeWindowStartDate: Date?,
    val windowEndDate: Date,
    val externalImpressionQualificationFilterId: String? = null,
  )

  private data class ReportingWindowResultValues(
    val populationSize: Int,
    val reportResultValuesByExternalReportingSetId:
      MutableMap<String, ReportingSetResult.ReportingWindowResult.ReportResultValues>,
  )

  private data class ComponentReportingSetIds(
    val componentReportingSetId: String,
    val reportingUnitReportingSetId: String?,
    val reportingUnitWithoutComponentReportingSetId: String?,
  )

  data class PrimitiveInfo(
    val eventGroupKeys: Set<ReportingSet.Primitive.EventGroupKey>,
    val externalReportingSetId: String,
  )
}
