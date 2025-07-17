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

import com.google.type.DayOfWeek
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.reporting.v2alpha.EventTemplateField
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.MetricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ReportingUnit
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.ResultGroupSpec
import org.wfanet.measurement.reporting.v2alpha.reportingSet
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpec.ReportingUnitMetricSetSpec
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpec.ComponentMetricSetSpec

private data class MetricCalculationSpecBuilderKey(
  val filter: String,
  val groupings: List<MetricCalculationSpec.Grouping>,
  val metricFrequencySpec: MetricCalculationSpec.MetricFrequencySpec? = null,
  val trailingWindow: MetricCalculationSpec.TrailingWindow? = null,
)

private data class MetricCalculationSpecBuilder(
  var hasFrequency: Boolean = false,
  var hasReach: Boolean = false,
  var hasImpressionCount: Boolean = false,
)

/**
 * Transforms a List of [ResultGroupSpec] into a Map for building [Report.ReportingMetricEntry]s.
 * This assumes that all parameters have already been validated.
 *
 * @param campaignGroupName resource name of [ReportingSet] that is a campaign group
 * @param impressionQualificationFilterSpecsFilters List of CEL filters each representing an
 *   [ImpressionQualificationFilter]
 * @param dataProviderPrimitiveReportingSetMap Map of [DataProvider] resource name to primitive
 *   [ReportingSet] containing associated [EventGroup] resource names
 * @param resultGroupSpecs List of [ResultGroupSpec] to transform
 * @return Map of [ReportingSet] to [MetricCalculationSpec.Details]
 */
fun buildReportingSetMetricCalculationSpecDetailsMap(
  campaignGroupName: String,
  impressionQualificationFilterSpecsFilters: List<String>,
  dataProviderPrimitiveReportingSetMap: Map<String, ReportingSet>,
  resultGroupSpecs: List<ResultGroupSpec>,
): Map<ReportingSet, List<MetricCalculationSpec.Details>> {
  val reportingSetMetricCalculationSpecBuilderMap:
    Map<ReportingSet, MutableMap<MetricCalculationSpecBuilderKey, MetricCalculationSpecBuilder>> =
    buildMap {
      for (resultGroupSpec in resultGroupSpecs) {
        // TODO(tristanvuong2021): create groupings from dimension_spec
        val groupings = emptyList<MetricCalculationSpec.Grouping>()
        val dimensionSpecFilter =
          resultGroupSpec.dimensionSpec.filtersList.joinToString(
            prefix = "(",
            postfix = ")",
            separator = " && ",
          ) {
            val term = it.termsList.first()
            val termValue =
              @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
              when (term.value.selectorCase) {
                EventTemplateField.FieldValue.SelectorCase.STRING_VALUE -> term.value.stringValue
                EventTemplateField.FieldValue.SelectorCase.ENUM_VALUE -> term.value.enumValue
                EventTemplateField.FieldValue.SelectorCase.FLOAT_VALUE -> term.value.floatValue
                EventTemplateField.FieldValue.SelectorCase.BOOL_VALUE -> term.value.boolValue
                EventTemplateField.FieldValue.SelectorCase.SELECTOR_NOT_SET ->
                  IllegalArgumentException("Selector not set")
              }
            "${term.path} == $termValue"
          }

        val metricCalculationSpecFilters = buildList {
          for (impressionQualificationSpecsFilter in impressionQualificationFilterSpecsFilters) {
            add("$impressionQualificationSpecsFilter && $dimensionSpecFilter")
          }
        }

        val primitiveReportingSets: List<ReportingSet> =
          resultGroupSpec.reportingUnit.componentsList.map {
            dataProviderPrimitiveReportingSetMap.getValue(it)
          }

        val primitiveReportingSetNames: List<String> = primitiveReportingSets.map { it.name }

        for (filter in metricCalculationSpecFilters) {
          val reportingUnitReportingSet =
            if (primitiveReportingSets.size == 1) {
              primitiveReportingSets.first()
            } else {
              buildUnionCompositeReportingSet(campaignGroupName, primitiveReportingSetNames)
            }

          if (resultGroupSpec.resultGroupMetricSpec.hasReportingUnit()) {
            computeReportingUnitMetricSetSpecTransformation(
              reportingUnitReportingSet,
              resultGroupSpec.resultGroupMetricSpec.reportingUnit,
              resultGroupSpec.reportingUnit,
              resultGroupSpec.metricFrequency,
              groupings,
              filter,
              dataProviderPrimitiveReportingSetMap,
              primitiveReportingSetNames,
              campaignGroupName,
            )
          }

          if (resultGroupSpec.resultGroupMetricSpec.hasComponent()) {
            computeComponentMetricSetSpecTransformation(
              reportingUnitReportingSet,
              resultGroupSpec.resultGroupMetricSpec.component,
              resultGroupSpec.metricFrequency,
              groupings,
              filter,
              primitiveReportingSets,
              primitiveReportingSetNames,
              campaignGroupName,
            )
          }
        }
      }
    }

  return reportingSetMetricCalculationSpecBuilderMap.entries
    .filter { it.value.isNotEmpty() }
    .associate { entry ->
      entry.key to
        entry.value.entries.map {
          MetricCalculationSpecKt.details {
            groupings += it.key.groupings
            filter = it.key.filter
            if (it.key.metricFrequencySpec != null) {
              metricFrequencySpec = it.key.metricFrequencySpec!!
            }
            if (it.key.trailingWindow != null) {
              trailingWindow = it.key.trailingWindow!!
            }

            // TODO(tristanvuong2021): Add privacy params
            if (it.value.hasFrequency) {
              metricSpecs += metricSpec {
                reachAndFrequency = MetricSpecKt.reachAndFrequencyParams {}
              }
            } else if (it.value.hasReach) {
              metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
            }

            if (it.value.hasImpressionCount) {
              metricSpecs += metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
            }
          }
        }
    }
}

/**
 * Helper method for [buildReportingSetMetricCalculationSpecDetailsMap]. Transforms [ReportingUnitMetricSetSpec]
 */
private fun MutableMap<ReportingSet, MutableMap<MetricCalculationSpecBuilderKey, MetricCalculationSpecBuilder>>.computeReportingUnitMetricSetSpecTransformation(
  reportingUnitReportingSet: ReportingSet,
  reportingUnitMetricSetSpec: ReportingUnitMetricSetSpec,
  reportingUnit: ReportingUnit,
  metricFrequencySpec: MetricFrequencySpec,
  groupings: List<MetricCalculationSpec.Grouping>,
  filter: String,
  dataProviderPrimitiveReportingSetMap: Map<String, ReportingSet>,
  primitiveReportingSetNames: List<String>,
  campaignGroupName: String,
) {
  val metricCalculationSpecBuilderMap =
    computeIfAbsent(reportingUnitReportingSet) { mutableMapOf() }

  if (reportingUnitMetricSetSpec.hasNonCumulative()) {
    val key =
      createMetricCalculationSpecBuilderKey(
        filter,
        groupings,
        false,
        metricFrequencySpec,
      )
    metricCalculationSpecBuilderMap
      .computeIfAbsent(key) { MetricCalculationSpecBuilder() }
      .updateRequestedMetricSpecs(
        reportingUnitMetricSetSpec.nonCumulative
      )
  }

  if (reportingUnitMetricSetSpec.hasCumulative()) {
    val key =
      createMetricCalculationSpecBuilderKey(
        filter,
        groupings,
        true,
        metricFrequencySpec,
      )
    metricCalculationSpecBuilderMap
      .computeIfAbsent(key) { MetricCalculationSpecBuilder() }
      .updateRequestedMetricSpecs(
        reportingUnitMetricSetSpec.cumulative
      )
  }

  if (reportingUnitMetricSetSpec.stackedIncrementalReach) {
    val firstMetricCalculationSpecBuilderMap =
      computeIfAbsent(
        dataProviderPrimitiveReportingSetMap.getValue(
          reportingUnit.componentsList.first()
        )
      ) {
        mutableMapOf()
      }
    val firstKey =
      createMetricCalculationSpecBuilderKey(
        filter,
        groupings,
        true,
        metricFrequencySpec,
      )
    firstMetricCalculationSpecBuilderMap
      .computeIfAbsent(firstKey) { MetricCalculationSpecBuilder() }
      .hasReach = true

    if (primitiveReportingSetNames.size >= 2) {
      for (i in 2..primitiveReportingSetNames.size) {
        val partialList = primitiveReportingSetNames.subList(0, i)

        val partialReportingUnitCompositeReportingSet =
          buildUnionCompositeReportingSet(campaignGroupName, partialList)

        val partialMetricCalculationSpecBuilderMap =
          computeIfAbsent(partialReportingUnitCompositeReportingSet) { mutableMapOf() }
        val key =
          createMetricCalculationSpecBuilderKey(
            filter,
            groupings,
            true,
            metricFrequencySpec,
          )
        partialMetricCalculationSpecBuilderMap
          .computeIfAbsent(key) { MetricCalculationSpecBuilder() }
          .hasReach = true
      }
    }
  }
}

/**
 * Helper method for [buildReportingSetMetricCalculationSpecDetailsMap]. Transforms [ComponentMetricSetSpec]
 */
private fun MutableMap<ReportingSet, MutableMap<MetricCalculationSpecBuilderKey, MetricCalculationSpecBuilder>>.computeComponentMetricSetSpecTransformation(
  reportingUnitReportingSet: ReportingSet,
  componentMetricSetSpec: ComponentMetricSetSpec,
  metricFrequencySpec: MetricFrequencySpec,
  groupings: List<MetricCalculationSpec.Grouping>,
  filter: String,
  primitiveReportingSets: List<ReportingSet>,
  primitiveReportingSetNames: List<String>,
  campaignGroupName: String,
) {
  for (primitiveReportingSet in primitiveReportingSets) {
    val metricCalculationSpecBuilderMap =
      computeIfAbsent(primitiveReportingSet) { mutableMapOf() }

    if (componentMetricSetSpec.hasNonCumulative()) {
      val key =
        createMetricCalculationSpecBuilderKey(
          filter,
          groupings,
          false,
          metricFrequencySpec,
        )
      metricCalculationSpecBuilderMap
        .computeIfAbsent(key) { MetricCalculationSpecBuilder() }
        .updateRequestedMetricSpecs(
          componentMetricSetSpec.nonCumulative
        )
    }

    if (componentMetricSetSpec.hasCumulative()) {
      val key =
        createMetricCalculationSpecBuilderKey(
          filter,
          groupings,
          true,
          metricFrequencySpec,
        )
      metricCalculationSpecBuilderMap
        .computeIfAbsent(key) { MetricCalculationSpecBuilder() }
        .updateRequestedMetricSpecs(
          componentMetricSetSpec.cumulative
        )
    }
  }

  if (
    componentMetricSetSpec.hasNonCumulativeUnique() ||
    componentMetricSetSpec.hasCumulativeUnique()
  ) {
    val firstMetricCalculationSpecBuilderMap =
      computeIfAbsent(reportingUnitReportingSet) { mutableMapOf() }

    if (componentMetricSetSpec.hasNonCumulativeUnique()) {
      val firstKey =
        createMetricCalculationSpecBuilderKey(
          filter,
          groupings,
          false,
          metricFrequencySpec,
        )
      firstMetricCalculationSpecBuilderMap
        .computeIfAbsent(firstKey) { MetricCalculationSpecBuilder() }
        .updateRequestedMetricSpecs(
          componentMetricSetSpec.nonCumulativeUnique
        )
    }

    if (componentMetricSetSpec.hasCumulativeUnique()) {
      val firstKey =
        createMetricCalculationSpecBuilderKey(
          filter,
          groupings,
          true,
          metricFrequencySpec,
        )
      firstMetricCalculationSpecBuilderMap
        .computeIfAbsent(firstKey) { MetricCalculationSpecBuilder() }
        .updateRequestedMetricSpecs(
          componentMetricSetSpec.cumulativeUnique
        )
    }

    // Less than 3 means only primitive reporting sets are needed. There is no point in
    // creating a composite reporting set with just 1 primitive reporting set
    if (primitiveReportingSetNames.size >= 3) {
      for (i in primitiveReportingSetNames.indices) {
        val partialList =
          primitiveReportingSetNames.filterIndexed { index, _ -> index != i }

        val partialReportingUnitCompositeReportingSet =
          buildUnionCompositeReportingSet(campaignGroupName, partialList)

        val partialMetricCalculationSpecBuilderMap =
          computeIfAbsent(partialReportingUnitCompositeReportingSet) { mutableMapOf() }

        if (componentMetricSetSpec.hasNonCumulativeUnique()) {
          val key =
            createMetricCalculationSpecBuilderKey(
              filter,
              groupings,
              false,
              metricFrequencySpec,
            )
          partialMetricCalculationSpecBuilderMap
            .computeIfAbsent(key) { MetricCalculationSpecBuilder() }
            .updateRequestedMetricSpecs(
              componentMetricSetSpec.nonCumulativeUnique
            )
        }

        if (componentMetricSetSpec.hasCumulativeUnique()) {
          val key =
            createMetricCalculationSpecBuilderKey(
              filter,
              groupings,
              true,
              metricFrequencySpec,
            )
          partialMetricCalculationSpecBuilderMap
            .computeIfAbsent(key) { MetricCalculationSpecBuilder() }
            .updateRequestedMetricSpecs(
              componentMetricSetSpec.cumulativeUnique
            )
        }
      }
    } else {
      for (primitiveReportingSet in primitiveReportingSets) {
        val metricCalculationSpecBuilderMap =
          computeIfAbsent(primitiveReportingSet) { mutableMapOf() }

        if (componentMetricSetSpec.hasNonCumulativeUnique()) {
          val key =
            createMetricCalculationSpecBuilderKey(
              filter,
              groupings,
              false,
              metricFrequencySpec,
            )
          metricCalculationSpecBuilderMap
            .computeIfAbsent(key) { MetricCalculationSpecBuilder() }
            .updateRequestedMetricSpecs(
              componentMetricSetSpec.nonCumulativeUnique
            )
        }

        if (componentMetricSetSpec.hasCumulativeUnique()) {
          val key =
            createMetricCalculationSpecBuilderKey(
              filter,
              groupings,
              true,
              metricFrequencySpec,
            )
          metricCalculationSpecBuilderMap
            .computeIfAbsent(key) { MetricCalculationSpecBuilder() }
            .updateRequestedMetricSpecs(
              componentMetricSetSpec.cumulativeUnique
            )
        }
      }
    }
  }
}

/**
 * Set which [MetricSpec]s will be needed
 *
 * @param basicMetricSetSpec [ResultGroupMetricSpec.BasicMetricSetSpec]
 */
private fun MetricCalculationSpecBuilder.updateRequestedMetricSpecs(
  basicMetricSetSpec: ResultGroupMetricSpec.BasicMetricSetSpec
) {
  if (basicMetricSetSpec.averageFrequency || basicMetricSetSpec.kPlusReach > 0) {
    this.hasFrequency = true
    this.hasReach = true
  } else if (basicMetricSetSpec.reach || basicMetricSetSpec.percentReach) {
    this.hasReach = true
  }

  if (basicMetricSetSpec.impressions || basicMetricSetSpec.grps) {
    this.hasImpressionCount = true
  }
}

/**
 * Set which [MetricSpec]s will be needed
 *
 * @param uniqueMetricSetSpec [ResultGroupMetricSpec.UniqueMetricSetSpec]
 */
private fun MetricCalculationSpecBuilder.updateRequestedMetricSpecs(
  uniqueMetricSetSpec: ResultGroupMetricSpec.UniqueMetricSetSpec
) {
  if (uniqueMetricSetSpec.reach) {
    this.hasReach = true
  }
}

/**
 * Create an object for using as a key in a map
 *
 * @param metricCalculationSpecFilter filter for [MetricCalculationSpec]
 * @param groupings list of [MetricCalculationSpec.Grouping]
 * @param cumulative determines whether [MetricCalculationSpec.TrailingWindow] is set
 * @param dayOfWeek [DayOfWeek] if applicable. Determines whether
 *   [MetricCalculationSpec.MetricFrequencySpec] is set
 * @return [MetricCalculationSpecBuilderKey] for use as a key in a map
 */
private fun createMetricCalculationSpecBuilderKey(
  metricCalculationSpecFilter: String,
  groupings: List<MetricCalculationSpec.Grouping>,
  cumulative: Boolean,
  basicReportMetricFrequencySpec: MetricFrequencySpec,
): MetricCalculationSpecBuilderKey {
  val metricFrequencySpec =
    if (basicReportMetricFrequencySpec.hasWeekly()) {
      MetricCalculationSpecKt.metricFrequencySpec {
        weekly =
          MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
            dayOfWeek = basicReportMetricFrequencySpec.weekly
          }
      }
    } else {
      null
    }

  val trailingWindow =
    if (!cumulative) {
      MetricCalculationSpecKt.trailingWindow {
        count = 1
        increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
      }
    } else {
      null
    }

  return MetricCalculationSpecBuilderKey(
    filter = metricCalculationSpecFilter,
    groupings = groupings,
    metricFrequencySpec = metricFrequencySpec,
    trailingWindow = trailingWindow,
  )
}

/**
 * Builds a Composite [ReportingSet] that is a union of the given ReportingSets.
 *
 * @param campaignGroupName Resource name of Campaign Group
 * @param reportingSetNames List of resource names of [ReportingSet]s to union
 * @return Composite [ReportingSet] containing the union of reportingSetNames
 */
fun buildUnionCompositeReportingSet(
  campaignGroupName: String,
  reportingSetNames: List<String>,
): ReportingSet {
  var setExpression =
    ReportingSetKt.setExpression {
      operation = ReportingSet.SetExpression.Operation.UNION
      lhs = ReportingSetKt.SetExpressionKt.operand { reportingSet = reportingSetNames.first() }
    }

  for (reportingSetName in reportingSetNames.subList(1, reportingSetNames.size)) {
    setExpression =
      ReportingSetKt.setExpression {
        operation = ReportingSet.SetExpression.Operation.UNION
        lhs = ReportingSetKt.SetExpressionKt.operand { reportingSet = reportingSetName }
        rhs = ReportingSetKt.SetExpressionKt.operand { expression = setExpression }
      }
  }

  return reportingSet {
    campaignGroup = campaignGroupName
    composite = ReportingSetKt.composite { expression = setExpression }
  }
}
