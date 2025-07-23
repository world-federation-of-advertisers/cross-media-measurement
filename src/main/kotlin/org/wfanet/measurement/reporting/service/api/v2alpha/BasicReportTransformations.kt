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
import org.wfanet.measurement.internal.reporting.v2.metricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.reporting.v2alpha.DimensionSpec
import org.wfanet.measurement.reporting.v2alpha.EventFilter
import org.wfanet.measurement.reporting.v2alpha.EventTemplateField
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.MetricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ReportingUnit
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpec.ComponentMetricSetSpec
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpec.ReportingUnitMetricSetSpec
import org.wfanet.measurement.reporting.v2alpha.ResultGroupSpec
import org.wfanet.measurement.reporting.v2alpha.reportingSet

/** [MetricCalculationSpec] fields for equality check */
private data class MetricCalculationSpecInfoKey(
  val filter: String,
  val groupings: List<MetricCalculationSpec.Grouping>,
  val metricFrequencySpec: MetricCalculationSpec.MetricFrequencySpec? = null,
  val trailingWindow: MetricCalculationSpec.TrailingWindow? = null,
)

/** [MetricCalculationSpec] fields not used for equality check */
private data class MetricCalculationSpecInfo(
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
 * @return Map of [ReportingSet] to [MetricCalculationSpec]
 */
fun buildReportingSetMetricCalculationSpecDetailsMap(
  campaignGroupName: String,
  impressionQualificationFilterSpecsFilters: List<String>,
  dataProviderPrimitiveReportingSetMap: Map<String, ReportingSet>,
  resultGroupSpecs: List<ResultGroupSpec>,
): Map<ReportingSet, List<MetricCalculationSpec>> {
  // This intermediate map is for reducing the number of MetricCalculationSpecs created. Without
  // this map, MetricCalculationSpecs with everything identical except for MetricSpecs can be
  // created. If the MetricSpecs have some overlap, that will result in some Metrics being
  // calculated multiple times.
  val reportingSetMetricCalculationSpecInfoMap:
    Map<ReportingSet, MutableMap<MetricCalculationSpecInfoKey, MetricCalculationSpecInfo>> =
    buildMap {
      for (resultGroupSpec in resultGroupSpecs) {
        // TODO(tristanvuong2021): create groupings from dimension_spec
        val groupings = emptyList<MetricCalculationSpec.Grouping>()

        // List of filters to be used in creating the MetricCalculationSpecs given the
        // DimensionSpec
        val metricCalculationSpecFilters: List<String> =
          createMetricCalculationSpecFilters(
            impressionQualificationFilterSpecsFilters,
            resultGroupSpec.dimensionSpec.filtersList,
          )

        // The Primitive ReportingSets for the ReportingUnit
        val primitiveReportingSets: List<ReportingSet> =
          resultGroupSpec.reportingUnit.componentsList.map {
            dataProviderPrimitiveReportingSetMap.getValue(it)
          }

        // Composite ReportingSets are made from Primitive ReportingSet names
        val primitiveReportingSetNames: List<String> = primitiveReportingSets.map { it.name }

        // Adds or updates entries in the map
        computeResultGroupSpecTransformation(
          primitiveReportingSets,
          campaignGroupName,
          primitiveReportingSetNames,
          resultGroupSpec,
          groupings,
          metricCalculationSpecFilters,
        )
      }
    }

  val cmmsMeasurementConsumerId =
    ReportingSetKey.fromName(campaignGroupName)!!.cmmsMeasurementConsumerId

  return reportingSetMetricCalculationSpecInfoMap.entries
    .filter { it.value.isNotEmpty() }
    .associate { entry ->
      entry.key to entry.value.entries.map { it.toMetricCalculationSpec(cmmsMeasurementConsumerId) }
    }
}

/**
 * Creates a [MetricCalculationSpec] from the given entry in the map
 *
 * @param cmmsMeasurementConsumerId For setting the cmmsMeasurementConsumerId
 */
private fun MutableMap.MutableEntry<MetricCalculationSpecInfoKey, MetricCalculationSpecInfo>
  .toMetricCalculationSpec(cmmsMeasurementConsumerId: String): MetricCalculationSpec {
  val source = this
  return metricCalculationSpec {
    this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
    details =
      MetricCalculationSpecKt.details {
        groupings += source.key.groupings
        filter = source.key.filter
        if (source.key.metricFrequencySpec != null) {
          metricFrequencySpec = source.key.metricFrequencySpec!!
        }
        if (source.key.trailingWindow != null) {
          trailingWindow = source.key.trailingWindow!!
        }

        // TODO(tristanvuong2021): Add privacy params
        if (source.value.hasFrequency) {
          metricSpecs += metricSpec { reachAndFrequency = MetricSpecKt.reachAndFrequencyParams {} }
        } else if (source.value.hasReach) {
          metricSpecs += metricSpec { reach = MetricSpecKt.reachParams {} }
        }

        if (source.value.hasImpressionCount) {
          metricSpecs += metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
        }
      }
  }
}

/**
 * Creates a List of CEL strings
 *
 * @param impressionQualificationFilterSpecsFilters List of CEL strings created from
 *   [ReportingImpressionQualificationFilter]s
 * @param dimensionSpecFilters List of [EventFilter]s from [DimensionSpec]
 */
private fun createMetricCalculationSpecFilters(
  impressionQualificationFilterSpecsFilters: List<String>,
  dimensionSpecFilters: List<EventFilter>,
): List<String> {
  val dimensionSpecFilter =
    dimensionSpecFilters.joinToString(prefix = "(", postfix = ")", separator = " && ") {
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

  return buildList {
    for (impressionQualificationSpecsFilter in impressionQualificationFilterSpecsFilters) {
      add("$impressionQualificationSpecsFilter && $dimensionSpecFilter")
    }
  }
}

/**
 * Transforms each field of the [ResultGroupMetricSpec] in the [ResultGroupSpec]
 *
 * @param primitiveReportingSets List of Primitive [ReportingSet]s in order of [ReportingUnit]
 *   components
 * @param campaignGroupName Resource name of the CampaignGrouop [ReportingSet]
 * @param primitiveReportingSetNames Resource names of the Primitive [ReportingSet]s
 * @param resultGroupSpec [ResultGroupSpec] to transform
 * @param groupings: List of [MetricCalculationSpec.Grouping] to use
 * @param metricCalculationSpecFilters: List of CEL filters to use
 */
private fun MutableMap<
  ReportingSet,
  MutableMap<MetricCalculationSpecInfoKey, MetricCalculationSpecInfo>,
>
  .computeResultGroupSpecTransformation(
  primitiveReportingSets: List<ReportingSet>,
  campaignGroupName: String,
  primitiveReportingSetNames: List<String>,
  resultGroupSpec: ResultGroupSpec,
  groupings: List<MetricCalculationSpec.Grouping>,
  metricCalculationSpecFilters: List<String>,
) {
  // ReportingSet that represents the entire ReportingUnit
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
      resultGroupSpec.metricFrequency,
      groupings,
      metricCalculationSpecFilters,
      primitiveReportingSets.first(),
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
      metricCalculationSpecFilters,
      primitiveReportingSets,
      primitiveReportingSetNames,
      campaignGroupName,
    )
  }
}

/**
 * Helper method for [buildReportingSetMetricCalculationSpecDetailsMap]. Transforms
 * [ReportingUnitMetricSetSpec] into entries for the map
 */
private fun MutableMap<
  ReportingSet,
  MutableMap<MetricCalculationSpecInfoKey, MetricCalculationSpecInfo>,
>
  .computeReportingUnitMetricSetSpecTransformation(
  reportingUnitReportingSet: ReportingSet,
  reportingUnitMetricSetSpec: ReportingUnitMetricSetSpec,
  metricFrequencySpec: MetricFrequencySpec,
  groupings: List<MetricCalculationSpec.Grouping>,
  filters: List<String>,
  firstReportingSet: ReportingSet,
  primitiveReportingSetNames: List<String>,
  campaignGroupName: String,
) {
  val metricCalculationSpecInfoMap = computeIfAbsent(reportingUnitReportingSet) { mutableMapOf() }

  if (reportingUnitMetricSetSpec.hasNonCumulative()) {
    for (filter in filters) {
      val key = createMetricCalculationSpecInfoKey(filter, groupings, false, metricFrequencySpec)

      // Insert or update entry in map belonging to ReportingSet containing ReportingUnit
      metricCalculationSpecInfoMap
        .computeIfAbsent(key) { MetricCalculationSpecInfo() }
        .updateRequestedMetricSpecs(reportingUnitMetricSetSpec.nonCumulative)
    }
  }

  if (reportingUnitMetricSetSpec.hasCumulative()) {
    for (filter in filters) {
      val key = createMetricCalculationSpecInfoKey(filter, groupings, true, metricFrequencySpec)

      // Insert or update entry in map belonging to ReportingSet containing ReportingUnit
      metricCalculationSpecInfoMap
        .computeIfAbsent(key) { MetricCalculationSpecInfo() }
        .updateRequestedMetricSpecs(reportingUnitMetricSetSpec.cumulative)
    }
  }

  // Builds ReportingSets incrementally. Starting with the first ReportingSet including just the
  // first component, which has already been created as part of the set of Primitive ReportingSets.
  // Then the second ReportingSet including the first two components, and so on.
  if (reportingUnitMetricSetSpec.stackedIncrementalReach) {
    // First ReportingSet
    val firstMetricCalculationSpecInfoMap = computeIfAbsent(firstReportingSet) { mutableMapOf() }

    for (filter in filters) {
      val firstKey =
        createMetricCalculationSpecInfoKey(filter, groupings, true, metricFrequencySpec)

      // Insert or update entry in map belonging to first ReportingSet
      firstMetricCalculationSpecInfoMap
        .computeIfAbsent(firstKey) { MetricCalculationSpecInfo() }
        .hasReach = true
    }

    // Second ReportingSet and so on if there are at least two components.
    if (primitiveReportingSetNames.size >= 2) {
      for (i in 2..primitiveReportingSetNames.size) {
        val partialList = primitiveReportingSetNames.subList(0, i)

        val partialReportingUnitCompositeReportingSet =
          buildUnionCompositeReportingSet(campaignGroupName, partialList)

        val partialMetricCalculationSpecInfoMap =
          computeIfAbsent(partialReportingUnitCompositeReportingSet) { mutableMapOf() }

        for (filter in filters) {
          val key = createMetricCalculationSpecInfoKey(filter, groupings, true, metricFrequencySpec)

          // Insert or update entry in map belonging to subsequent ReportingSets
          partialMetricCalculationSpecInfoMap
            .computeIfAbsent(key) { MetricCalculationSpecInfo() }
            .hasReach = true
        }
      }
    }
  }
}

/**
 * Helper method for [buildReportingSetMetricCalculationSpecDetailsMap]. Transforms
 * [ComponentMetricSetSpec] into entries for the map
 */
private fun MutableMap<
  ReportingSet,
  MutableMap<MetricCalculationSpecInfoKey, MetricCalculationSpecInfo>,
>
  .computeComponentMetricSetSpecTransformation(
  reportingUnitReportingSet: ReportingSet,
  componentMetricSetSpec: ComponentMetricSetSpec,
  metricFrequencySpec: MetricFrequencySpec,
  groupings: List<MetricCalculationSpec.Grouping>,
  filters: List<String>,
  primitiveReportingSets: List<ReportingSet>,
  primitiveReportingSetNames: List<String>,
  campaignGroupName: String,
) {
  for (primitiveReportingSet in primitiveReportingSets) {
    val metricCalculationSpecInfoMap = computeIfAbsent(primitiveReportingSet) { mutableMapOf() }

    if (componentMetricSetSpec.hasNonCumulative()) {
      for (filter in filters) {
        val key = createMetricCalculationSpecInfoKey(filter, groupings, false, metricFrequencySpec)

        // Insert or update entry in map belonging to Primitive ReportingSet
        metricCalculationSpecInfoMap
          .computeIfAbsent(key) { MetricCalculationSpecInfo() }
          .updateRequestedMetricSpecs(componentMetricSetSpec.nonCumulative)
      }
    }

    if (componentMetricSetSpec.hasCumulative()) {
      for (filter in filters) {
        val key = createMetricCalculationSpecInfoKey(filter, groupings, true, metricFrequencySpec)

        // Insert or update entry in map belonging to Primitive ReportingSet
        metricCalculationSpecInfoMap
          .computeIfAbsent(key) { MetricCalculationSpecInfo() }
          .updateRequestedMetricSpecs(componentMetricSetSpec.cumulative)
      }
    }
  }

  if (
    componentMetricSetSpec.hasNonCumulativeUnique() || componentMetricSetSpec.hasCumulativeUnique()
  ) {
    val firstMetricCalculationSpecInfoMap =
      computeIfAbsent(reportingUnitReportingSet) { mutableMapOf() }

    if (componentMetricSetSpec.hasNonCumulativeUnique()) {
      for (filter in filters) {
        val firstKey =
          createMetricCalculationSpecInfoKey(filter, groupings, false, metricFrequencySpec)

        // Insert or update entry in map belonging to ReportingSet containing ReportingUnit
        firstMetricCalculationSpecInfoMap
          .computeIfAbsent(firstKey) { MetricCalculationSpecInfo() }
          .updateRequestedMetricSpecs(componentMetricSetSpec.nonCumulativeUnique)
      }
    }

    if (componentMetricSetSpec.hasCumulativeUnique()) {
      for (filter in filters) {
        val firstKey =
          createMetricCalculationSpecInfoKey(filter, groupings, true, metricFrequencySpec)

        // Insert or update entry in map belonging to ReportingSet containing ReportingUnit
        firstMetricCalculationSpecInfoMap
          .computeIfAbsent(firstKey) { MetricCalculationSpecInfo() }
          .updateRequestedMetricSpecs(componentMetricSetSpec.cumulativeUnique)
      }
    }

    // Less than 3 means only primitive reporting sets are needed. The ReportingSet containing the
    // ReportingUnit has already been created so subtracting 1 component from 2 components results
    // in 1 component, which is already represented by the Primitive ReportingSets.
    if (primitiveReportingSetNames.size >= 3) {
      for (i in primitiveReportingSetNames.indices) {
        val partialList = primitiveReportingSetNames.filterIndexed { index, _ -> index != i }

        val partialReportingUnitCompositeReportingSet =
          buildUnionCompositeReportingSet(campaignGroupName, partialList)

        val partialMetricCalculationSpecInfoMap =
          computeIfAbsent(partialReportingUnitCompositeReportingSet) { mutableMapOf() }

        if (componentMetricSetSpec.hasNonCumulativeUnique()) {
          for (filter in filters) {
            val key =
              createMetricCalculationSpecInfoKey(filter, groupings, false, metricFrequencySpec)

            // Insert or update entry in map belonging to ReportingSet containing ReportingUnit
            // minus one component
            partialMetricCalculationSpecInfoMap
              .computeIfAbsent(key) { MetricCalculationSpecInfo() }
              .updateRequestedMetricSpecs(componentMetricSetSpec.nonCumulativeUnique)
          }
        }

        if (componentMetricSetSpec.hasCumulativeUnique()) {
          for (filter in filters) {
            val key =
              createMetricCalculationSpecInfoKey(filter, groupings, true, metricFrequencySpec)

            // Insert or update entry in map belonging to ReportingSet containing ReportingUnit
            // minus one component
            partialMetricCalculationSpecInfoMap
              .computeIfAbsent(key) { MetricCalculationSpecInfo() }
              .updateRequestedMetricSpecs(componentMetricSetSpec.cumulativeUnique)
          }
        }
      }
    } else {
      for (primitiveReportingSet in primitiveReportingSets) {
        val metricCalculationSpecInfoMap = computeIfAbsent(primitiveReportingSet) { mutableMapOf() }

        if (componentMetricSetSpec.hasNonCumulativeUnique()) {
          for (filter in filters) {
            val key =
              createMetricCalculationSpecInfoKey(filter, groupings, false, metricFrequencySpec)

            // Insert or update entry in map belonging to Primitive ReportingSet
            metricCalculationSpecInfoMap
              .computeIfAbsent(key) { MetricCalculationSpecInfo() }
              .updateRequestedMetricSpecs(componentMetricSetSpec.nonCumulativeUnique)
          }
        }

        if (componentMetricSetSpec.hasCumulativeUnique()) {
          for (filter in filters) {
            val key =
              createMetricCalculationSpecInfoKey(filter, groupings, true, metricFrequencySpec)

            // Insert or update entry in map belonging to Primitive ReportingSet
            metricCalculationSpecInfoMap
              .computeIfAbsent(key) { MetricCalculationSpecInfo() }
              .updateRequestedMetricSpecs(componentMetricSetSpec.cumulativeUnique)
          }
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
private fun MetricCalculationSpecInfo.updateRequestedMetricSpecs(
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
private fun MetricCalculationSpecInfo.updateRequestedMetricSpecs(
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
 * @return [MetricCalculationSpecInfoKey] for use as a key in a map
 */
private fun createMetricCalculationSpecInfoKey(
  metricCalculationSpecFilter: String,
  groupings: List<MetricCalculationSpec.Grouping>,
  cumulative: Boolean,
  basicReportMetricFrequencySpec: MetricFrequencySpec,
): MetricCalculationSpecInfoKey {
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

  return MetricCalculationSpecInfoKey(
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
