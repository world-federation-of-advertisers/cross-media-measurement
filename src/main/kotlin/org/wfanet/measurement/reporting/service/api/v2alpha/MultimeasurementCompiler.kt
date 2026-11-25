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

import com.google.protobuf.Empty
import com.google.protobuf.Timestamp
import com.google.type.DateTime
import com.google.type.interval
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.temporal.TemporalAdjusters
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.EventMessageDescriptor
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.PopulationAttributeValue
import org.wfanet.measurement.api.v2alpha.Query
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.VectorSet
import org.wfanet.measurement.api.v2alpha.VidFilter
import org.wfanet.measurement.api.v2alpha.VidFilterKt
import org.wfanet.measurement.api.v2alpha.populationAttributeValue
import org.wfanet.measurement.api.v2alpha.query
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.vectorSet
import org.wfanet.measurement.api.v2alpha.vidFilter
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.ReportComputation
import org.wfanet.measurement.internal.reporting.v2.ReportingImpressionQualificationFilter as InternalReportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.ResultGroupQueryMapping
import org.wfanet.measurement.internal.reporting.v2.ResultGroupQueryMappingKt
import org.wfanet.measurement.internal.reporting.v2.reportComputation
import org.wfanet.measurement.internal.reporting.v2.resultGroupQueryMapping
import org.wfanet.measurement.reporting.v2alpha.DimensionSpec
import org.wfanet.measurement.reporting.v2alpha.EventFilter
import org.wfanet.measurement.reporting.v2alpha.EventTemplateField
import org.wfanet.measurement.reporting.v2alpha.MetricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.ResultGroupSpec

/**
 * Compiles a [BasicReport] into one TrusTEE v2 multimeasurement per effective impression
 * qualification filter, with a [ReportComputation] mapping each `Query` to the `ResultGroup` slot
 * its cells fill.
 *
 * A base vector is one `DataProvider`'s `EventGroup`s of one component, under one CEL event filter
 * (the impression qualification filter and the non-demographic dimension filters), over one period.
 * The periods partition the reporting interval at every window boundary of every `ResultGroupSpec`,
 * so any window is the sum of the vectors of the periods it covers. Identical base vectors are
 * shared across queries. Demographic filters become `VidFilter.attributes` and demographic
 * groupings become `Query.grouping_paths`; neither adds vectors.
 *
 * @param eventTemplateFieldsByPath map of event template field path to its info
 * @param emitCelNullGuardsForNestedMembers whether generated CEL guards nested oneof members
 *   against null
 */
class MultimeasurementCompiler(
  private val eventTemplateFieldsByPath: Map<String, EventMessageDescriptor.EventTemplateFieldInfo>,
  private val emitCelNullGuardsForNestedMembers: Boolean,
) {
  /**
   * A `ReportingUnit` component resolved against the CMMS.
   *
   * @property externalReportingSetId external ID of the component's primitive `ReportingSet`
   * @property cmmsEventGroups resource names of the CMMS `EventGroup`s the component entails
   */
  data class ReportingUnitComponent(
    val externalReportingSetId: String,
    val cmmsEventGroups: List<String>,
  )

  /** The multimeasurement for one impression qualification filter of a [BasicReport]. */
  data class CompiledReportComputation(
    val impressionQualificationFilter: InternalReportingImpressionQualificationFilter,
    /** Multimeasurement whose `RequisitionSpec`s have no `nonce` or `measurement_public_key`. */
    val multimeasurement: Measurement.Multimeasurement,
    /** Computation without request or `Measurement` IDs. */
    val reportComputation: ReportComputation,
  )

  /**
   * Compiles [basicReport].
   *
   * @param basicReport [BasicReport] with `effective_report_start` and effective impression
   *   qualification filters set
   * @param components map of `ReportingUnit` component resource name, as in the public
   *   `ReportingUnit.components`, to the resolved component
   * @param impressionQualificationFilterIds map of external impression qualification filter ID to
   *   its internal ID
   * @return one [CompiledReportComputation] per effective impression qualification filter, in order
   * @throws IllegalArgumentException if [basicReport] uses a feature with no multimeasurement form,
   *   or references a component or filter missing from the maps
   */
  fun compile(
    basicReport: BasicReport,
    components: Map<String, ReportingUnitComponent>,
    impressionQualificationFilterIds: Map<String, Long>,
  ): List<CompiledReportComputation> {
    val details = basicReport.details
    require(details.effectiveImpressionQualificationFiltersCount > 0) {
      "BasicReport has no effective impression qualification filters"
    }
    val reportingInterval = details.reportingInterval
    require(reportingInterval.hasEffectiveReportStart()) {
      "BasicReport has no effective_report_start"
    }
    val clock = ReportClock(reportingInterval.effectiveReportStart)
    val reportEnd: LocalDate =
      reportingInterval.reportEnd.let { LocalDate.of(it.year, it.month, it.day) }
    require(clock.reportStart < reportEnd) { "Reporting interval is empty" }

    val specPlans: List<SpecPlan> =
      details.resultGroupSpecsList.map { internalSpec ->
        planSpec(internalSpec.toResultGroupSpec(), clock.reportStart, reportEnd, components)
      }
    val boundaries: List<LocalDate> =
      (specPlans.flatMap { it.windowEnds } + clock.reportStart).toSortedSet().toList()
    val periods: List<Period> = boundaries.zipWithNext { start, end -> Period(start, end) }

    return details.effectiveImpressionQualificationFiltersList.map { iqf ->
      val impressionQualificationFilterId: Long =
        if (iqf.externalImpressionQualificationFilterId.isEmpty()) {
          CUSTOM_IMPRESSION_QUALIFICATION_FILTER_ID
        } else {
          val externalId: String = iqf.externalImpressionQualificationFilterId
          requireNotNull(impressionQualificationFilterIds[externalId]) {
            "Impression qualification filter $externalId not found"
          }
        }
      val iqfFilter: String =
        buildCelExpression(
          iqf.filterSpecsList.map { it.toImpressionQualificationFilterSpec() },
          eventTemplateFieldsByPath,
          emitCelNullGuardsForNestedMembers,
        )
      val builder = ComputationBuilder(clock, periods)
      for ((specIndex, specPlan) in specPlans.withIndex()) {
        builder.addSpec(specIndex, specPlan, iqfFilter)
      }
      CompiledReportComputation(
        impressionQualificationFilter = iqf,
        multimeasurement = builder.buildMultimeasurement(),
        reportComputation =
          reportComputation {
            cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
            externalBasicReportId = basicReport.externalBasicReportId
            this.impressionQualificationFilterId = impressionQualificationFilterId
            mappings += builder.mappings
          },
      )
    }
  }

  private fun planSpec(
    spec: ResultGroupSpec,
    reportStart: LocalDate,
    reportEnd: LocalDate,
    components: Map<String, ReportingUnitComponent>,
  ): SpecPlan {
    require(spec.reportingUnit.componentsCount > 0) { "ResultGroupSpec has no components" }
    require(!spec.resultGroupMetricSpec.reportingUnit.stackedIncrementalReach) {
      "stacked_incremental_reach has no ResultGroup slot in a ResultGroupQueryMapping"
    }
    val resolvedComponents: List<ReportingUnitComponent> =
      spec.reportingUnit.componentsList.map {
        requireNotNull(components[it]) { "Component $it not resolved" }
      }
    val windowEnds: List<LocalDate> = buildWindowEnds(spec.metricFrequency, reportStart, reportEnd)
    val attributes = mutableMapOf<String, PopulationAttributeValue>()
    val eventFilters = mutableListOf<EventFilter>()
    for (filter in spec.dimensionSpec.filtersList) {
      val term: EventTemplateField? = filter.termsList.singleOrNull()
      // A disjunction, a non-enum value, or a second filter on the same attribute has no
      // `VidFilter.attributes` form, so the DataProvider applies it as an event filter instead.
      if (
        term != null &&
          term.value.selectorCase == EventTemplateField.FieldValue.SelectorCase.ENUM_VALUE &&
          eventTemplateFieldsByPath[term.path]?.isPopulationAttribute == true &&
          term.path !in attributes
      ) {
        attributes[term.path] = populationAttributeValue { enumValue = term.value.enumValue }
      } else {
        eventFilters += filter
      }
    }
    return SpecPlan(
      spec = spec,
      components = resolvedComponents,
      reportStart = reportStart,
      windowEnds = windowEnds,
      attributes = attributes,
      groupingPaths = buildGroupingPaths(spec.dimensionSpec),
      dimensionFilter =
        buildCelExpression(
          eventFilters,
          eventTemplateFieldsByPath,
          emitCelNullGuardsForNestedMembers,
        ),
    )
  }

  private fun buildGroupingPaths(dimensionSpec: DimensionSpec): List<String> {
    val paths: List<String> = dimensionSpec.grouping.eventTemplateFieldsList
    for (path in paths) {
      val fieldInfo: EventMessageDescriptor.EventTemplateFieldInfo? =
        eventTemplateFieldsByPath[path]
      require(fieldInfo != null && fieldInfo.isPopulationAttribute && fieldInfo.enumType != null) {
        "Grouping $path is not an enum population attribute"
      }
    }
    return paths
  }

  /** Inclusive start and exclusive end of a period, each at the report's start time of day. */
  private data class Period(val start: LocalDate, val end: LocalDate)

  private data class Window(val start: LocalDate, val end: LocalDate, val cumulative: Boolean)

  private data class SpecPlan(
    val spec: ResultGroupSpec,
    val components: List<ReportingUnitComponent>,
    val reportStart: LocalDate,
    /** Ends of the windows, ascending, the last being the report end. */
    val windowEnds: List<LocalDate>,
    val attributes: Map<String, PopulationAttributeValue>,
    val groupingPaths: List<String>,
    /** CEL for the dimension filters without a `VidFilter.attributes` form. */
    val dimensionFilter: String,
  ) {
    val nonCumulativeWindows: List<Window>
      get() =
        (listOf(reportStart) + windowEnds.dropLast(1)).zip(windowEnds) { start, end ->
          Window(start, end, cumulative = false)
        }

    val cumulativeWindows: List<Window>
      get() = windowEnds.map { Window(reportStart, it, cumulative = true) }
  }

  private data class VectorKey(
    val dataProvider: String,
    val eventGroups: List<String>,
    val filter: String,
    val period: Period,
  )

  /** The `ResultGroup` slot a query fills, identified within its window by [id]. */
  private sealed class Slot(val id: String) {
    object ReportingUnit : Slot("ru")

    class Component(val index: Int) : Slot("c$index")

    class ComponentUnique(val index: Int) : Slot("u$index")

    class Intersection(val indexes: List<Int>) : Slot("i" + indexes.joinToString("."))
  }

  /** Accumulates the vectors, queries, and mappings of one multimeasurement. */
  private class ComputationBuilder(
    private val clock: ReportClock,
    private val periods: List<Period>,
  ) {
    private val indexesByVector = mutableMapOf<VectorKey, Int>()
    private val queries = mutableListOf<Query>()
    val mappings = mutableListOf<ResultGroupQueryMapping>()

    fun addSpec(specIndex: Int, plan: SpecPlan, iqfFilter: String) {
      val filter: String = buildCelExpressions(listOf(iqfFilter), plan.dimensionFilter).single()
      val metricSpec: ResultGroupMetricSpec = plan.spec.resultGroupMetricSpec
      val componentIndexes: List<Int> = plan.components.indices.toList()

      fun addWindows(windows: List<Window>, slots: List<Slot>, maxFrequency: Int) {
        for (window in windows) {
          for (slot in slots) {
            addQuery(specIndex, plan, filter, window, slot, maxFrequency)
          }
        }
      }

      if (metricSpec.hasReportingUnit()) {
        val reportingUnit = metricSpec.reportingUnit
        if (reportingUnit.hasNonCumulative()) {
          addWindows(
            plan.nonCumulativeWindows,
            listOf(Slot.ReportingUnit),
            reportingUnit.nonCumulative.maxFrequency,
          )
        }
        if (reportingUnit.hasCumulative()) {
          addWindows(
            plan.cumulativeWindows,
            listOf(Slot.ReportingUnit),
            reportingUnit.cumulative.maxFrequency,
          )
        }
      }
      if (metricSpec.hasComponent()) {
        val component = metricSpec.component
        if (component.hasNonCumulative()) {
          addWindows(
            plan.nonCumulativeWindows,
            componentIndexes.map { Slot.Component(it) },
            component.nonCumulative.maxFrequency,
          )
        }
        if (component.hasCumulative()) {
          addWindows(
            plan.cumulativeWindows,
            componentIndexes.map { Slot.Component(it) },
            component.cumulative.maxFrequency,
          )
        }
        if (component.hasNonCumulativeUnique()) {
          addWindows(
            plan.nonCumulativeWindows,
            componentIndexes.map { Slot.ComponentUnique(it) },
            maxFrequency = 1,
          )
        }
        if (component.hasCumulativeUnique()) {
          addWindows(
            plan.cumulativeWindows,
            componentIndexes.map { Slot.ComponentUnique(it) },
            maxFrequency = 1,
          )
        }
      }
      if (metricSpec.hasComponentIntersection()) {
        val intersection = metricSpec.componentIntersection
        val slots: List<Slot> =
          intersection.contributorCountList.distinct().sorted().flatMap { count ->
            require(count in 1..componentIndexes.size) { "Invalid contributor_count $count" }
            combinations(componentIndexes, count).map { Slot.Intersection(it) }
          }
        if (intersection.hasNonCumulative()) {
          addWindows(plan.nonCumulativeWindows, slots, intersection.nonCumulative.maxFrequency)
        }
        if (intersection.hasCumulative()) {
          addWindows(plan.cumulativeWindows, slots, intersection.cumulative.maxFrequency)
        }
      }
    }

    private fun addQuery(
      specIndex: Int,
      plan: SpecPlan,
      filter: String,
      window: Window,
      slot: Slot,
      maxFrequency: Int,
    ) {
      fun vectorsOf(componentIndexes: Iterable<Int>): VectorSet = vectorSet {
        baseVectors +=
          componentIndexes
            .flatMap { vectorsFor(plan.components[it], filter, window) }
            .distinct()
            .sorted()
      }

      val allIndexes: List<Int> = plan.components.indices.toList()
      val queryId = buildQueryId(specIndex, window, slot)
      // Summands first, so that vectors are indexed in the order queries sum them.
      val summands: VectorSet =
        when (slot) {
          is Slot.ReportingUnit -> vectorsOf(allIndexes)
          is Slot.Component -> vectorsOf(listOf(slot.index))
          is Slot.ComponentUnique -> vectorsOf(listOf(slot.index))
          is Slot.Intersection -> vectorsOf(slot.indexes)
        }
      val reachClauses: List<VidFilter.ReachClause> =
        when (slot) {
          is Slot.ReportingUnit,
          is Slot.Component -> emptyList()
          is Slot.ComponentUnique -> {
            val others: List<Int> = allIndexes - slot.index
            if (others.isEmpty()) {
              emptyList()
            } else {
              listOf(VidFilterKt.reachClause { unreachedSet = vectorsOf(others) })
            }
          }
          is Slot.Intersection ->
            listOf(
              VidFilterKt.reachClause { reachedSets += slot.indexes.map { vectorsOf(listOf(it)) } }
            )
        }

      queries += query {
        id = queryId
        this.summands = summands
        if (reachClauses.isNotEmpty() || plan.attributes.isNotEmpty()) {
          vidFilter = vidFilter {
            this.reachClauses += reachClauses
            attributes.putAll(plan.attributes)
          }
        }
        groupingPaths += plan.groupingPaths
        this.maxFrequency = maxFrequency
      }
      mappings += resultGroupQueryMapping {
        this.queryId = queryId
        resultGroupSpecIndex = specIndex
        windowStart = window.start.toProtoDate()
        windowEnd = window.end.toProtoDate()
        cumulative = window.cumulative
        when (slot) {
          is Slot.ReportingUnit -> reportingUnit = Empty.getDefaultInstance()
          is Slot.Component ->
            component =
              ResultGroupQueryMappingKt.component {
                component = plan.components[slot.index].externalReportingSetId
              }
          is Slot.ComponentUnique ->
            componentUnique =
              ResultGroupQueryMappingKt.component {
                component = plan.components[slot.index].externalReportingSetId
              }
          is Slot.Intersection ->
            intersection =
              ResultGroupQueryMappingKt.intersection {
                components += slot.indexes.map { plan.components[it].externalReportingSetId }
              }
        }
      }
    }

    /** Returns the indexes of the vectors that sum to [component] over [window]. */
    private fun vectorsFor(
      component: ReportingUnitComponent,
      filter: String,
      window: Window,
    ): List<Int> {
      val eventGroupsByDataProvider: Map<String, List<String>> =
        component.cmmsEventGroups
          .groupBy { eventGroup ->
            requireNotNull(CmmsEventGroupKey.fromName(eventGroup)) {
                "Invalid EventGroup name $eventGroup"
              }
              .parentKey
              .toName()
          }
          .toSortedMap()
      return buildList {
        for ((dataProvider, eventGroups) in eventGroupsByDataProvider) {
          for (period in periods) {
            if (period.start >= window.start && period.end <= window.end) {
              val key = VectorKey(dataProvider, eventGroups.distinct().sorted(), filter, period)
              add(indexesByVector.getOrPut(key) { indexesByVector.size })
            }
          }
        }
      }
    }

    fun buildMultimeasurement(): Measurement.Multimeasurement {
      val vectorsByDataProvider: Map<String, List<Map.Entry<VectorKey, Int>>> =
        indexesByVector.entries.groupBy { it.key.dataProvider }
      return MeasurementKt.multimeasurement {
        for ((dataProvider, entries) in vectorsByDataProvider) {
          dataProviderVectors +=
            MeasurementKt.MultimeasurementKt.dataProviderVectors {
              this.dataProvider = dataProvider
              for ((key, index) in entries) {
                vectors +=
                  MeasurementKt.MultimeasurementKt.vector {
                    this.index = index
                    requisitionSpec = buildRequisitionSpec(key)
                  }
              }
            }
        }
        queries += this@ComputationBuilder.queries
      }
    }

    /** Highest frequency a `BasicMetricSetSpec` needs a histogram bin for. */
    private val ResultGroupMetricSpec.BasicMetricSetSpec.maxFrequency: Int
      get() = maxOf(1, kPlusReach)

    /**
     * Builds the ID of the query filling [slot] of `ResultGroupSpec` [specIndex] in [window]. The
     * ID depends only on the slot, so recompiling the same `BasicReport` yields the same IDs.
     */
    private fun buildQueryId(specIndex: Int, window: Window, slot: Slot): String {
      return listOf(
          specIndex.toString(),
          if (window.cumulative) "c" else "n",
          window.start.toString(),
          window.end.toString(),
          slot.id,
        )
        .joinToString("/")
    }

    /** Returns the [size]-element combinations of [items], in lexicographic order. */
    private fun combinations(items: List<Int>, size: Int): List<List<Int>> {
      if (size == 0) {
        return listOf(emptyList())
      }
      return items.indices.flatMap { i ->
        combinations(items.subList(i + 1, items.size), size - 1).map { listOf(items[i]) + it }
      }
    }

    private fun buildRequisitionSpec(vectorKey: VectorKey): RequisitionSpec {
      val collectionInterval = interval {
        startTime = clock.toTimestamp(vectorKey.period.start)
        endTime = clock.toTimestamp(vectorKey.period.end)
      }
      return requisitionSpec {
        events =
          RequisitionSpecKt.events {
            for (eventGroup in vectorKey.eventGroups) {
              eventGroups +=
                RequisitionSpecKt.eventGroupEntry {
                  this.key = eventGroup
                  value =
                    RequisitionSpecKt.EventGroupEntryKt.value {
                      this.collectionInterval = collectionInterval
                      if (vectorKey.filter.isNotEmpty()) {
                        filter = RequisitionSpecKt.eventFilter { expression = vectorKey.filter }
                      }
                    }
                }
            }
          }
      }
    }
  }

  /** Converts dates to instants at the time of day and offset of the report start. */
  private class ReportClock(effectiveReportStart: DateTime) {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf case fields cannot be null.
    private val zoneId: ZoneId =
      when (effectiveReportStart.timeOffsetCase) {
        DateTime.TimeOffsetCase.UTC_OFFSET ->
          ZoneOffset.ofTotalSeconds(effectiveReportStart.utcOffset.seconds.toInt())
        DateTime.TimeOffsetCase.TIME_ZONE -> ZoneId.of(effectiveReportStart.timeZone.id)
        DateTime.TimeOffsetCase.TIMEOFFSET_NOT_SET ->
          throw IllegalArgumentException("effective_report_start has no time offset")
      }
    private val timeOfDay: LocalTime =
      LocalTime.of(
        effectiveReportStart.hours,
        effectiveReportStart.minutes,
        effectiveReportStart.seconds,
        effectiveReportStart.nanos,
      )
    val reportStart: LocalDate =
      LocalDate.of(effectiveReportStart.year, effectiveReportStart.month, effectiveReportStart.day)

    fun toTimestamp(date: LocalDate): Timestamp =
      ZonedDateTime.of(date, timeOfDay, zoneId).toInstant().toProtoTime()
  }

  companion object {
    /**
     * Internal ID recorded for a custom impression qualification filter, matching the Reporting
     * Spanner `ReportingSetResults` convention.
     */
    const val CUSTOM_IMPRESSION_QUALIFICATION_FILTER_ID = -1L

    /**
     * Returns the window ends of [metricFrequency] over [reportStart, reportEnd), following
     * `ReportsService` time interval generation: weekly windows end on the given day of the week,
     * with partial first and last windows.
     */
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf case fields cannot be null.
    private fun buildWindowEnds(
      metricFrequency: MetricFrequencySpec,
      reportStart: LocalDate,
      reportEnd: LocalDate,
    ): List<LocalDate> {
      return when (metricFrequency.selectorCase) {
        MetricFrequencySpec.SelectorCase.TOTAL -> listOf(reportEnd)
        MetricFrequencySpec.SelectorCase.WEEKLY -> {
          val dayOfWeek = java.time.DayOfWeek.valueOf(metricFrequency.weekly.name)
          buildList {
            var end: LocalDate = reportStart.with(TemporalAdjusters.nextOrSame(dayOfWeek))
            if (end == reportStart) {
              end = end.plusWeeks(1)
            }
            while (end < reportEnd) {
              add(end)
              end = end.plusWeeks(1)
            }
            add(reportEnd)
          }
        }
        MetricFrequencySpec.SelectorCase.SELECTOR_NOT_SET ->
          throw IllegalArgumentException("metric_frequency not set")
      }
    }
  }
}
