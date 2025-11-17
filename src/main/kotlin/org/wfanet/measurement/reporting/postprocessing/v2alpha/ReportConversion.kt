// Copyright 2024 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.reporting.postprocessing.v2alpha

import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.util.JsonFormat
import org.wfanet.measurement.internal.reporting.postprocessing.MeasurementDetail
import org.wfanet.measurement.internal.reporting.postprocessing.MeasurementDetailKt
import org.wfanet.measurement.internal.reporting.postprocessing.MeasurementDetailKt.measurementResult
import org.wfanet.measurement.internal.reporting.postprocessing.ReportSummary
import org.wfanet.measurement.internal.reporting.postprocessing.measurementDetail
import org.wfanet.measurement.internal.reporting.postprocessing.reportSummary
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricResult
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.Report.MetricCalculationResult

/** Represents a summary of a reporting set. */
data class ReportingSetSummary(
  /** The measurement policy used for the reporting set. */
  val measurementPolicy: String,
  /** The target for the reporting set. */
  val target: List<String>,
  /** The unique reach target of the reporting set. */
  val uniqueReachTarget: String,
  /** The IDs of the left-hand side reporting sets used in the set operation. */
  val lhsReportingSetIds: List<String>,
  /** The IDs of the right-hand side reporting sets used in the set operation. */
  val rhsReportingSetIds: List<String>,
)

/** Represents a metric calculation specification. */
data class MetricCalculationSpec(
  /** The common filter used in the measurement. */
  val commonFilter: String,
  /** Whether the measurement is cumulative. */
  val cumulative: Boolean,
  /** The grouping used in the measurement. */
  val grouping: String,
  /** The frequency of cumulative measurements. */
  val metricFrequency: String,
  /** The list of metrics to measure (e.g. reach, impressions). */
  val metrics: List<String>,
  /** The set operation used in the calculation (e.g. cumulative, union, difference). */
  val setOperation: String,
)

object ReportConversion {
  const val POPULATION_KEY = "population"

  fun getReportFromJsonString(reportAsJsonString: String): Report {
    val protoBuilder = Report.newBuilder()
    try {
      JsonFormat.parser().merge(reportAsJsonString, protoBuilder)
    } catch (e: InvalidProtocolBufferException) {
      throw IllegalArgumentException("Failed to parse Report from JSON string", e)
    }
    return protoBuilder.build()
  }

  /**
   * A map to maintain backward compatibility with the old population coding.
   *
   * All these coding are temporary and will be replaced by market-agnostic processing in MC API
   * Phase II.
   *
   * TODO(@ple13): Remove this map when the old population coding has been deprecated.
   */
  val populationCodingMap: Map<String, String> =
    mapOf(
      "MALE_YEARS_16_TO_34" to "MALE_AGE_GROUP1",
      "MALE_YEARS_35_TO_54" to "MALE_AGE_GROUP2",
      "MALE_YEARS_55_PLUS" to "MALE_AGE_GROUP3",
      "FEMALE_YEARS_16_TO_34" to "FEMALE_AGE_GROUP1",
      "FEMALE_YEARS_35_TO_54" to "FEMALE_AGE_GROUP2",
      "FEMALE_YEARS_55_PLUS" to "FEMALE_AGE_GROUP3",
      "MALE_AGE_GROUP1" to "MALE_AGE_GROUP1",
      "MALE_AGE_GROUP2" to "MALE_AGE_GROUP2",
      "MALE_AGE_GROUP3" to "MALE_AGE_GROUP3",
      "FEMALE_AGE_GROUP1" to "FEMALE_AGE_GROUP1",
      "FEMALE_AGE_GROUP2" to "FEMALE_AGE_GROUP2",
      "FEMALE_AGE_GROUP3" to "FEMALE_AGE_GROUP3",
      "-" to "-",
    )

  /**
   * A map where the key is a canonical list of predicate strings defining a demographic group and
   * the value is the corresponding canonical string constant representing that group (e.g.,
   * "MALE_AGE_GROUP1").
   *
   * This map facilitates looking up the string constant based on the set of predicates that define
   * it.
   *
   * TODO(ple13): Read the population mapping from the report when it is available, instead of using
   *   this hardcoded map.
   */
  val groupingPredicatesToStringMap: Map<List<String>, String> =
    mapOf(
      listOf("common.sex==1", "common.age_group==1") to "MALE_AGE_GROUP1",
      listOf("common.sex==1", "common.age_group==2") to "MALE_AGE_GROUP2",
      listOf("common.sex==1", "common.age_group==3") to "MALE_AGE_GROUP3",
      listOf("common.sex==2", "common.age_group==1") to "FEMALE_AGE_GROUP1",
      listOf("common.sex==2", "common.age_group==2") to "FEMALE_AGE_GROUP2",
      listOf("common.sex==2", "common.age_group==3") to "FEMALE_AGE_GROUP3",
    )

  // TODO(@ple13): Move this function to a separate Origin-specific package.
  fun getReportingSetSummaryFromTag(tag: String): ReportingSetSummary {
    val keyValuePairs: List<String> = tag.trim('{', '}').split(", ")
    val data = mutableMapOf<String, String>()

    for (pair in keyValuePairs) {
      val (key, value) = pair.split("=")
      data[key] = value
    }

    return ReportingSetSummary(
      measurementPolicy =
        data.getValue("measurement_policy").ifEmpty {
          data.getValue("measurement_policy_incrementality")
        },
      target = data.getValue("target").split(","),
      uniqueReachTarget = data.getValue("unique_Reach_Target"),
      lhsReportingSetIds =
        data.getValue("lhs_reporting_set_ids").takeUnless { it.isEmpty() }?.split(" ")
          ?: emptyList(),
      rhsReportingSetIds =
        data.getValue("rhs_reporting_set_ids").takeUnless { it.isEmpty() }?.split(" ")
          ?: emptyList(),
    )
  }

  // TODO(@ple13): Move this function to a separate Origin-specific package.
  fun getMetricCalculationSpecFromTag(tag: String): MetricCalculationSpec {
    val keyValuePairs: List<String> = tag.trim('{', '}').split(", ")
    val data = mutableMapOf<String, String>()

    for (pair in keyValuePairs) {
      val (key, value) = pair.split("=", limit = 2)
      data[key] = value
    }

    return MetricCalculationSpec(
      commonFilter = data.getValue("common_filter"),
      cumulative = data.getValue("cumulative").toBoolean(),
      grouping = data.getValue("grouping"),
      metricFrequency = data.getValue("metric_frequency"),
      metrics = data.getValue("metrics").split(","),
      setOperation = data.getValue("set_operation"),
    )
  }

  /** Parses the population tag into a map representing demographic group populations. */
  fun getDemographicGroupPopulationFromTag(tag: String): Map<String, Long> {
    val keyValuePairs: List<String> = tag.split(';')
    val populationMap = mutableMapOf<String, Long>()

    for (pair in keyValuePairs) {
      val (key, value) = pair.split("=", limit = 2)
      populationMap[populationCodingMap.getValue(key)] =
        value.toLongOrNull()
          ?: throw IllegalArgumentException("Value for key '$key' is not a valid Long: '$value'")
    }
    return populationMap
  }

  // TODO(@ple13): Move this function to a separate Origin-specific package.
  fun convertJsontoReportSummaries(reportAsJsonString: String): List<ReportSummary> {
    return getReportFromJsonString(reportAsJsonString).toReportSummaries()
  }
}

// TODO(@ple13): Move this function to a separate Origin-specific package.
fun Report.toReportSummaries(): List<ReportSummary> {
  require(state == Report.State.SUCCEEDED) { "Unsucceeded report is not supported." }

  val reportingSetSummaryById: Map<String, ReportingSetSummary> =
    reportingMetricEntriesList.associate { entry ->
      val reportingSetId: String = entry.key
      val tag: String = tags.getValue(reportingSetId)
      reportingSetId to ReportConversion.getReportingSetSummaryFromTag(tag)
    }

  val metricCalculationSpecs: Set<String> =
    reportingMetricEntriesList.flatMapTo(mutableSetOf()) { it.value.metricCalculationSpecsList }

  val metricCalculationSpecById: Map<String, MetricCalculationSpec> =
    metricCalculationSpecs.associate { specId ->
      val tag: String = tags.getValue(specId)
      specId to ReportConversion.getMetricCalculationSpecFromTag(tag)
    }

  // Reads the population map from the tags.
  val populationPerDemographicGroup: Map<String, Long> =
    if (tags.containsKey(ReportConversion.POPULATION_KEY)) {
      ReportConversion.getDemographicGroupPopulationFromTag(
        tags.getValue(ReportConversion.POPULATION_KEY)
      )
    } else {
      emptyMap()
    }

  val targetByReportingSetId: Map<String, List<String>> =
    reportingSetSummaryById
      .map { (reportingSetId, reportingSetSummary) ->
        reportingSetId.substringAfterLast("/") to reportingSetSummary.target
      }
      .toMap()

  // Extracts the demographic filter groups.
  //
  // All metric calculation specs must have the same common filter. The value of the common filter
  // field is either '-' (i.e. no filtering) or a list of demographic groups separated by
  // semicolons.
  val demographicFilterGroups: List<String> =
    metricCalculationSpecById.values
      .firstOrNull()
      ?.commonFilter
      ?.trim(';')
      ?.split(';')
      ?.filter { it.isNotEmpty() }
      ?.map { group -> ReportConversion.populationCodingMap.getValue(group) } ?: emptyList()

  // Generates a set of demographic groups. If the report doesn't support demographic slicing,
  // the set contains an empty list, otherwise, it contains all the demographic groups.
  val demographicGroups: Set<List<String>> =
    metricCalculationSpecById.values
      .flatMap {
        val groups: List<String> = it.grouping.split(",")
        val sexes: List<String> = groups.filter { it.startsWith("common.sex==") }
        val ageGroups: List<String> = groups.filter { it.startsWith("common.age_group==") }
        when {
          sexes.isNotEmpty() && ageGroups.isNotEmpty() ->
            sexes.flatMap { sex -> ageGroups.map { ageGroup -> listOf(sex, ageGroup) } }
          sexes.isEmpty() && ageGroups.isEmpty() -> listOf(emptyList())
          else -> throw IllegalArgumentException("AgeGroup and Sex must be both set or empty.")
        }
      }
      .toSet()

  // Groups results by (reporting set x metric calculation spec).
  val measurementSets: Map<Pair<String, String>, List<MetricCalculationResult>> =
    metricCalculationResultsList.groupBy { Pair(it.reportingSet, it.metricCalculationSpec) }

  val reportSummaries = mutableListOf<ReportSummary>()

  // Groups the measurements by demographic groups. If the report doesn't support demographic
  // slicing, all measurements belong to the same report summary.
  for (demographicGroup in demographicGroups) {
    val reportSummary = reportSummary {
      if (demographicGroup.isEmpty()) {
        // demographicGroups contains an empty list indicates that this report supports demographic
        // filtering. The population is the sum of all the filtered demographic groups.
        // When common filter is equal to '-', all groups are selected. If the population
        // information is not available in the report, use the default value of 0 as the population.
        this.population =
          if (demographicFilterGroups.contains("-")) {
            populationPerDemographicGroup.values.sum()
          } else {
            demographicFilterGroups.sumOf { group ->
              populationPerDemographicGroup.getOrDefault(group, 0L)
            }
          }
        this.demographicGroups += demographicFilterGroups
      } else {
        // This report supports demographic slicing. If population information is not available in
        // the report, assign the value 0 to the population of that slice.
        this.population =
          populationPerDemographicGroup.getOrDefault(
            ReportConversion.groupingPredicatesToStringMap.getValue(demographicGroup),
            0L,
          )
        this.demographicGroups +=
          ReportConversion.groupingPredicatesToStringMap.getValue(demographicGroup)
      }

      for ((key, value) in measurementSets) {
        val reportingSetSummary: ReportingSetSummary = reportingSetSummaryById.getValue(key.first)
        val metricCalculationSpec: MetricCalculationSpec =
          metricCalculationSpecById.getValue(key.second)

        measurementDetails += measurementDetail {
          measurementPolicy = reportingSetSummary.measurementPolicy.lowercase()
          dataProviders += reportingSetSummary.target
          isCumulative = metricCalculationSpec.cumulative
          setOperation = metricCalculationSpec.setOperation
          uniqueReachTarget = reportingSetSummary.uniqueReachTarget
          rightHandSideTargets +=
            reportingSetSummary.rhsReportingSetIds
              .flatMap { id -> targetByReportingSetId.getValue(id) }
              .toSet()
              .toList()
              .sorted()
          leftHandSideTargets +=
            reportingSetSummary.lhsReportingSetIds
              .flatMap { id -> targetByReportingSetId.getValue(id) }
              .toSet()
              .toList()
              .sorted()
          var measurementList: List<MeasurementDetail.MeasurementResult> =
            value
              .flatMap { it.resultAttributesList }
              .sortedBy { it.timeInterval.endTime.seconds }
              .filter {
                it.groupingPredicatesList.containsAll(demographicGroup) &&
                  demographicGroup.containsAll(it.groupingPredicatesList) &&
                  (it.metricResult.hasReach() ||
                    it.metricResult.hasReachAndFrequency() ||
                    it.metricResult.hasImpressionCount())
              }
              .map { resultAttribute ->
                require(resultAttribute.state == Metric.State.SUCCEEDED) {
                  "Unsucceeded measurement result is not supported."
                }
                MeasurementDetailKt.measurementResult {
                  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
                  when (resultAttribute.metricResult.resultCase) {
                    MetricResult.ResultCase.REACH -> {
                      reach =
                        MeasurementDetailKt.reachResult {
                          this.value = resultAttribute.metricResult.reach.value
                          standardDeviation =
                            resultAttribute.metricResult.reach.univariateStatistics
                              .standardDeviation
                        }
                    }
                    MetricResult.ResultCase.REACH_AND_FREQUENCY -> {
                      reachAndFrequency =
                        MeasurementDetailKt.reachAndFrequencyResult {
                          reach =
                            MeasurementDetailKt.reachResult {
                              this.value =
                                resultAttribute.metricResult.reachAndFrequency.reach.value
                              standardDeviation =
                                resultAttribute.metricResult.reachAndFrequency.reach
                                  .univariateStatistics
                                  .standardDeviation
                            }
                          frequency =
                            MeasurementDetailKt.frequencyResult {
                              bins +=
                                resultAttribute.metricResult.reachAndFrequency.frequencyHistogram
                                  .binsList
                                  .map { bin ->
                                    MeasurementDetailKt.FrequencyResultKt.binResult {
                                      label = bin.label
                                      // If reach is 0, all frequencies are set to 0 as well.
                                      this.value =
                                        if (
                                          resultAttribute.metricResult.reachAndFrequency.reach
                                            .value > 0
                                        ) {
                                          bin.binResult.value
                                        } else {
                                          0.0
                                        }
                                      // TODO(@ple13): Read the standard deviations directly from
                                      // the frequency buckets when the report populates the
                                      // standard deviations for frequency histogram when reach is
                                      // 0.
                                      standardDeviation =
                                        if (
                                          resultAttribute.metricResult.reachAndFrequency.reach
                                            .value > 0 ||
                                            bin.resultUnivariateStatistics.standardDeviation > 0
                                        ) {
                                          bin.resultUnivariateStatistics.standardDeviation
                                        } else {
                                          resultAttribute.metricResult.reachAndFrequency.reach
                                            .univariateStatistics
                                            .standardDeviation
                                        }
                                    }
                                  }
                            }
                        }
                    }
                    MetricResult.ResultCase.IMPRESSION_COUNT -> {
                      impressionCount =
                        MeasurementDetailKt.impressionCountResult {
                          this.value = resultAttribute.metricResult.impressionCount.value
                          standardDeviation =
                            resultAttribute.metricResult.impressionCount.univariateStatistics
                              .standardDeviation
                        }
                    }
                    MetricResult.ResultCase.WATCH_DURATION,
                    MetricResult.ResultCase.POPULATION_COUNT -> {}
                    MetricResult.ResultCase.RESULT_NOT_SET -> {
                      throw IllegalArgumentException(
                        "The result type in MetricResult is not specified."
                      )
                    }
                  }
                  metric = resultAttribute.metric
                }
              }
          measurementResults.addAll(measurementList)
        }
      }
    }
    reportSummaries.add(reportSummary)
  }
  return reportSummaries
}
