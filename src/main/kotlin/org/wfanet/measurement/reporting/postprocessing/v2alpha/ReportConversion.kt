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
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.Report

data class ReportingSet(
  val measurementPolicy: String,
  val target: List<String>,
  val uniqueReachTarget: String,
  val lhsReportingSetIds: List<String>,
  val rhsReportingSetIds: List<String>,
)

data class MetricCalculationSpec(
  val commonFilter: String,
  val cumulative: Boolean,
  val grouping: String,
  val metricFrequency: String,
  val metrics: List<String>,
  val setOperation: String,
)

object ReportConversion {
  fun getReportFromJsonString(reportAsJsonString: String): Report {
    val protoBuilder = Report.newBuilder()
    try {
      JsonFormat.parser().merge(reportAsJsonString, protoBuilder)
    } catch (e: InvalidProtocolBufferException) {
      throw IllegalArgumentException("Failed to parse Report from JSON string", e)
    }
    return protoBuilder.build()
  }

  // TODO(@ple13): Move this function to a separate Origin-specific package.
  fun getReportingSetFromTag(tag: String): ReportingSet {
    val keyValuePairs = tag.trim('{', '}').split(", ")
    val data = mutableMapOf<String, String>()

    for (pair in keyValuePairs) {
      val (key, value) = pair.split("=")
      data[key] = value
    }

    return ReportingSet(
      measurementPolicy = data.getValue("measurement_policy"),
      target = data.getValue("target").split(","),
      uniqueReachTarget = data.getValue("unique_Reach_Target").takeUnless { it.isEmpty() }?: "",
      lhsReportingSetIds = data.getValue("lhs_reporting_set_ids").takeUnless { it.isEmpty() }
        ?.split(" ") ?: emptyList(),
      rhsReportingSetIds = data.getValue("rhs_reporting_set_ids").takeUnless { it.isEmpty() }
        ?.split(" ") ?: emptyList(),
    )
  }

  // TODO(@ple13): Move this function to a separate Origin-specific package.
  fun getMetricCalculationSpecFromTag(tag: String): MetricCalculationSpec {
    val keyValuePairs = tag.trim('{', '}').split(", ")
    val data = mutableMapOf<String, String>()

    for (pair in keyValuePairs) {
      val (key, value) = pair.split("=")
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

  // TODO(@ple13): Move this function to a separate Origin-specific package.
  fun convertJsontoReportSummaries(reportAsJsonString: String): List<ReportSummary> {
    return getReportFromJsonString(reportAsJsonString).toReportSummaries()
  }
}

// TODO(@ple13): Move this function to a separate Origin-specific package.
fun Report.toReportSummaries(): List<ReportSummary> {
  require(state == Report.State.SUCCEEDED) { "Unsucceeded report is not supported." }

  val reportingSetById =
    reportingMetricEntriesList.associate { entry ->
      val reportingSetId = entry.key
      val tag = tags.getValue(reportingSetId)
      reportingSetId to ReportConversion.getReportingSetFromTag(tag)
    }

  val metricCalculationSpecs =
    reportingMetricEntriesList.flatMapTo(mutableSetOf()) { it.value.metricCalculationSpecsList }

  val metricCalculationSpecById =
    metricCalculationSpecs.associate { specId ->
      val tag = tags.getValue(specId)
      specId to ReportConversion.getMetricCalculationSpecFromTag(tag)
    }

  val targetByShortReportingSetId =
    reportingSetById
      .map { (reportingSetId, reportingSet) ->
        reportingSetId.substringAfterLast("/") to reportingSet.target
      }
      .toMap()

  val filterGroups = metricCalculationSpecById.values.map { it.commonFilter }.toSet()

  // Groups results by (reporting set x metric calculation spec).
  val measurementSets =
    metricCalculationResultsList.groupBy { Pair(it.reportingSet, it.metricCalculationSpec) }

  val reportSummaries = mutableListOf<ReportSummary>()
  for (filter in filterGroups) {
    val reportSummary = reportSummary {
      measurementSets.forEach { (key, value) ->
        val reportingSet = reportingSetById.getValue(key.first)
        val metricCalculationSpec = metricCalculationSpecById.getValue(key.second)

        if (metricCalculationSpec.commonFilter == filter) {
          measurementDetails += measurementDetail {
            measurementPolicy = reportingSet.measurementPolicy.lowercase()
            dataProviders += reportingSet.target
            isCumulative = metricCalculationSpec.cumulative
            setOperation = metricCalculationSpec.setOperation
            uniqueReachTarget = reportingSet.uniqueReachTarget
            rightHandSideTargets +=
              reportingSet.rhsReportingSetIds
                .flatMap { id -> targetByShortReportingSetId.getValue(id) }
                .toSet()
                .toList()
                .sorted()
            leftHandSideTargets +=
              reportingSet.lhsReportingSetIds
                .flatMap { id -> targetByShortReportingSetId.getValue(id) }
                .toSet()
                .toList()
                .sorted()
            var measurementList =
              value
                .flatMap { it.resultAttributesList }
                .sortedBy { it.timeInterval.endTime.seconds }
                .filter { it.metricResult.hasReach() || it.metricResult.hasReachAndFrequency() }
                .map { resultAttribute ->
                  require(resultAttribute.state == Metric.State.SUCCEEDED) {
                    "Unsucceeded measurement result is not supported."
                  }
                  MeasurementDetailKt.measurementResult {
                    if (resultAttribute.metricResult.hasReach()) {
                      reach = resultAttribute.metricResult.reach.value
                      standardDeviation =
                        resultAttribute.metricResult.reach.univariateStatistics.standardDeviation
                    } else {
                      reach = resultAttribute.metricResult.reachAndFrequency.reach.value
                      standardDeviation =
                        resultAttribute.metricResult.reachAndFrequency.reach.univariateStatistics
                          .standardDeviation
                    }
                    metric = resultAttribute.metric
                  }
                }
            measurementResults.addAll(measurementList)
          }
        }
      }
    }
    reportSummaries.add(reportSummary)
  }
  return reportSummaries
}
