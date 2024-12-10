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
  fun convertTagToJsonFormat(tag: String): String {
    // Remove the curly braces
    val cleanedInput = tag.substring(1, tag.length - 1)

    // Split the tag into key-value pairs
    val pairs = cleanedInput.split(", ")

    var output = "{"

    for (pair in pairs) {
      val parts = pair.split("=")
      val key = parts[0].trim().lowercase()
      val value =
        if (parts.size > 1) {
          parts[1].trim()
        } else {
          ""
        }

      when (key) {
        "target",
        "measured_entity",
        "measurement_entities",
        "metrics",
        "set_operation" -> {
          if (value.isEmpty()) {
            output += "\"$key\": [], "
          } else {
            // Split the value string into a list of strings separated by comma.
            val valueList = value.split(",").map { it.trim() }

            // Add the list to the output string.
            output += ("\"$key\": [")
            for (i in valueList.indices) {
              output += ("\"${valueList[i]}\"")
              if (i < valueList.size - 1) {
                output += (", ")
              }
            }
            output += ("], ")
          }
        }
        "lhs_reporting_set_ids",
        "rhs_reporting_set_ids" -> {
          if (value.isEmpty()) {
            output += "\"$key\": [], "
          } else {
            // Split the value string into a list of strings separated by space.
            val valueList = value.split(" ").map { it.trim() }

            // Add the list to the output string.
            output += ("\"$key\": [")
            for (i in valueList.indices) {
              output += ("\"${valueList[i]}\"")
              if (i < valueList.size - 1) {
                output += (", ")
              }
            }
            output += ("], ")
          }
        }
        else -> {
          // Add other key-value pairs.
          output += ("\"$key\": \"$value\", ")
        }
      }
    }

    // Remove the trailing comma and space if there is any.
    if (output.length > 1) {
      output = output.take(output.length - 2)
    }

    output += ("}")

    return output
  }

  // TODO(@ple13): Move this function to a separate Origin-specific package.
  fun getReportingSetFromTag(tag: String): ReportingSet {
    val protoBuilder = ReportingSet.newBuilder()
    try {
      JsonFormat.parser().merge(convertTagToJsonFormat(tag), protoBuilder)
    } catch (e: InvalidProtocolBufferException) {
      throw IllegalArgumentException("Failed to parse ReportingSet from JSON string", e)
    }
    return protoBuilder.build()
  }

  // TODO(@ple13): Move this function to a separate Origin-specific package.
  fun getMetricCalculationSpecFromTag(tag: String): MetricCalculationSpec {
    val protoBuilder = MetricCalculationSpec.newBuilder()
    try {
      JsonFormat.parser().merge(convertTagToJsonFormat(tag), protoBuilder)
    } catch (e: InvalidProtocolBufferException) {
      throw IllegalArgumentException("Failed to parse ReportingSet from JSON string", e)
    }
    return protoBuilder.build()
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
        reportingSetId.substringAfterLast("/") to reportingSet.targetList
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
            dataProviders += reportingSet.targetList
            isCumulative = metricCalculationSpec.cumulative
            setOperation = metricCalculationSpec.setOperation
            uniqueReachTarget = reportingSet.uniqueReachTarget
            rightHandSideTargets +=
              reportingSet.rhsReportingSetIdsList
                .flatMap { id -> targetByShortReportingSetId.getValue(id) }
                .toSet()
                .toList()
                .sorted()
            leftHandSideTargets +=
              reportingSet.lhsReportingSetIdsList
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
