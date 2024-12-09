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

data class ReportingSetSummary(
  /** The measurement policy (e.g. AMI, MRC, or CUSTOM) used for this reporting set. */
  val measurementPolicy: String,
  /** The data providers associated with the reporting set. */
  val dataProviders: List<String>,
)

data class SetOperationSummary(
  val isCumulative: Boolean,
  /** The type of set operation which is one of cumulative, union, difference, or incremental. */
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

  fun convertTagToJsonFormat(tag: String): String {
    // Remove the curly braces
    val cleanedInput = tag.substring(1, tag.length - 1)

    // Split the tag into key-value pairs
    val pairs = cleanedInput.split(", ")

    var output = "{"

    for (pair in pairs) {
      val parts = pair.split("=")
      val key = parts[0].trim().lowercase()
      val value = if (parts.size > 1) {
        parts[1].trim()
      } else {
        ""
      }

      when(key) {
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

  fun getReportingSetFromTag(tag: String): ReportingSet {
    val protoBuilder = ReportingSet.newBuilder()
    try {
      JsonFormat.parser().merge(convertTagToJsonFormat(tag), protoBuilder)
    } catch (e: InvalidProtocolBufferException) {
      throw IllegalArgumentException("Failed to parse ReportingSet from JSON string", e)
    }
    return protoBuilder.build()
  }

  fun getMetricCalculationSpecFromTag(tag: String): MetricCalculationSpec {
    val protoBuilder = MetricCalculationSpec.newBuilder()
    try {
      JsonFormat.parser().merge(convertTagToJsonFormat(tag), protoBuilder)
    } catch (e: InvalidProtocolBufferException) {
      throw IllegalArgumentException("Failed to parse ReportingSet from JSON string", e)
    }
    return protoBuilder.build()
  }

  fun convertJsontoReportSummaries(reportAsJsonString: String): List<ReportSummary> {
    return getReportFromJsonString(reportAsJsonString).toReportSummaries()
  }

  // TODO(@ple13): Move this function to a separate Origin-specific package.
  fun getMeasurementPolicy(tag: String): String {
    when {
      "measurement_policy=AMI" in tag -> return "ami"
      "measurement_policy=MRC" in tag -> return "mrc"
      "measurement_policy=CUSTOM" in tag -> return "custom"
      else -> error("Measurement policy must be ami, or mrc, or custom.")
    }
  }

  // TODO(@ple13): Move this function to a separate Origin-specific package.
  fun getSetOperation(tag: String): String {
    val parts = tag.split(", ")
    val setOperationPart = parts.find { it.startsWith("set_operation=") }
    return setOperationPart?.let { it.substringAfter("set_operation=") }
      ?: error("Set operation must be specified.")
  }

  // TODO(@ple13): Move this function to a separate Origin-specific package.
  fun isCumulative(tag: String): Boolean {
    return tag.contains("cumulative=true")
  }

  // TODO(@ple13): Move this function to a separate Origin-specific package.
  fun getTargets(tag: String): List<String> {
    val parts = tag.split(", ")
    val targetPart = parts.find { it.startsWith("target=") }
    return targetPart?.let { it.substringAfter("target=").split(",") }
      ?: error("There must be at least one target.")
  }
}

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

  val measurementPoliciesByReportingSet =
    reportingMetricEntriesList.associate { entry ->
      val reportingSetId = entry.key
      val reportingSet = reportingSetById.getValue(reportingSetId)
      reportingSetId to
        ReportingSetSummary(
          reportingSet.measurementPolicy.lowercase(),
          reportingSet.targetList,
        )
    }

  println(measurementPoliciesByReportingSet)


  val setOperationByMetricCalculationSpec =
    metricCalculationSpecs.associate { specId ->
      val metricCalculationSpec = metricCalculationSpecById.getValue(specId)
      specId to
        SetOperationSummary(
          metricCalculationSpec.cumulative,
          metricCalculationSpec.setOperation,
        )
    }

  val filterGroupByMetricCalculationSpec =
    metricCalculationSpecs.associate { spec ->
      val tag = tags.getValue(spec)
      spec to tag.split(", ").find { it.startsWith("common_filter=") }
    }

  val filterGroups = filterGroupByMetricCalculationSpec.values.toSet()

  // Groups results by (reporting set x metric calculation spec).
  val measurementSets =
    metricCalculationResultsList.groupBy { Pair(it.metricCalculationSpec, it.reportingSet) }

  val reportSummaries = mutableListOf<ReportSummary>()
  for (filter in filterGroups) {
    val reportSummary = reportSummary {
      measurementSets.forEach { (key, value) ->
        if (filterGroupByMetricCalculationSpec.getValue(key.first) == filter) {
          measurementDetails += measurementDetail {
            measurementPolicy = measurementPoliciesByReportingSet[key.second]!!.measurementPolicy
            dataProviders += measurementPoliciesByReportingSet[key.second]!!.dataProviders
            isCumulative = setOperationByMetricCalculationSpec[key.first]!!.isCumulative
            setOperation = setOperationByMetricCalculationSpec[key.first]!!.setOperation
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
