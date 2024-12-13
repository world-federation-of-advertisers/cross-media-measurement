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

// TODO(@ple13): Declare protobuf messages for reporting set and metric calculation spec and parse
// them as JSON objects.
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

  fun convertJsontoReportSummaries(reportAsJsonString: String): List<ReportSummary> {
    return getReportFromJsonString(reportAsJsonString).toReportSummaries()
  }

  // TODO(@ple13): Move this function to a separate Origin-specific package.
  fun getMeasurementPolicy(tag: String): String {
    when {
      "measurement_policy=AMI" in tag -> return "ami"
      "measurement_policy=MRC" in tag -> return "mrc"
      "measurement_policy=CUSTOM" in tag -> return "custom"
      else -> error("Measurement policy must be ami, or mrc, or custom, but get $tag.")
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

  // TODO(@ple13): Move this function to a separate Origin-specific package.
  fun getUniqueReachTarget(tag: String): String {
    val parts = tag.split(", ")
    val uniqueReachTargetPart = parts.find { it.startsWith("unique_Reach_Target=") }
    return uniqueReachTargetPart?.substringAfter("unique_Reach_Target=") ?: ""
  }
}

// TODO(@ple13): Move this function to a separate Origin-specific package.
fun Report.toReportSummaries(): List<ReportSummary> {
  require(state == Report.State.SUCCEEDED) { "Unsucceeded report is not supported." }

  val measurementPoliciesByReportingSet =
    reportingMetricEntriesList.associate { entry ->
      val reportingSet = entry.key
      val tag = tags.getValue(reportingSet).replace("{", "").replace("}", "")
      reportingSet to
        ReportingSetSummary(
          ReportConversion.getMeasurementPolicy(tag),
          ReportConversion.getTargets(tag),
        )
    }

  val uniqueReachTargetByReportingSet =
    reportingMetricEntriesList.associate { entry ->
      val reportingSet = entry.key
      val tag = tags.getValue(reportingSet)
      reportingSet to ReportConversion.getUniqueReachTarget(tag)
    }

  val metricCalculationSpecs =
    reportingMetricEntriesList.flatMapTo(mutableSetOf()) { it.value.metricCalculationSpecsList }

  val setOperationByMetricCalculationSpec =
    metricCalculationSpecs.associate { spec ->
      val tag = tags.getValue(spec).replace("{", "").replace("}", "")
      spec to
        SetOperationSummary(
          ReportConversion.isCumulative(tag),
          ReportConversion.getSetOperation(tag),
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
            uniqueReachTarget = uniqueReachTargetByReportingSet[key.second]!!
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
