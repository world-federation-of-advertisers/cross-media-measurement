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

package experimental.dp_consistency.src.main.kotlin.tools

import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.util.JsonFormat
import experimental.dp_consistency.src.main.proto.reporting.MeasurementDetailKt
import experimental.dp_consistency.src.main.proto.reporting.ReportSummary
import experimental.dp_consistency.src.main.proto.reporting.measurementDetail
import experimental.dp_consistency.src.main.proto.reporting.reportSummary
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.report

data class ReportingSetSummary(val measurementPolicy: String, val dataProviders: List<String>)

data class SetOperationSummary(val isCumulative: Boolean, val setOperation: String)

fun getReportFromJsonString(reportAsJsonString: String): Report {
  val report =
    try {
      val protoBuilder = Report.newBuilder()
      JsonFormat.parser().ignoringUnknownFields().merge(reportAsJsonString, protoBuilder)
      protoBuilder.build()
    } catch (e: InvalidProtocolBufferException) {
      Report.getDefaultInstance()
    }
  return report
}

fun convertJsonToReportSummary(reportAsJsonString: String): ReportSummary {
  val report =
    try {
      val protoBuilder = Report.newBuilder()
      JsonFormat.parser().ignoringUnknownFields().merge(reportAsJsonString, protoBuilder)
      protoBuilder.build()
    } catch (e: InvalidProtocolBufferException) {
      Report.getDefaultInstance()
    }

  return report.toReportSummary()
}

fun getMeasurementPolicy(tag: String): String {
  when {
    "measurement_policy=AMI" in tag -> return "ami"
    "measurement_policy=MRC" in tag -> return "mrc"
    "measurement_policy=CUSTOM" in tag -> return "custom"
    else -> error("Measurement policy not specified in the tag.")
  }
}

fun getSetOperation(tag: String): String {
  val parts = tag.split(", ")
  val setOperationPart = parts.find { it.startsWith("set_operation=") }
  return setOperationPart?.let { it.substringAfter("set_operation=") }
    ?: error("Set operation must be specified.")
}

fun isCumulative(tag: String): Boolean {
  return tag.contains("cumulative=true")
}

fun getTargets(tag: String): List<String> {
  val parts = tag.split(", ")
  val targetPart = parts.find { it.startsWith("target=") }
  return targetPart?.let { it.substringAfter("target=").split(",") }
    ?: error("There must be at least one target.")
}

fun Report.toReportSummary(): ReportSummary {
  require(this.state == Report.State.SUCCEEDED) { "Unsucceeded report is not supported." }
  val measurementPoliciesByReportingSet =
    this.reportingMetricEntriesList.associate { entry ->
      val reportingSet = entry.key
      val tag = this.tags.getValue(reportingSet)
      reportingSet to ReportingSetSummary(getMeasurementPolicy(tag), getTargets(tag))
    }

  val metricCalculationSpecs =
    this.reportingMetricEntriesList.flatMapTo(mutableSetOf()) {
      it.value.metricCalculationSpecsList
    }

  val setOperationByMetricCalculationSpec =
    metricCalculationSpecs.associate { spec ->
      val tag = this.tags.getValue(spec)
      spec to SetOperationSummary(isCumulative(tag), getSetOperation(tag))
    }

  // Groups measurements by metic calculation spec.
  val metricCalculationResults =
    this.metricCalculationResultsList.groupBy { it.metricCalculationSpec }

  // Groups results by (reporting set x metric calculation spec).
  val measurementSets =
    this.metricCalculationResultsList.groupBy { Pair(it.metricCalculationSpec, it.reportingSet) }

  return reportSummary {
    measurementSets.forEach { (key, value) ->
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
