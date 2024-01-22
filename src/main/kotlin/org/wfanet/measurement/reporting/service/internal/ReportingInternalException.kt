// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.internal

import com.google.protobuf.Any
import com.google.rpc.errorInfo
import com.google.rpc.status
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.wfanet.measurement.internal.reporting.ErrorCode

/** TODO(tristanvuong2021): Add context when each of these exceptions are thrown. */
sealed class ReportingInternalException : Exception {
  val code: ErrorCode
  protected abstract val context: Map<String, String>

  constructor(code: ErrorCode) : super() {
    this.code = code
  }

  constructor(code: ErrorCode, buildMessage: () -> String) : super(buildMessage()) {
    this.code = code
  }

  fun asStatusRuntimeException(
    statusCode: Status.Code,
    message: String = this.message!!,
  ): StatusRuntimeException {
    val statusProto = status {
      code = statusCode.value()
      this.message = message
      details +=
        Any.pack(
          errorInfo {
            reason = this@ReportingInternalException.code.toString()
            domain = ErrorCode.getDescriptor().fullName
            metadata.putAll(context)
          }
        )
    }
    return StatusProto.toStatusRuntimeException(statusProto)
  }
}

class ReportingSetAlreadyExistsException(
  provideDescription: () -> String = { "Reporting Set already exists" }
) : ReportingInternalException(ErrorCode.REPORTING_SET_ALREADY_EXISTS, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class MetricAlreadyExistsException(provideDescription: () -> String = { "Metric already exists" }) :
  ReportingInternalException(ErrorCode.METRIC_ALREADY_EXISTS, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class ReportAlreadyExistsException(provideDescription: () -> String = { "Report already exists" }) :
  ReportingInternalException(ErrorCode.REPORT_ALREADY_EXISTS, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class MeasurementAlreadyExistsException(
  provideDescription: () -> String = { "Measurement already exists" }
) : ReportingInternalException(ErrorCode.MEASUREMENT_ALREADY_EXISTS, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class MeasurementNotFoundException(provideDescription: () -> String = { "Measurement not found" }) :
  ReportingInternalException(ErrorCode.MEASUREMENT_NOT_FOUND, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class ReportingSetNotFoundException(
  provideDescription: () -> String = { "Reporting Set not found" }
) : ReportingInternalException(ErrorCode.REPORTING_SET_NOT_FOUND, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class MeasurementCalculationTimeIntervalNotFoundException(
  provideDescription: () -> String = { "Measurement Calculation Time Interval not found" }
) :
  ReportingInternalException(
    ErrorCode.MEASUREMENT_CALCULATION_TIME_INTERVAL_NOT_FOUND,
    provideDescription,
  ) {
  override val context
    get() = emptyMap<String, String>()
}

class ReportNotFoundException(provideDescription: () -> String = { "Report not found" }) :
  ReportingInternalException(ErrorCode.REPORT_NOT_FOUND, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class MeasurementStateInvalidException(
  provideDescription: () -> String = { "Measurement state invalid" }
) : ReportingInternalException(ErrorCode.MEASUREMENT_STATE_INVALID, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class MeasurementConsumerNotFoundException(
  provideDescription: () -> String = { "Measurement Consumer not found" }
) : ReportingInternalException(ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class MeasurementConsumerAlreadyExistsException(
  provideDescription: () -> String = { "Measurement Consumer already exists" }
) : ReportingInternalException(ErrorCode.MEASUREMENT_CONSUMER_ALREADY_EXISTS, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class MetricNotFoundException(provideDescription: () -> String = { "Metric not found" }) :
  ReportingInternalException(ErrorCode.METRIC_NOT_FOUND, provideDescription) {
  override val context: Map<String, String>
    get() = emptyMap<String, String>()
}

class MetricCalculationSpecAlreadyExistsException(
  private val cmmsMeasurementConsumerId: String,
  private val externalMetricCalculationSpecId: String,
  provideDescription: () -> String = { "Metric Calculation Spec already exists" },
) :
  ReportingInternalException(ErrorCode.METRIC_CALCULATION_SPEC_ALREADY_EXISTS, provideDescription) {
  override val context: Map<String, String>
    get() =
      mapOf(
        "cmms_measurement_consumer_id" to cmmsMeasurementConsumerId,
        "external_metric_calculation_spec_id" to externalMetricCalculationSpecId,
      )
}

class MetricCalculationSpecNotFoundException(
  private val cmmsMeasurementConsumerId: String,
  private val externalMetricCalculationSpecId: String,
  provideDescription: () -> String = { "Metric Calculation Spec not found" },
) : ReportingInternalException(ErrorCode.METRIC_CALCULATION_SPEC_NOT_FOUND, provideDescription) {
  override val context: Map<String, String>
    get() =
      mapOf(
        "cmms_measurement_consumer_id" to cmmsMeasurementConsumerId,
        "external_metric_calculation_spec_id" to externalMetricCalculationSpecId,
      )
}

class ReportScheduleAlreadyExistsException(
  private val cmmsMeasurementConsumerId: String,
  private val externalReportScheduleId: String,
  provideDescription: () -> String = { "Report Schedule already exists" },
) : ReportingInternalException(ErrorCode.REPORT_SCHEDULE_ALREADY_EXISTS, provideDescription) {
  override val context: Map<String, String>
    get() =
      mapOf(
        "cmms_measurement_consumer_id" to cmmsMeasurementConsumerId,
        "external_report_schedule_id" to externalReportScheduleId,
      )
}

class ReportScheduleNotFoundException(
  private val cmmsMeasurementConsumerId: String,
  private val externalReportScheduleId: String,
  provideDescription: () -> String = { "Report Schedule not found" },
) : ReportingInternalException(ErrorCode.REPORT_SCHEDULE_NOT_FOUND, provideDescription) {
  override val context: Map<String, String>
    get() =
      mapOf(
        "cmms_measurement_consumer_id" to cmmsMeasurementConsumerId,
        "external_report_schedule_id" to externalReportScheduleId,
      )
}

class ReportScheduleStateInvalidException(
  private val cmmsMeasurementConsumerId: String,
  private val externalReportScheduleId: String,
  provideDescription: () -> String = { "Report Schedule state invalid" },
) : ReportingInternalException(ErrorCode.REPORT_SCHEDULE_STATE_INVALID, provideDescription) {
  override val context: Map<String, String>
    get() =
      mapOf(
        "cmms_measurement_consumer_id" to cmmsMeasurementConsumerId,
        "external_report_schedule_id" to externalReportScheduleId,
      )
}

class ReportScheduleIterationNotFoundException(
  private val cmmsMeasurementConsumerId: String,
  private val externalReportScheduleId: String,
  private val externalReportScheduleIterationId: String,
  provideDescription: () -> String = { "Report Schedule Iteration not found" },
) : ReportingInternalException(ErrorCode.REPORT_SCHEDULE_ITERATION_NOT_FOUND, provideDescription) {
  override val context: Map<String, String>
    get() =
      mapOf(
        "cmms_measurement_consumer_id" to cmmsMeasurementConsumerId,
        "external_report_schedule_id" to externalReportScheduleId,
        "external_report_schedule_iteration_id" to externalReportScheduleIterationId,
      )
}

class ReportScheduleIterationStateInvalidException(
  private val cmmsMeasurementConsumerId: String,
  private val externalReportScheduleId: String,
  private val externalReportScheduleIterationId: String,
  provideDescription: () -> String = { "Report Schedule Iteration state invalid" },
) :
  ReportingInternalException(
    ErrorCode.REPORT_SCHEDULE_ITERATION_STATE_INVALID,
    provideDescription,
  ) {
  override val context: Map<String, String>
    get() =
      mapOf(
        "cmms_measurement_consumer_id" to cmmsMeasurementConsumerId,
        "external_report_schedule_id" to externalReportScheduleId,
        "external_report_schedule_iteration_id" to externalReportScheduleIterationId,
      )
}
