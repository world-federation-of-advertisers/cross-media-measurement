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

import com.google.rpc.ErrorInfo
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import org.wfanet.measurement.common.grpc.Errors
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.internal.reporting.ErrorCode

/** TODO(tristanvuong2021): Add context when each of these exceptions are thrown. */
sealed class ReportingInternalException : Exception {
  val code: ErrorCode
  protected abstract val context: Map<String, String>
  override val message: String
    get() = super.message!!

  constructor(code: ErrorCode, message: String) : super(message) {
    this.code = code
  }

  constructor(code: ErrorCode, buildMessage: () -> String) : this(code, buildMessage())

  fun asStatusRuntimeException(
    statusCode: Status.Code,
    message: String = this.message,
  ): StatusRuntimeException {
    return Errors.buildStatusRuntimeException(
      statusCode,
      message,
      errorInfo {
        domain = DOMAIN
        reason = this@ReportingInternalException.code.name
        metadata.putAll(context)
      },
    )
  }

  companion object {
    val DOMAIN = ErrorCode.getDescriptor().fullName

    /**
     * Returns the [ErrorCode] extracted from [exception], or `null` if [exception] is not this type
     * of error.
     */
    fun getErrorCode(exception: StatusException): ErrorCode? {
      val errorInfo: ErrorInfo = exception.errorInfo ?: return null
      return getErrorCode(errorInfo)
    }

    /**
     * Returns the [ErrorCode] extracted from [errorInfo], or `null` if [errorInfo] is not this type
     * of error.
     */
    fun getErrorCode(errorInfo: ErrorInfo): ErrorCode? {
      if (errorInfo.domain != DOMAIN) {
        return null
      }
      return ErrorCode.valueOf(errorInfo.reason)
    }
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
  val cmmsMeasurementConsumerId: String,
  val externalReportingSetId: String,
  buildMessage: () -> String = {
    "ReportingSet with external key ($cmmsMeasurementConsumerId, $externalReportingSetId) not found"
  },
) : ReportingInternalException(ErrorCode.REPORTING_SET_NOT_FOUND, buildMessage()) {
  override val context
    get() =
      mapOf(
        "cmmsMeasurementConsumerId" to cmmsMeasurementConsumerId,
        "externalReportingSetId" to externalReportingSetId,
      )
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

class CampaignGroupInvalidException(
  val cmmsMeasurementConsumerId: String,
  val externalReportingSetId: String,
) :
  ReportingInternalException(
    ErrorCode.CAMPAIGN_GROUP_INVALID,
    "ReportingSet with CMMS MeasurementConsumer ID $cmmsMeasurementConsumerId and " +
      "external ReportingSet ID $externalReportingSetId is not a valid Campaign Group",
  ) {
  override val context: Map<String, String>
    get() =
      mapOf(
        "cmmsMeasurementConsumerId" to cmmsMeasurementConsumerId,
        "externalReportingSetId" to externalReportingSetId,
      )
}

class MeasurementStateInvalidException(
  provideDescription: () -> String = { "Measurement state invalid" }
) : ReportingInternalException(ErrorCode.MEASUREMENT_STATE_INVALID, provideDescription) {
  override val context
    get() = emptyMap<String, String>()
}

class MeasurementConsumerAlreadyExistsException(
  provideDescription: () -> String = { "Measurement Consumer already exists" }
) : ReportingInternalException(ErrorCode.MEASUREMENT_CONSUMER_ALREADY_EXISTS, provideDescription) {
  override val context
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
