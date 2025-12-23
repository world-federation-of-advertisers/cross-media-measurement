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

package org.wfanet.measurement.reporting.service.internal

import com.google.rpc.ErrorInfo
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.Metric
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult
import org.wfanet.measurement.internal.reporting.v2.nonCumulativeStartOrNull

object Errors {
  const val DOMAIN = "internal.reporting.halo-cmm.org"

  enum class Reason {
    MEASUREMENT_CONSUMER_NOT_FOUND,
    BASIC_REPORT_NOT_FOUND,
    METRIC_NOT_FOUND,
    BASIC_REPORT_ALREADY_EXISTS,
    REQUIRED_FIELD_NOT_SET,
    IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND,
    INVALID_METRIC_STATE_TRANSITION,
    INVALID_FIELD_VALUE,
    REPORT_RESULT_NOT_FOUND,
    REPORTING_SET_RESULT_NOT_FOUND,
    REPORTING_WINDOW_RESULT_NOT_FOUND,
    BASIC_REPORT_STATE_INVALID,
    INVALID_BASIC_REPORT,
  }

  enum class Metadata(val key: String) {
    CMMS_MEASUREMENT_CONSUMER_ID("cmmsMeasurementConsumerId"),
    EXTERNAL_BASIC_REPORT_ID("externalBasicReportId"),
    IMPRESSION_QUALIFICATION_FILTER_ID("impressionQualificationFilterId"),
    EXTERNAL_METRIC_ID("externalMetricId"),
    METRIC_STATE("metricState"),
    NEW_METRIC_STATE("newMetricState"),
    FIELD_NAME("fieldName"),
    EXTERNAL_REPORT_RESULT_ID("externalReportResultId"),
    EXTERNAL_REPORTING_SET_RESULT_ID("externalReportingSetResultId"),
    REPORTING_WINDOW_NON_CUMULATIVE_START("reportingWindowNonCumulativeStart"),
    REPORTING_WINDOW_END("reportingWindowEnd"),
    BASIC_REPORT_STATE("basicReportState");

    companion object {
      private val METADATA_BY_KEY by lazy { entries.associateBy { it.key } }

      fun fromKey(key: String): Metadata = METADATA_BY_KEY.getValue(key)
    }
  }

  /**
   * Returns the [Reason] extracted from [exception], or `null` if [exception] is not this type of
   * error.
   */
  fun getReason(exception: StatusException): Reason? {
    val errorInfo = exception.errorInfo ?: return null
    return getReason(errorInfo)
  }

  /**
   * Returns the [Reason] extracted from [errorInfo], or `null` if [errorInfo] is not this type of
   * error.
   */
  fun getReason(errorInfo: ErrorInfo): Reason? {
    if (errorInfo.domain != DOMAIN) {
      return null
    }

    return Reason.valueOf(errorInfo.reason)
  }

  fun parseMetadata(errorInfo: ErrorInfo): Map<Metadata, String> {
    require(errorInfo.domain == DOMAIN) { "Error domain is not $DOMAIN" }
    return errorInfo.metadataMap.mapKeys { Metadata.fromKey(it.key) }
  }
}

sealed class ServiceException(
  private val reason: Errors.Reason,
  message: String,
  private val metadata: Map<Errors.Metadata, String>,
  cause: Throwable?,
) : Exception(message, cause) {
  override val message: String
    get() = super.message!!

  fun asStatusRuntimeException(code: Status.Code): StatusRuntimeException {
    val source = this
    val errorInfo = errorInfo {
      domain = Errors.DOMAIN
      reason = source.reason.name
      metadata.putAll(source.metadata.mapKeys { it.key.key })
    }
    return org.wfanet.measurement.common.grpc.Errors.buildStatusRuntimeException(
      code,
      message,
      errorInfo,
      this,
    )
  }
}

class MeasurementConsumerNotFoundException(
  cmmsMeasurementConsumerId: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.MEASUREMENT_CONSUMER_NOT_FOUND,
    "Measurement Consumer with cmms ID $cmmsMeasurementConsumerId not found",
    mapOf(Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID to cmmsMeasurementConsumerId),
    cause,
  )

class BasicReportNotFoundException(
  cmmsMeasurementConsumerId: String,
  externalBasicReportId: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.BASIC_REPORT_NOT_FOUND,
    "Basic Report with cmms measurement consumer ID $cmmsMeasurementConsumerId and external ID $externalBasicReportId not found",
    mapOf(
      Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID to cmmsMeasurementConsumerId,
      Errors.Metadata.EXTERNAL_BASIC_REPORT_ID to externalBasicReportId,
    ),
    cause,
  )

class BasicReportAlreadyExistsException(
  cmmsMeasurementConsumerId: String,
  externalBasicReportId: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.BASIC_REPORT_ALREADY_EXISTS,
    "Basic Report with cmms measurement consumer ID $cmmsMeasurementConsumerId and external ID $externalBasicReportId already exists",
    mapOf(
      Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID to cmmsMeasurementConsumerId,
      Errors.Metadata.EXTERNAL_BASIC_REPORT_ID to externalBasicReportId,
    ),
    cause,
  )

class BasicReportStateInvalidException(
  cmmsMeasurementConsumerId: String,
  externalBasicReportId: String,
  state: BasicReport.State,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.BASIC_REPORT_STATE_INVALID,
    "BasicReport with external key ($cmmsMeasurementConsumerId, $externalBasicReportId) is in state ${state.name} which is invalid for the operation",
    mapOf(
      Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID to cmmsMeasurementConsumerId,
      Errors.Metadata.EXTERNAL_BASIC_REPORT_ID to externalBasicReportId,
      Errors.Metadata.BASIC_REPORT_STATE to state.name,
    ),
    cause,
  )

class ReportResultNotFoundException(
  cmmsMeasurementConsumerId: String,
  externalReportResultId: Long,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.REPORT_RESULT_NOT_FOUND,
    "ReportResult with CMMS measurement consumer ID $cmmsMeasurementConsumerId and external ID $externalReportResultId not found",
    mapOf(
      Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID to cmmsMeasurementConsumerId,
      Errors.Metadata.EXTERNAL_REPORT_RESULT_ID to externalReportResultId.toString(),
    ),
    cause,
  )

class ReportingSetResultNotFoundException(
  cmmsMeasurementConsumerId: String,
  externalReportResultId: Long,
  externalReportingSetResultId: Long,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.REPORTING_SET_RESULT_NOT_FOUND,
    "ReportingSetResult with external key ($cmmsMeasurementConsumerId, $externalReportResultId, " +
      "$externalReportingSetResultId) not found",
    mapOf(
      Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID to cmmsMeasurementConsumerId,
      Errors.Metadata.EXTERNAL_REPORT_RESULT_ID to externalReportResultId.toString(),
      Errors.Metadata.EXTERNAL_REPORTING_SET_RESULT_ID to externalReportingSetResultId.toString(),
    ),
    cause,
  )

class ReportingWindowResultNotFoundException
private constructor(
  cmmsMeasurementConsumerId: String,
  externalReportResultId: String,
  externalReportingSetResultId: String,
  reportingWindowNonCumulativeStart: String?,
  reportingWindowEnd: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.REPORTING_WINDOW_RESULT_NOT_FOUND,
    "ReportingWindow with external key ($cmmsMeasurementConsumerId, $externalReportResultId, " +
      "$externalReportingSetResultId, $reportingWindowNonCumulativeStart, $reportingWindowEnd) " +
      "not found",
    buildMap {
      put(Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID, cmmsMeasurementConsumerId)
      put(Errors.Metadata.EXTERNAL_REPORT_RESULT_ID, externalReportResultId)
      put(Errors.Metadata.EXTERNAL_REPORTING_SET_RESULT_ID, externalReportingSetResultId)
      if (reportingWindowNonCumulativeStart != null) {
        put(
          Errors.Metadata.REPORTING_WINDOW_NON_CUMULATIVE_START,
          reportingWindowNonCumulativeStart,
        )
      }
      put(Errors.Metadata.REPORTING_WINDOW_END, reportingWindowEnd)
    },
    cause,
  ) {
  constructor(
    cmmsMeasurementConsumerId: String,
    externalReportResultId: Long,
    externalReportingSetResultId: Long,
    reportingWindow: ReportingSetResult.ReportingWindow,
    cause: Throwable? = null,
  ) : this(
    cmmsMeasurementConsumerId,
    externalReportResultId.toString(),
    externalReportingSetResultId.toString(),
    reportingWindow.nonCumulativeStartOrNull?.toLocalDate()?.toString(),
    reportingWindow.end.toLocalDate().toString(),
  )
}

class RequiredFieldNotSetException(
  fieldPath: String,
  cause: Throwable? = null,
  buildMessage: (fieldPath: String) -> String = { "$fieldPath not set" },
) :
  ServiceException(
    Errors.Reason.REQUIRED_FIELD_NOT_SET,
    buildMessage(fieldPath),
    mapOf(Errors.Metadata.FIELD_NAME to fieldPath),
    cause,
  )

class ImpressionQualificationFilterNotFoundException(
  impressionQualificationFilterId: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND,
    "Impression Qualification Filter with ID $impressionQualificationFilterId not found",
    mapOf(Errors.Metadata.IMPRESSION_QUALIFICATION_FILTER_ID to impressionQualificationFilterId),
    cause,
  )

class InvalidFieldValueException(
  fieldName: String,
  cause: Throwable? = null,
  buildMessage: (fieldName: String) -> String = { "Invalid value for field $fieldName" },
) :
  ServiceException(
    Errors.Reason.INVALID_FIELD_VALUE,
    buildMessage(fieldName),
    mapOf(Errors.Metadata.FIELD_NAME to fieldName),
    cause,
  )

class MetricNotFoundException(
  cmmsMeasurementConsumerId: String,
  externalMetricId: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.METRIC_NOT_FOUND,
    "Metric with cmms measurement consumer ID $cmmsMeasurementConsumerId and external ID $externalMetricId not found",
    mapOf(
      Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID to cmmsMeasurementConsumerId,
      Errors.Metadata.EXTERNAL_METRIC_ID to externalMetricId,
    ),
    cause,
  )

class InvalidMetricStateTransitionException(
  cmmsMeasurementConsumerId: String,
  externalMetricId: String,
  metricState: Metric.State,
  newMetricState: Metric.State,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.INVALID_METRIC_STATE_TRANSITION,
    """
      Metric with cmms measurement consumer ID $cmmsMeasurementConsumerId and external ID
      $externalMetricId cannot be transitioned from $metricState to $newMetricState
    """,
    mapOf(
      Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID to cmmsMeasurementConsumerId,
      Errors.Metadata.EXTERNAL_METRIC_ID to externalMetricId,
      Errors.Metadata.METRIC_STATE to metricState.name,
      Errors.Metadata.NEW_METRIC_STATE to newMetricState.name,
    ),
    cause,
  )

class InvalidBasicReportException(
  cmmsMeasurementConsumerId: String,
  externalBasicReportId: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.INVALID_BASIC_REPORT,
    "BasicReport with external key ($cmmsMeasurementConsumerId, $externalBasicReportId) is invalid",
    mapOf(
      Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID to cmmsMeasurementConsumerId,
      Errors.Metadata.EXTERNAL_BASIC_REPORT_ID to externalBasicReportId,
    ),
    cause,
  )
