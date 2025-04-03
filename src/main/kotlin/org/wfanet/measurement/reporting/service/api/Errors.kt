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

package org.wfanet.measurement.reporting.service.api

import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import org.wfanet.measurement.common.grpc.Errors as CommonErrors
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.reporting.service.internal.Errors as InternalErrors
import org.wfanet.measurement.reporting.v2alpha.Metric

object Errors {
  const val DOMAIN = "reporting.halo-cmm.org"

  enum class Reason {
    BASIC_REPORT_NOT_FOUND,
    METRIC_NOT_FOUND,
    CAMPAIGN_GROUP_INVALID,
    REQUIRED_FIELD_NOT_SET,
    INVALID_FIELD_VALUE,
    INVALID_METRIC_STATE_TRANSITION,
    ARGUMENT_CHANGED_IN_REQUEST_FOR_NEXT_PAGE,
    IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND,
  }

  enum class Metadata(val key: String) {
    BASIC_REPORT("basicReport"),
    METRIC("metric"),
    METRIC_STATE("metricState"),
    NEW_METRIC_STATE("newMetricState"),
    REPORTING_SET("reportingSet"),
    FIELD_NAME("fieldName"),
    IMPRESSION_QUALIFICATION_FILTER("impressionQualificationFilter");

    companion object {
      private val METADATA_BY_KEY by lazy { entries.associateBy { it.key } }

      fun fromKey(key: String): Metadata = METADATA_BY_KEY.getValue(key)
    }
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
    return CommonErrors.buildStatusRuntimeException(code, message, errorInfo, this)
  }

  abstract class Factory<T : ServiceException> {
    protected abstract val reason: Errors.Reason

    protected abstract fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): T

    fun fromInternal(cause: StatusException): T {
      val errorInfo = requireNotNull(cause.errorInfo)
      require(errorInfo.domain == InternalErrors.DOMAIN)
      require(errorInfo.reason == reason.name)
      return fromInternal(InternalErrors.parseMetadata(errorInfo), cause)
    }
  }
}

class BasicReportNotFoundException(name: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.BASIC_REPORT_NOT_FOUND,
    "Basic Report $name not found",
    mapOf(Errors.Metadata.BASIC_REPORT to name),
    cause,
  )

class MetricNotFoundException(name: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.METRIC_NOT_FOUND,
    "Metric $name not found",
    mapOf(Errors.Metadata.METRIC to name),
    cause,
  )

class CampaignGroupInvalidException(reportingSet: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.CAMPAIGN_GROUP_INVALID,
    "$reportingSet is not a valid Campaign Group",
    mapOf(Errors.Metadata.REPORTING_SET to reportingSet),
    cause,
  )

class RequiredFieldNotSetException(fieldName: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.REQUIRED_FIELD_NOT_SET,
    "$fieldName not set",
    mapOf(Errors.Metadata.FIELD_NAME to fieldName),
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

class ArgumentChangedInRequestForNextPageException(
  fieldName: String,
  cause: Throwable? = null,
  buildMessage: (fieldName: String) -> String = {
    "Value for field $fieldName does not match page token."
  },
) :
  ServiceException(
    Errors.Reason.ARGUMENT_CHANGED_IN_REQUEST_FOR_NEXT_PAGE,
    buildMessage(fieldName),
    mapOf(Errors.Metadata.FIELD_NAME to fieldName),
    cause,
  )

class InvalidMetricStateTransitionException(
  name: String,
  metricState: Metric.State,
  newMetricState: Metric.State,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.INVALID_METRIC_STATE_TRANSITION,
    "Metric $name cannot be transitioned from $metricState to $newMetricState",
    mapOf(
      Errors.Metadata.METRIC to name,
      Errors.Metadata.METRIC_STATE to metricState.name,
      Errors.Metadata.NEW_METRIC_STATE to newMetricState.name,
    ),
    cause,
  )

class ImpressionQualificationFilterNotFoundException(name: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND,
    "Impression Qualification Filter $name not found",
    mapOf(Errors.Metadata.IMPRESSION_QUALIFICATION_FILTER to name),
    cause,
  ) {}
