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

object Errors {
  const val DOMAIN = "internal.reporting.halo-cmm.org"

  enum class Reason {
    MEASUREMENT_CONSUMER_NOT_FOUND,
    BASIC_REPORT_NOT_FOUND,
    BASIC_REPORT_ALREADY_EXISTS,
    REQUIRED_FIELD_NOT_SET,
    IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND,
    INVALID_FIELD_VALUE,
  }

  enum class Metadata(val key: String) {
    CMMS_MEASUREMENT_CONSUMER_ID("cmmsMeasurementConsumerId"),
    EXTERNAL_BASIC_REPORT_ID("externalBasicReportId"),
    FIELD_NAME("fieldName"),
    IMPRESSION_QUALIFICATION_FILTER_ID("impressionQualificationFilterId");

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
    require(errorInfo.domain == DOMAIN) { "Error domain is not ${DOMAIN}" }
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

class RequiredFieldNotSetException(fieldName: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.REQUIRED_FIELD_NOT_SET,
    "$fieldName not set",
    mapOf(Errors.Metadata.FIELD_NAME to fieldName),
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
