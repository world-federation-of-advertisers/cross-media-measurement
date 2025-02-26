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

import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException

object Errors {
  const val DOMAIN = "internal.reporting-v2.halo-cmm.org"

  enum class Reason {
    BASIC_REPORT_NOT_FOUND,
    BASIC_REPORT_ALREADY_EXISTS,
    REQUIRED_FIELD_NOT_SET,
  }

  enum class Metadata(val key: String) {
    EXTERNAL_BASIC_REPORT_ID("externalBasicReportId"),
    FIELD_NAME("fieldName");

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
    return org.wfanet.measurement.common.grpc.Errors.buildStatusRuntimeException(code, message, errorInfo, this)
  }
}

class BasicReportNotFoundException(externalBasicReportId: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.BASIC_REPORT_NOT_FOUND,
    "Basic Report with external ID $externalBasicReportId not found",
    mapOf(Errors.Metadata.EXTERNAL_BASIC_REPORT_ID to externalBasicReportId),
    cause,
  )

class BasicReportAlreadyExistsException(externalBasicReportId: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.BASIC_REPORT_ALREADY_EXISTS,
    "Basic Report with external ID $externalBasicReportId already exists",
    mapOf(Errors.Metadata.EXTERNAL_BASIC_REPORT_ID to externalBasicReportId),
    cause,
  )

class RequiredFieldNotSetException(fieldName: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.REQUIRED_FIELD_NOT_SET,
    "$fieldName not set",
    mapOf(Errors.Metadata.FIELD_NAME to fieldName),
    cause,
  )
