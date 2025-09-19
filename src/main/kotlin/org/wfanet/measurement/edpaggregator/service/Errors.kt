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

package org.wfanet.measurement.edpaggregator.service

import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import org.wfanet.measurement.common.grpc.Errors as CommonErrors
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.edpaggregator.service.internal.Errors as InternalErrors

object Errors {
  const val DOMAIN = "access.halo-cmm.org"

  enum class Reason {
    IMPRESSION_METADATA_NOT_FOUND,
    IMPRESSION_METADATA_ALREADY_EXISTS,
    REQUIRED_FIELD_NOT_SET,
    INVALID_FIELD_VALUE,
    ETAG_MISMATCH,
  }

  enum class Metadata(val key: String) {
    DATA_PROVIDER("dataProvider"),
    FIELD_NAME("fieldName"),
    IMPRESSION_METADATA("impressionMetadata"),
    ETAG("etag"),
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

class RequiredFieldNotSetException(fieldName: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.REQUIRED_FIELD_NOT_SET,
    "Required field $fieldName not set",
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

class ImpressionMetadataNotFoundException(
  dataProviderResourceName: String,
  impressionMetadataResourceName: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.IMPRESSION_METADATA_NOT_FOUND,
    "ImpressionMetadata with Impression Metadata Resource Name $impressionMetadataResourceName for DataProvider with resource Name $dataProviderResourceName not found",
    mapOf(
      Errors.Metadata.DATA_PROVIDER to dataProviderResourceName,
      Errors.Metadata.IMPRESSION_METADATA to impressionMetadataResourceName,
    ),
    cause,
  )

class ImpressionMetadataAlreadyExistsException(name: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
    "ImpressionMetadata $name already exists",
    mapOf(Errors.Metadata.IMPRESSION_METADATA to name),
    cause,
  )
