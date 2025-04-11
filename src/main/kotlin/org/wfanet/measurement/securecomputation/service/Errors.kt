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

package org.wfanet.measurement.securecomputation.service

import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import org.wfanet.measurement.common.grpc.Errors as CommonErrors
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.securecomputation.service.internal.Errors as InternalErrors

object Errors {
  const val DOMAIN = "control-plane.secure-computation.halo-cmm.org"

  enum class Reason {
    REQUIRED_FIELD_NOT_SET,
    INVALID_WORK_ITEM_STATE,
    INVALID_WORK_ITEM_ATTEMPT_STATE,
    WORK_ITEM_NOT_FOUND,
    WORK_ITEM_ATTEMPT_NOT_FOUND,
    WORK_ITEM_ALREADY_EXISTS,
    WORK_ITEM_ATTEMPT_ALREADY_EXISTS,
    INVALID_FIELD_VALUE,
  }

  enum class Metadata(val key: String) {
    WORK_ITEM("workItem"),
    WORK_ITEM_ATTEMPT("workItem"),
    WORK_ITEM_ATTEMPT_STATE("workItemAttemptState"),
    WORK_ITEM_STATE("workItemState"),
    FIELD_NAME("fieldName"),
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

class WorkItemNotFoundException(name: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.WORK_ITEM_NOT_FOUND,
    "WorkItem $name not found",
    mapOf(Errors.Metadata.WORK_ITEM to name),
    cause,
  )

class WorkItemAttemptNotFoundException(name: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.WORK_ITEM_ATTEMPT_NOT_FOUND,
    "WorkItemAttempt $name not found",
    mapOf(Errors.Metadata.WORK_ITEM_ATTEMPT to name),
    cause,
  )

class WorkItemAlreadyExistsException(name: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.WORK_ITEM_ALREADY_EXISTS,
    "WorkItem $name already exists",
    mapOf(Errors.Metadata.WORK_ITEM to name),
    cause,
  )

class WorkItemAttemptAlreadyExistsException(name: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.WORK_ITEM_ATTEMPT_ALREADY_EXISTS,
    "WorkItemAttempt $name already exists",
    mapOf(Errors.Metadata.WORK_ITEM_ATTEMPT to name),
    cause,
  )

class WorkItemInvalidStateException(name: String, workItemState: String, cause: Throwable? = null) :
  ServiceException(
    reason,
    "WorkItem $name is in an invalid state for this operation",
    mapOf(Errors.Metadata.WORK_ITEM to name, Errors.Metadata.WORK_ITEM_STATE to workItemState),
    cause,
  ) {
  companion object : Factory<WorkItemInvalidStateException>() {
    override val reason: Errors.Reason
      get() = Errors.Reason.INVALID_WORK_ITEM_STATE

    override fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): WorkItemInvalidStateException {

      val workItemKey =
        WorkItemKey(internalMetadata.getValue(InternalErrors.Metadata.WORK_ITEM_RESOURCE_ID))
      return WorkItemInvalidStateException(
        workItemKey.toName(),
        internalMetadata.getValue(InternalErrors.Metadata.WORK_ITEM_STATE),
      )
    }
  }
}

class WorkItemAttemptInvalidStateException(
  name: String,
  workItemAttemptState: String,
  cause: Throwable? = null,
) :
  ServiceException(
    reason,
    "WorkItemAttempt $name is in an invalid state for this operation",
    mapOf(
      Errors.Metadata.WORK_ITEM_ATTEMPT to name,
      Errors.Metadata.WORK_ITEM_ATTEMPT_STATE to workItemAttemptState,
    ),
    cause,
  ) {
  companion object : Factory<WorkItemAttemptInvalidStateException>() {
    override val reason: Errors.Reason
      get() = Errors.Reason.INVALID_WORK_ITEM_ATTEMPT_STATE

    override fun fromInternal(
      internalMetadata: Map<InternalErrors.Metadata, String>,
      cause: Throwable,
    ): WorkItemAttemptInvalidStateException {

      val workItemAttemptKey =
        WorkItemAttemptKey(
          internalMetadata.getValue(InternalErrors.Metadata.WORK_ITEM_RESOURCE_ID),
          internalMetadata.getValue(InternalErrors.Metadata.WORK_ITEM_ATTEMPT_RESOURCE_ID),
        )
      return WorkItemAttemptInvalidStateException(
        workItemAttemptKey.toName(),
        internalMetadata.getValue(InternalErrors.Metadata.WORK_ITEM_ATTEMPT_STATE),
      )
    }
  }
}
