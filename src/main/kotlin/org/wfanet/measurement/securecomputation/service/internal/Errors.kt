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

package org.wfanet.measurement.securecomputation.service.internal

import com.google.rpc.ErrorInfo
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import org.wfanet.measurement.common.grpc.Errors as CommonErrors
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt

object Errors {
  const val DOMAIN = "internal.control-plane.secure-computation.halo-cmm.org"

  enum class Reason {
    REQUIRED_FIELD_NOT_SET,
    QUEUE_NOT_FOUND,
    QUEUE_NOT_FOUND_FOR_WORK_ITEM,
    INVALID_WORK_ITEM_STATE,
    INVALID_WORK_ITEM_ATTEMPT_STATE,
    WORK_ITEM_NOT_FOUND,
    WORK_ITEM_ATTEMPT_NOT_FOUND,
    WORK_ITEM_ALREADY_EXISTS,
    WORK_ITEM_ATTEMPT_ALREADY_EXISTS,
    INVALID_FIELD_VALUE,
  }

  enum class Metadata(val key: String) {
    QUEUE_RESOURCE_ID("queueResourceId"),
    QUEUE_ID("queueId"),
    WORK_ITEM_RESOURCE_ID("workItemResourceId"),
    WORK_ITEM_STATE("workItemState"),
    WORK_ITEM_ATTEMPT_RESOURCE_ID("workItemAttemptResourceId"),
    WORK_ITEM_ATTEMPT_STATE("workItemAttemptState"),
    FIELD_NAME("fieldName");

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
    return CommonErrors.buildStatusRuntimeException(code, message, errorInfo, this)
  }
}

class RequiredFieldNotSetException(fieldName: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.REQUIRED_FIELD_NOT_SET,
    "$fieldName not set",
    mapOf(Errors.Metadata.FIELD_NAME to fieldName),
    cause,
  )

class QueueNotFoundException(queueResourceId: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.QUEUE_NOT_FOUND,
    "Queue with resource ID $queueResourceId not found",
    mapOf(Errors.Metadata.QUEUE_RESOURCE_ID to queueResourceId),
    cause,
  )

class QueueNotFoundForWorkItem(workItemResourceId: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.QUEUE_NOT_FOUND_FOR_WORK_ITEM,
    "Queue for WorkItem with ID $workItemResourceId not found",
    mapOf(Errors.Metadata.WORK_ITEM_RESOURCE_ID to workItemResourceId),
    cause,
  )

class WorkItemNotFoundException(workItemResourceId: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.WORK_ITEM_NOT_FOUND,
    "WorkItem with resource ID $workItemResourceId not found",
    mapOf(Errors.Metadata.WORK_ITEM_RESOURCE_ID to workItemResourceId),
    cause,
  )

class WorkItemAttemptNotFoundException(
  workItemResourceId: String,
  workItemAttemptResourceId: String,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.WORK_ITEM_ATTEMPT_NOT_FOUND,
    "WorkItemAttempt with workItemResource ID $workItemResourceId and workItemAttemptResource ID $workItemAttemptResourceId not found",
    mapOf(
      Errors.Metadata.WORK_ITEM_RESOURCE_ID to workItemResourceId,
      Errors.Metadata.WORK_ITEM_ATTEMPT_RESOURCE_ID to workItemAttemptResourceId,
    ),
    cause,
  )

class WorkItemAlreadyExistsException(cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.WORK_ITEM_ALREADY_EXISTS,
    "WorkItem already exists",
    emptyMap(),
    cause,
  )

class WorkItemAttemptAlreadyExistsException(cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.WORK_ITEM_ATTEMPT_ALREADY_EXISTS,
    "WorkItemAttempt already exists",
    emptyMap(),
    cause,
  )

class WorkItemInvalidStateException(
  workItemResourceId: String,
  workItemState: WorkItem.State,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.INVALID_WORK_ITEM_STATE,
    "WorkItem with resource ID $workItemResourceId is in an invalid state for this operation",
    mapOf(
      Errors.Metadata.WORK_ITEM_RESOURCE_ID to workItemResourceId,
      Errors.Metadata.WORK_ITEM_STATE to workItemState.name,
    ),
    cause,
  )

class WorkItemAttemptInvalidStateException(
  workItemResourceId: String,
  workItemAttemptResourceId: String,
  workItemAttemptState: WorkItemAttempt.State,
  cause: Throwable? = null,
) :
  ServiceException(
    Errors.Reason.INVALID_WORK_ITEM_ATTEMPT_STATE,
    "WorkItemAttempt with resource ID $workItemAttemptResourceId is in an invalid state for this operation",
    mapOf(
      Errors.Metadata.WORK_ITEM_RESOURCE_ID to workItemResourceId,
      Errors.Metadata.WORK_ITEM_ATTEMPT_RESOURCE_ID to workItemAttemptResourceId,
      Errors.Metadata.WORK_ITEM_ATTEMPT_STATE to workItemAttemptState.name,
    ),
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
