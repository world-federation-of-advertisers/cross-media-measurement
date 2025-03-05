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
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.securecomputation.service.internal.Errors as InternalErrors

object Errors {
  const val DOMAIN = "securecomputation.halo-cmm.org"

  enum class Reason {
    REQUIRED_FIELD_NOT_SET,
    QUEUE_NOT_FOUND,
    QUEUE_NOT_FOUND_FOR_INTERNAL_ID,
    INVALID_WORK_ITEM_PRECONDITION_STATE,
    WORK_ITEM_NOT_FOUND,
    WORK_ITEM_ATTEMPT_NOT_FOUND,
    WORK_ITEM_ALREADY_EXISTS,
    WORK_ITEM_ATTEMPT_ALREADY_EXISTS,
    INVALID_FIELD_VALUE,
  }

  enum class Metadata(val key: String) {
    QUEUE_RESOURCE_ID("queueResourceId"),
    QUEUE_ID("queueId"),
    WORK_ITEM("workItem"),
    WORK_ITEM_ATTEMPT("workItem"),
    FIELD_NAME("fieldName");
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

//class QueueNotFoundException(queueResourceId: String, cause: Throwable? = null) :
//  ServiceException(
//    Errors.Reason.QUEUE_NOT_FOUND,
//    "Queue with resource ID $queueResourceId not found",
//    mapOf(Errors.Metadata.QUEUE_RESOURCE_ID to queueResourceId),
//    cause,
//  )
//
//class QueueNotFoundForInternalIdException(queueId: Long, cause: Throwable? = null) :
//  ServiceException(
//    Errors.Reason.QUEUE_NOT_FOUND_FOR_INTERNAL_ID,
//    "Queue with ID $queueId not found",
//    mapOf(Errors.Metadata.QUEUE_ID to queueId.toString()),
//    cause,
//  )

class WorkItemNotFoundException(name: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.WORK_ITEM_NOT_FOUND,
    "WorkItem $name not found",
    mapOf(Errors.Metadata.WORK_ITEM to name),
    cause,
  ) {
    companion object : Factory<WorkItemNotFoundException>() {
      override val reason: Errors.Reason
        get() = Errors.Reason.WORK_ITEM_NOT_FOUND

      override fun fromInternal(
        internalMetadata: Map<InternalErrors.Metadata, String>,
        cause: Throwable,
      ):WorkItemNotFoundException {
        return WorkItemNotFoundException(
          internalMetadata.getValue(InternalErrors.Metadata.WORK_ITEM_RESOURCE_ID)
        )
      }
    }
  }

//class EtagMismatchException(requestEtag: String, etag: String, cause: Throwable? = null) :
//  ServiceException(
//    reason,
//    "Request etag $requestEtag does not match actual etag $etag",
//    mapOf(Errors.Metadata.REQUEST_ETAG to requestEtag, Errors.Metadata.ETAG to etag),
//    cause,
//  ) {
//  companion object : Factory<EtagMismatchException>() {
//    override val reason: Errors.Reason
//      get() = Errors.Reason.ETAG_MISMATCH
//
//    override fun fromInternal(
//      internalMetadata: Map<InternalErrors.Metadata, String>,
//      cause: Throwable,
//    ): EtagMismatchException {
//      return EtagMismatchException(
//        internalMetadata.getValue(InternalErrors.Metadata.REQUEST_ETAG),
//        internalMetadata.getValue(InternalErrors.Metadata.ETAG),
//        cause,
//      )
//    }
//  }
//}

//class WorkItemAttemptNotFoundException(
//  workItemResourceId: String,
//  workItemAttemptResourceId: String,
//  cause: Throwable? = null) :
//  org.wfanet.measurement.securecomputation.service.internal.ServiceException(
//    org.wfanet.measurement.securecomputation.service.internal.Errors.Reason.WORK_ITEM_ATTEMPT_NOT_FOUND,
//    "WorkItemAttempt with workItemResource ID $workItemResourceId and workItemAttemptResource ID $workItemAttemptResourceId not found",
//    mapOf(
//      org.wfanet.measurement.securecomputation.service.internal.Errors.Metadata.WORK_ITEM_RESOURCE_ID to workItemResourceId.toString(),
//      org.wfanet.measurement.securecomputation.service.internal.Errors.Metadata.WORK_ITEM_ATTEMPT_RESOURCE_ID to workItemAttemptResourceId.toString()
//
//    ),
//    cause,
//  )
//

class WorkItemAlreadyExistsException(name: String, cause: Throwable? = null) :
  ServiceException(
    Errors.Reason.WORK_ITEM_ALREADY_EXISTS,
    "WorkItem $name already exists",
    mapOf(Errors.Metadata.WORK_ITEM to name),
    cause,
  )

//class WorkItemAttemptAlreadyExistsException(cause: Throwable? = null) :
//  org.wfanet.measurement.securecomputation.service.internal.ServiceException(org.wfanet.measurement.securecomputation.service.internal.Errors.Reason.WORK_ITEM_ATTEMPT_ALREADY_EXISTS, "WorkItemAttempt already exists", emptyMap(), cause)
//
//class WorkItemInvalidPreconditionStateException(workItemResourceId: String, cause: Throwable? = null) :
//  org.wfanet.measurement.securecomputation.service.internal.ServiceException(
//    org.wfanet.measurement.securecomputation.service.internal.Errors.Reason.INVALID_WORK_ITEM_PRECONDITION_STATE,
//    "WorkItemAttempt cannot be created when parent WorkItem has state either SUCCEEDED or FAILED",
//    mapOf(org.wfanet.measurement.securecomputation.service.internal.Errors.Metadata.WORK_ITEM_RESOURCE_ID to workItemResourceId.toString()),
//    cause,
//  )
