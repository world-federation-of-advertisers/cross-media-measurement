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

package org.wfanet.measurement.securecomputation.controlplane.v1alpha

import io.grpc.Status
import io.grpc.StatusException
import java.io.IOException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.api.ResourceIds
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemAttemptsPageToken
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemAttemptsResponse as InternalListWorkItemAttemptsResponse
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt as InternalWorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub as InternalWorkItemAttemptsCoroutineStub
import org.wfanet.measurement.internal.securecomputation.controlplane.completeWorkItemAttemptRequest as internalCompleteWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemAttemptRequest as internalCreateWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.failWorkItemAttemptRequest as internalFailWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.getWorkItemAttemptRequest as internalGetWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemAttemptsRequest as internalListWorkItemAttemptsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.workItemAttempt as internalWorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase
import org.wfanet.measurement.securecomputation.service.InvalidFieldValueException
import org.wfanet.measurement.securecomputation.service.RequiredFieldNotSetException
import org.wfanet.measurement.securecomputation.service.WorkItemAttemptAlreadyExistsException
import org.wfanet.measurement.securecomputation.service.WorkItemAttemptInvalidStateException
import org.wfanet.measurement.securecomputation.service.WorkItemAttemptKey
import org.wfanet.measurement.securecomputation.service.WorkItemAttemptNotFoundException
import org.wfanet.measurement.securecomputation.service.WorkItemInvalidStateException
import org.wfanet.measurement.securecomputation.service.WorkItemKey
import org.wfanet.measurement.securecomputation.service.WorkItemNotFoundException
import org.wfanet.measurement.securecomputation.service.internal.Errors as InternalErrors

class WorkItemAttemptsService(
  private val internalWorkItemAttemptsStub: InternalWorkItemAttemptsCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : WorkItemAttemptsCoroutineImplBase(coroutineContext) {
  override suspend fun createWorkItemAttempt(
    request: CreateWorkItemAttemptRequest
  ): WorkItemAttempt {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.workItemAttemptId.isEmpty()) {
      throw RequiredFieldNotSetException("work_item_attempt_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (!ResourceIds.RFC_1034_REGEX.matches(request.workItemAttemptId)) {
      throw InvalidFieldValueException("work_item_attempt_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val parentKey =
      WorkItemKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalWorkItemAttempt =
      try {
        internalWorkItemAttemptsStub.createWorkItemAttempt(
          internalCreateWorkItemAttemptRequest {
            this.workItemAttempt = internalWorkItemAttempt {
              workItemResourceId = parentKey.workItemId
              workItemAttemptResourceId = request.workItemAttemptId
            }
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.WORK_ITEM_NOT_FOUND ->
            WorkItemNotFoundException(request.parent, e).asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.WORK_ITEM_ATTEMPT_ALREADY_EXISTS ->
            WorkItemAttemptAlreadyExistsException(request.workItemAttempt.name, e)
              .asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.INVALID_WORK_ITEM_STATE ->
            WorkItemInvalidStateException.fromInternal(e).asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.QUEUE_NOT_FOUND,
          InternalErrors.Reason.QUEUE_NOT_FOUND_FOR_WORK_ITEM,
          InternalErrors.Reason.INVALID_WORK_ITEM_ATTEMPT_STATE,
          InternalErrors.Reason.WORK_ITEM_ATTEMPT_NOT_FOUND,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.WORK_ITEM_ALREADY_EXISTS,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }
    return internalResponse.toWorkItemAttempt()
  }

  override suspend fun getWorkItemAttempt(request: GetWorkItemAttemptRequest): WorkItemAttempt {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val key =
      WorkItemAttemptKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalWorkItemAttempt =
      try {
        internalWorkItemAttemptsStub.getWorkItemAttempt(
          internalGetWorkItemAttemptRequest {
            workItemResourceId = key.workItemId
            workItemAttemptResourceId = key.workItemAttemptId
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.WORK_ITEM_ATTEMPT_NOT_FOUND ->
            WorkItemAttemptNotFoundException(request.name, e)
              .asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.QUEUE_NOT_FOUND,
          InternalErrors.Reason.QUEUE_NOT_FOUND_FOR_WORK_ITEM,
          InternalErrors.Reason.INVALID_WORK_ITEM_STATE,
          InternalErrors.Reason.INVALID_WORK_ITEM_ATTEMPT_STATE,
          InternalErrors.Reason.WORK_ITEM_NOT_FOUND,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.WORK_ITEM_ALREADY_EXISTS,
          InternalErrors.Reason.WORK_ITEM_ATTEMPT_ALREADY_EXISTS,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toWorkItemAttempt()
  }

  override suspend fun failWorkItemAttempt(request: FailWorkItemAttemptRequest): WorkItemAttempt {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val key =
      WorkItemAttemptKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalWorkItemAttempt =
      try {
        internalWorkItemAttemptsStub.failWorkItemAttempt(
          internalFailWorkItemAttemptRequest {
            workItemResourceId = key.workItemId
            workItemAttemptResourceId = key.workItemAttemptId
            errorMessage = request.errorMessage
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.WORK_ITEM_ATTEMPT_NOT_FOUND ->
            WorkItemAttemptNotFoundException(request.name, e)
              .asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.INVALID_WORK_ITEM_ATTEMPT_STATE ->
            WorkItemAttemptInvalidStateException.fromInternal(e)
              .asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.QUEUE_NOT_FOUND,
          InternalErrors.Reason.QUEUE_NOT_FOUND_FOR_WORK_ITEM,
          InternalErrors.Reason.INVALID_WORK_ITEM_STATE,
          InternalErrors.Reason.WORK_ITEM_NOT_FOUND,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.WORK_ITEM_ALREADY_EXISTS,
          InternalErrors.Reason.WORK_ITEM_ATTEMPT_ALREADY_EXISTS,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toWorkItemAttempt()
  }

  override suspend fun completeWorkItemAttempt(
    request: CompleteWorkItemAttemptRequest
  ): WorkItemAttempt {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val key =
      WorkItemAttemptKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalWorkItemAttempt =
      try {
        internalWorkItemAttemptsStub.completeWorkItemAttempt(
          internalCompleteWorkItemAttemptRequest {
            workItemResourceId = key.workItemId
            workItemAttemptResourceId = key.workItemAttemptId
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.WORK_ITEM_ATTEMPT_NOT_FOUND ->
            WorkItemAttemptNotFoundException(request.name, e)
              .asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.INVALID_WORK_ITEM_ATTEMPT_STATE ->
            WorkItemAttemptInvalidStateException.fromInternal(e)
              .asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.QUEUE_NOT_FOUND,
          InternalErrors.Reason.QUEUE_NOT_FOUND_FOR_WORK_ITEM,
          InternalErrors.Reason.INVALID_WORK_ITEM_STATE,
          InternalErrors.Reason.WORK_ITEM_NOT_FOUND,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.WORK_ITEM_ALREADY_EXISTS,
          InternalErrors.Reason.WORK_ITEM_ATTEMPT_ALREADY_EXISTS,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toWorkItemAttempt()
  }

  override suspend fun listWorkItemAttempts(
    request: ListWorkItemAttemptsRequest
  ): ListWorkItemAttemptsResponse {
    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size") { fieldName -> "$fieldName cannot be negative" }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val pageSize =
      when {
        request.pageSize == 0 -> DEFAULT_PAGE_SIZE
        request.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
        else -> request.pageSize
      }

    val internalPageToken: ListWorkItemAttemptsPageToken? =
      if (request.pageToken.isEmpty()) {
        null
      } else {
        try {
          ListWorkItemAttemptsPageToken.parseFrom(request.pageToken.base64UrlDecode())
        } catch (e: IOException) {
          throw InvalidFieldValueException("page_token", e)
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

    val internalResponse: InternalListWorkItemAttemptsResponse =
      internalWorkItemAttemptsStub.listWorkItemAttempts(
        internalListWorkItemAttemptsRequest {
          this.pageSize = pageSize
          if (internalPageToken != null) {
            pageToken = internalPageToken
          }
        }
      )

    return listWorkItemAttemptsResponse {
      workItemAttempts += internalResponse.workItemAttemptsList.map { it.toWorkItemAttempt() }
      if (internalResponse.hasNextPageToken()) {
        nextPageToken = internalResponse.nextPageToken.after.toByteString().base64UrlEncode()
      }
    }
  }

  companion object {
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 100
  }
}
