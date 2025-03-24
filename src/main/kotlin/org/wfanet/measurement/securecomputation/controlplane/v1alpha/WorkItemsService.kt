/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

import com.google.protobuf.Message
import io.grpc.Status
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt.WorkItemsCoroutineStub as InternalWorkItemsCoroutineStub
import io.grpc.StatusException
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem as InternalWorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem as internalWorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.createWorkItemRequest as internalCreateWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.getWorkItemRequest as internalGetWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.failWorkItemRequest as internalFailWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemsRequest as internalListWorkItemsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemsResponse as InternalListWorkItemsResponse
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.securecomputation.service.internal.Errors as InternalErrors
import java.io.IOException
import org.wfanet.measurement.common.api.ResourceIds
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemsPageToken
import org.wfanet.measurement.securecomputation.service.InvalidFieldValueException
import org.wfanet.measurement.securecomputation.service.RequiredFieldNotSetException
import org.wfanet.measurement.securecomputation.service.WorkItemAlreadyExistsException
import org.wfanet.measurement.securecomputation.service.WorkItemKey
import org.wfanet.measurement.securecomputation.service.WorkItemNotFoundException

abstract class WorkItemsService(private val internalWorkItemsStub: InternalWorkItemsCoroutineStub) :
  WorkItemsCoroutineImplBase() {

  abstract suspend fun publishMessage(queueName: String, message: Message)

  override suspend fun createWorkItem(request: CreateWorkItemRequest): WorkItem {

    if (!request.hasWorkItem()) {
      throw RequiredFieldNotSetException("work_item")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.workItem.queue.isEmpty()) {
      throw RequiredFieldNotSetException("queue")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.workItemId.isEmpty()) {
      throw RequiredFieldNotSetException("work_item_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (!ResourceIds.RFC_1034_REGEX.matches(request.workItemId)) {
      throw InvalidFieldValueException("work_item_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val workItem = request.workItem
    val topicId = workItem.queue

    try {
      publishMessage(topicId, workItem.workItemParams)
    } catch (e: Exception) {
      throw when {
        e.message?.contains("Topic id: $topicId does not exist") == true -> {
          Status.NOT_FOUND.withDescription(e.message).asRuntimeException()
        }

        else -> {
          Status.UNKNOWN.withDescription("An unknown error occurred: ${e.message}")
            .asRuntimeException()
        }
      }
    }

    val internalResponse: InternalWorkItem =
      try {
        internalWorkItemsStub.createWorkItem(
          internalCreateWorkItemRequest {
            internalWorkItem {
              queueResourceId = request.workItem.queue
              workItemResourceId = request.workItemId
            }
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.QUEUE_NOT_FOUND,
          InternalErrors.Reason.QUEUE_NOT_FOUND_FOR_WORK_ITEM,
          InternalErrors.Reason.INVALID_WORK_ITEM_STATE,
          InternalErrors.Reason.WORK_ITEM_NOT_FOUND,
          InternalErrors.Reason.WORK_ITEM_ATTEMPT_NOT_FOUND,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.WORK_ITEM_ALREADY_EXISTS ->
            WorkItemAlreadyExistsException(request.workItem.name, e).asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.WORK_ITEM_ATTEMPT_ALREADY_EXISTS,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toWorkItem()
  }

  override suspend fun getWorkItem(request: GetWorkItemRequest): WorkItem {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val key =
      WorkItemKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalWorkItem =
      try {
        internalWorkItemsStub.getWorkItem(
          internalGetWorkItemRequest {
            workItemResourceId = key.workItemId
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.QUEUE_NOT_FOUND,
          InternalErrors.Reason.QUEUE_NOT_FOUND_FOR_WORK_ITEM,
          InternalErrors.Reason.INVALID_WORK_ITEM_STATE,
          InternalErrors.Reason.WORK_ITEM_NOT_FOUND ->
            WorkItemNotFoundException(request.name, e).asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.WORK_ITEM_ATTEMPT_NOT_FOUND,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.WORK_ITEM_ALREADY_EXISTS,
          InternalErrors.Reason.WORK_ITEM_ATTEMPT_ALREADY_EXISTS,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toWorkItem()

  }

  override suspend fun listWorkItems(request: ListWorkItemsRequest): ListWorkItemsResponse {
    if (request.pageSize < 0) {
      throw InvalidFieldValueException("page_size") { fieldName -> "$fieldName cannot be negative" }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val internalPageToken: ListWorkItemsPageToken? =
      if (request.pageToken.isEmpty()) {
        null
      } else {
        try {
          ListWorkItemsPageToken.parseFrom(request.pageToken.base64UrlDecode())
        } catch (e: IOException) {
          throw InvalidFieldValueException("page_token", e)
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        }
      }

    val internalResponse: InternalListWorkItemsResponse =
      internalWorkItemsStub.listWorkItems(
        internalListWorkItemsRequest {
          pageSize = request.pageSize
          if (internalPageToken != null) {
            pageToken = internalPageToken
          }
        }
      )

    return listWorkItemsResponse {
      workItems += internalResponse.workItemsList.map { it.toWorkItem() }
      if (internalResponse.hasNextPageToken()) {
        nextPageToken = internalResponse.nextPageToken.after.toByteString().base64UrlEncode()
      }
    }

  }

  override suspend fun failWorkItem(request: FailWorkItemRequest): WorkItem {
    if (request.name.isEmpty()) {
      throw RequiredFieldNotSetException("name")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val key =
      WorkItemKey.fromName(request.name)
        ?: throw InvalidFieldValueException("name")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val internalResponse: InternalWorkItem =
      try {
        internalWorkItemsStub.failWorkItem(
          internalFailWorkItemRequest {
            workItemResourceId = key.workItemId
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.QUEUE_NOT_FOUND,
          InternalErrors.Reason.QUEUE_NOT_FOUND_FOR_WORK_ITEM,
          InternalErrors.Reason.INVALID_WORK_ITEM_STATE,
          InternalErrors.Reason.WORK_ITEM_NOT_FOUND ->
            WorkItemNotFoundException(request.name, e).asStatusRuntimeException(e.status.code)
          InternalErrors.Reason.WORK_ITEM_ATTEMPT_NOT_FOUND,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          InternalErrors.Reason.WORK_ITEM_ALREADY_EXISTS,
          InternalErrors.Reason.WORK_ITEM_ATTEMPT_ALREADY_EXISTS,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return internalResponse.toWorkItem()
  }
}
