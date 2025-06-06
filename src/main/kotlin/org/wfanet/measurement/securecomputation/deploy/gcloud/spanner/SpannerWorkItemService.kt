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

package org.wfanet.measurement.securecomputation.deploy.gcloud.spanner

import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.SpannerException
import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.securecomputation.controlplane.CreateWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.FailWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.GetWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemsPageTokenKt
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemsResponse
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.internal.securecomputation.controlplane.copy
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemsPageToken
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemsResponse
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.WorkItemResult
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.failWorkItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.failWorkItemAttempt
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.getWorkItemByResourceId
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.insertWorkItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.readWorkItemAttempts
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.readWorkItems
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.workItemIdExists
import org.wfanet.measurement.securecomputation.service.internal.InvalidFieldValueException
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.QueueNotFoundException
import org.wfanet.measurement.securecomputation.service.internal.QueueNotFoundForWorkItem
import org.wfanet.measurement.securecomputation.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemAlreadyExistsException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemNotFoundException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemPublisher

class SpannerWorkItemsService(
  private val databaseClient: AsyncDatabaseClient,
  private val queueMapping: QueueMapping,
  private val idGenerator: IdGenerator,
  private val workItemPublisher: WorkItemPublisher,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : WorkItemsCoroutineImplBase(coroutineContext) {

  override suspend fun createWorkItem(request: CreateWorkItemRequest): WorkItem {
    if (request.workItem.queueResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("queue_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.workItem.workItemResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("work_item_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (!request.workItem.hasWorkItemParams()) {
      throw RequiredFieldNotSetException("work_item_params")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val queue =
      try {
        getQueueByResourceId(request.workItem.queueResourceId)
      } catch (e: QueueNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
      }

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=createWorkItem"))

    val workItem =
      try {
        transactionRunner.run { txn ->
          val workItemId: Long = idGenerator.generateNewId { id -> txn.workItemIdExists(id) }

          val state: WorkItem.State =
            txn.insertWorkItem(
              workItemId,
              request.workItem.workItemResourceId,
              queue.queueId,
              request.workItem.workItemParams,
            )

          request.workItem.copy { this.state = state }
        }
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw WorkItemAlreadyExistsException(e)
            .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
        } else {
          throw e
        }
      }

    val commitTimestamp = transactionRunner.getCommitTimestamp().toProto()
    val result =
      workItem.copy {
        createTime = commitTimestamp
        updateTime = commitTimestamp
      }

    try {
      workItemPublisher.publishMessage(request.workItem.queueResourceId, request.workItem)
    } catch (e: Exception) {
      throw Status.INTERNAL.withCause(e).asRuntimeException()
    }

    return result
  }

  override suspend fun getWorkItem(request: GetWorkItemRequest): WorkItem {

    if (request.workItemResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("work_item_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val workItemResult: WorkItemResult =
      try {
        databaseClient.singleUse().use { txn ->
          txn.getWorkItemByResourceId(queueMapping, request.workItemResourceId)
        }
      } catch (e: WorkItemNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      } catch (e: QueueNotFoundForWorkItem) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }

    return workItemResult.workItem
  }

  override suspend fun listWorkItems(request: ListWorkItemsRequest): ListWorkItemsResponse {

    if (request.pageSize < 0) {
      throw InvalidFieldValueException("max_page_size") { fieldName ->
        "$fieldName must be non-negative"
      }
    }
    val pageSize =
      if (request.pageSize == 0) {
        DEFAULT_PAGE_SIZE
      } else {
        request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
      }
    val after = if (request.hasPageToken()) request.pageToken.after else null
    return databaseClient.singleUse().use { txn ->
      val workItems: Flow<WorkItem> =
        txn.readWorkItems(queueMapping, pageSize + 1, after).map { it.workItem }
      listWorkItemsResponse {
        workItems.collectIndexed { index, workItem ->
          if (index == pageSize) {
            nextPageToken = listWorkItemsPageToken {
              this.after =
                ListWorkItemsPageTokenKt.after {
                  workItemResourceId =
                    this@listWorkItemsResponse.workItems.last().workItemResourceId
                  createdAfter = this@listWorkItemsResponse.workItems.last().createTime
                }
            }
          } else {
            this.workItems += workItem
          }
        }
      }
    }
  }

  override suspend fun failWorkItem(request: FailWorkItemRequest): WorkItem {

    if (request.workItemResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("work_item_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=failWorkItem"))

    val workItem =
      transactionRunner.run { txn ->
        try {
          val workItemResult = txn.getWorkItemByResourceId(queueMapping, request.workItemResourceId)
          val state = txn.failWorkItem(workItemResult.workItemId)
          txn.readWorkItemAttempts(MAX_PAGE_SIZE, request.workItemResourceId).collect {
            workItemAttempt ->
            txn.failWorkItemAttempt(workItemResult.workItemId, workItemAttempt.workItemAttemptId)
          }
          workItemResult.workItem.copy { this.state = state }
        } catch (e: WorkItemNotFoundException) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        } catch (e: QueueNotFoundForWorkItem) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        }
      }
    val result = workItem.copy { updateTime = transactionRunner.getCommitTimestamp().toProto() }
    return result
  }

  /**
   * Returns the [QueueMapping.Queue] with the specified [queueResourceId].
   *
   * @throws QueueNotFoundException
   */
  private fun getQueueByResourceId(queueResourceId: String): QueueMapping.Queue {
    return queueMapping.getQueueByResourceId(queueResourceId)
      ?: throw QueueNotFoundException(queueResourceId)
  }

  companion object {
    private const val MAX_PAGE_SIZE = 100
    private const val DEFAULT_PAGE_SIZE = 50
  }
}
