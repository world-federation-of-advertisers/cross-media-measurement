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
import org.wfanet.measurement.internal.securecomputation.controlplane.EnsureWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.FailWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.GetWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemsPageTokenKt
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemsResponse
import org.wfanet.measurement.internal.securecomputation.controlplane.RetryWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.internal.securecomputation.controlplane.copy
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemsPageToken
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemsResponse
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.WorkItemResult
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.activeWorkItemAttemptExists
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.failActiveWorkItemAttempts
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.failWorkItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.getWorkItemByResourceId
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.insertWorkItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.insertWorkItemPublication
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.readWorkItems
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.retryWorkItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.workItemIdExists
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.workItemPublicationExists
import org.wfanet.measurement.securecomputation.service.internal.InvalidFieldValueException
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.QueueNotFoundException
import org.wfanet.measurement.securecomputation.service.internal.QueueNotFoundForWorkItem
import org.wfanet.measurement.securecomputation.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemAlreadyExistsException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemGenerationMismatchException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemInvalidStateException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemNotFoundException

class SpannerWorkItemsService(
  private val databaseClient: AsyncDatabaseClient,
  private val queueMapping: QueueMapping,
  private val idGenerator: IdGenerator,
  private val workItemPublicationRunner: WorkItemPublicationRunner,
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

    val (workItemId, workItem) =
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
          txn.insertWorkItemPublication(workItemId)

          Pair(
            workItemId,
            request.workItem.copy {
              this.state = state
              generation = INITIAL_GENERATION
            },
          )
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

    workItemPublicationRunner.publishWorkItem(workItemId)

    return result
  }

  override suspend fun ensureWorkItem(request: EnsureWorkItemRequest): WorkItem {
    validateWorkItem(request.workItem)
    val queue =
      try {
        getQueueByResourceId(request.workItem.queueResourceId)
      } catch (e: QueueNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
      }

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=ensureWorkItem"))
    val ensured =
      transactionRunner.run { txn ->
        try {
          val existing =
            txn.getWorkItemByResourceId(queueMapping, request.workItem.workItemResourceId)
          if (
            existing.workItem.queueResourceId != request.workItem.queueResourceId ||
              existing.workItem.workItemParams != request.workItem.workItemParams
          ) {
            throw WorkItemAlreadyExistsException()
          }
          when (existing.workItem.state) {
            WorkItem.State.QUEUED -> {
              if (!txn.workItemPublicationExists(existing.workItemId)) {
                txn.insertWorkItemPublication(existing.workItemId)
              }
            }
            WorkItem.State.RUNNING -> Unit
            WorkItem.State.FAILED,
            WorkItem.State.SUCCEEDED,
            WorkItem.State.STATE_UNSPECIFIED,
            WorkItem.State.UNRECOGNIZED ->
              throw WorkItemInvalidStateException(
                existing.workItem.workItemResourceId,
                existing.workItem.state,
              )
          }
          EnsuredWorkItem(existing.workItemId, existing.workItem, created = false)
        } catch (e: WorkItemNotFoundException) {
          val workItemId = idGenerator.generateNewId { id -> txn.workItemIdExists(id) }
          val state =
            txn.insertWorkItem(
              workItemId,
              request.workItem.workItemResourceId,
              queue.queueId,
              request.workItem.workItemParams,
            )
          txn.insertWorkItemPublication(workItemId)
          EnsuredWorkItem(
            workItemId,
            request.workItem.copy {
              this.state = state
              generation = INITIAL_GENERATION
            },
            created = true,
          )
        } catch (e: QueueNotFoundForWorkItem) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        } catch (e: WorkItemAlreadyExistsException) {
          throw e.asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
        } catch (e: WorkItemInvalidStateException) {
          throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        }
      }

    val result =
      if (ensured.created) {
        val commitTimestamp = transactionRunner.getCommitTimestamp().toProto()
        ensured.workItem.copy {
          createTime = commitTimestamp
          updateTime = commitTimestamp
        }
      } else {
        ensured.workItem
      }
    if (result.state == WorkItem.State.QUEUED) {
      workItemPublicationRunner.publishWorkItem(ensured.workItemId)
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
    if (request.expectedWorkItemGeneration < 0L) {
      throw InvalidFieldValueException("expected_work_item_generation") { fieldName ->
          "$fieldName must be non-negative"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val expectedGeneration =
      request.expectedWorkItemGeneration.takeUnless { it == 0L } ?: INITIAL_GENERATION

    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=failWorkItem"))

    val workItem =
      transactionRunner.run { txn ->
        try {
          val workItemResult = txn.getWorkItemByResourceId(queueMapping, request.workItemResourceId)
          if (workItemResult.workItem.generation != expectedGeneration) {
            throw WorkItemGenerationMismatchException(
              workItemResult.workItem.workItemResourceId,
              expectedGeneration,
              workItemResult.workItem.generation,
            )
          }
          when (workItemResult.workItem.state) {
            WorkItem.State.QUEUED,
            WorkItem.State.RUNNING,
            WorkItem.State.FAILED -> Unit
            WorkItem.State.SUCCEEDED,
            WorkItem.State.STATE_UNSPECIFIED,
            WorkItem.State.UNRECOGNIZED ->
              throw WorkItemInvalidStateException(
                workItemResult.workItem.workItemResourceId,
                workItemResult.workItem.state,
              )
          }
          txn.failActiveWorkItemAttempts(workItemResult.workItemId)
          val state = txn.failWorkItem(workItemResult.workItemId)
          workItemResult.workItem.copy { this.state = state }
        } catch (e: WorkItemNotFoundException) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        } catch (e: QueueNotFoundForWorkItem) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        } catch (e: WorkItemGenerationMismatchException) {
          throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        } catch (e: WorkItemInvalidStateException) {
          throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        }
      }
    val result = workItem.copy { updateTime = transactionRunner.getCommitTimestamp().toProto() }
    return result
  }

  override suspend fun retryWorkItem(request: RetryWorkItemRequest): WorkItem {
    if (request.workItemResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("work_item_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val transactionRunner = databaseClient.readWriteTransaction(Options.tag("action=retryWorkItem"))
    val (workItemId, workItem) =
      transactionRunner.run { txn ->
        try {
          val result = txn.getWorkItemByResourceId(queueMapping, request.workItemResourceId)
          val state =
            when (result.workItem.state) {
              WorkItem.State.FAILED,
              WorkItem.State.RUNNING -> {
                if (txn.activeWorkItemAttemptExists(result.workItemId)) {
                  throw WorkItemInvalidStateException(
                    result.workItem.workItemResourceId,
                    result.workItem.state,
                  )
                }
                txn.retryWorkItem(result.workItemId, result.workItem.generation)
              }
              WorkItem.State.QUEUED -> {
                if (!txn.workItemPublicationExists(result.workItemId)) {
                  txn.insertWorkItemPublication(result.workItemId)
                }
                WorkItem.State.QUEUED
              }
              WorkItem.State.SUCCEEDED,
              WorkItem.State.STATE_UNSPECIFIED,
              WorkItem.State.UNRECOGNIZED ->
                throw WorkItemInvalidStateException(
                  result.workItem.workItemResourceId,
                  result.workItem.state,
                )
            }
          result.workItemId to
            result.workItem.copy {
              this.state = state
              if (
                state == WorkItem.State.QUEUED && result.workItem.state != WorkItem.State.QUEUED
              ) {
                generation = result.workItem.generation + 1L
              }
            }
        } catch (e: WorkItemNotFoundException) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        } catch (e: QueueNotFoundForWorkItem) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        } catch (e: WorkItemInvalidStateException) {
          throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        }
      }
    val result = workItem.copy { updateTime = transactionRunner.getCommitTimestamp().toProto() }
    workItemPublicationRunner.publishWorkItem(workItemId)
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

  private fun validateWorkItem(workItem: WorkItem) {
    if (workItem.queueResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("queue_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (workItem.workItemResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("work_item_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (!workItem.hasWorkItemParams()) {
      throw RequiredFieldNotSetException("work_item_params")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
  }

  private data class EnsuredWorkItem(
    val workItemId: Long,
    val workItem: WorkItem,
    val created: Boolean,
  )

  companion object {
    private const val MAX_PAGE_SIZE = 100
    private const val DEFAULT_PAGE_SIZE = 50
    private const val INITIAL_GENERATION = 1L
  }
}
