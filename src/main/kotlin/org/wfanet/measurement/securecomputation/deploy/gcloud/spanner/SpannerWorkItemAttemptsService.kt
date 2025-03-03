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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.securecomputation.controlplane.CompleteWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.CreateWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.FailWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.GetWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemAttemptsPageTokenKt
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemAttemptsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemAttemptsResponse
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemsPageTokenKt
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.internal.securecomputation.controlplane.copy
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemAttemptsPageToken
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemAttemptsResponse
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemsPageToken
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemsResponse
import org.wfanet.measurement.internal.securecomputation.controlplane.workItemAttempt
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.WorkItemAttemptResult
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.completeWorkItemAttempt
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.countWorkItemAttempts
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.getWorkItemByResourceId
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.insertWorkItemAttempt
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.readWorkItemAttempts
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.workItemAttemptExists
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.workItemAttemptResourceIdExists
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.failWorkItemAttempt
import org.wfanet.measurement.securecomputation.service.internal.InvalidFieldValueException
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.QueueNotFoundForInternalIdException
import org.wfanet.measurement.securecomputation.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemAttemptAlreadyExistsException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemInvalidPreconditionStateException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemNotFoundException

class SpannerWorkItemAttemptsService(
  private val databaseClient: AsyncDatabaseClient,
  private val queueMapping: QueueMapping,
  private val idGenerator: org.wfanet.measurement.common.IdGenerator,
) : WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase() {

  override suspend fun createWorkItemAttempt(request: CreateWorkItemAttemptRequest): WorkItemAttempt {

    if (request.workItemAttempt.workItemResourceId == 0L) {
      throw RequiredFieldNotSetException("work_item_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val transactionRunner = databaseClient.readWriteTransaction(Options.tag("action=createWorkItemAttempt"))

    val workItemAttempt = try {
      transactionRunner.run { txn ->

        val result = txn.getWorkItemByResourceId(queueMapping, request.workItemAttempt.workItemResourceId)
        val workItemAttemptNumber = txn.countWorkItemAttempts(result.workItemId) + 1
        val workItemState = result.workItem.state
        if(workItemState == WorkItem.State.FAILED ||
          workItemState == WorkItem.State.SUCCEEDED) {
          throw WorkItemInvalidPreconditionStateException(result.workItem.workItemResourceId)
        }

        val workItemAttemptId = idGenerator.generateNewId { id -> txn.workItemAttemptExists(result.workItemId, id) }
        val workItemAttemptResourceId = idGenerator.generateNewId { id -> txn.workItemAttemptResourceIdExists(result.workItemId, workItemAttemptId, id) }

        val state = txn.insertWorkItemAttempt(result.workItemId, workItemAttemptId, workItemAttemptResourceId, workItemAttemptNumber)
        val commitTimestamp = transactionRunner.getCommitTimestamp().toProto()
        request.workItemAttempt.copy {
          this.workItemAttemptResourceId = workItemAttemptResourceId
          this.attemptNumber = workItemAttemptNumber
          this.state = state
          this.createTime = commitTimestamp
          this.updateTime = commitTimestamp
        }
      }
    } catch (e: SpannerException) {
      if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
        throw WorkItemAttemptAlreadyExistsException(e).asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
      } else {
        throw e
      }
    } catch (e: WorkItemNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
    return workItemAttempt

  }

  override suspend fun getWorkItemAttempt(request: GetWorkItemAttemptRequest): WorkItemAttempt {
    if (request.workItemResourceId == 0L) {
      throw RequiredFieldNotSetException("work_item_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.workItemAttemptResourceId == 0L) {
      throw RequiredFieldNotSetException("work_item_attempt_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val workItemAttemptResult: WorkItemAttemptResult =
      try {
        databaseClient.singleUse().use { txn ->
          txn.getWorkItemByResourceId(request.workItemResourceId, request.workItemAttemptResourceId)
        }
      } catch (e: WorkItemNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      } catch (e: QueueNotFoundForInternalIdException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }

    return workItemAttemptResult.workItemAttempt
  }

  override suspend fun failWorkItemAttempt(request: FailWorkItemAttemptRequest): WorkItemAttempt {
    if (request.workItemResourceId == 0L) {
      throw RequiredFieldNotSetException("work_item_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.workItemAttemptResourceId == 0L) {
      throw RequiredFieldNotSetException("work_item_attempt_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=failWorkItemAttempt"))

    val workItemAttempt = transactionRunner.run { txn ->
      try {
        val workItemAttemptResult = txn.getWorkItemByResourceId(request.workItemResourceId, request.workItemAttemptResourceId)
        val state = txn.failWorkItemAttempt(workItemAttemptResult.workItemId, workItemAttemptResult.workItemAttemptId)
        workItemAttemptResult.workItemAttempt.copy {
          this.state = state
          this.updateTime = transactionRunner.getCommitTimestamp().toProto()
        }
        workItemAttemptResult.workItemAttempt
      } catch (e: WorkItemNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      } catch (e: QueueNotFoundForInternalIdException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }
    }
    return workItemAttempt
  }

  override suspend fun completeWorkItemAttempt(request: CompleteWorkItemAttemptRequest): WorkItemAttempt {
    if (request.workItemResourceId == 0L) {
      throw RequiredFieldNotSetException("work_item_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.workItemAttemptResourceId == 0L) {
      throw RequiredFieldNotSetException("work_item_attempt_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=completeWorkItemAttempt"))

    val workItemAttempt = transactionRunner.run { txn ->
      try {
        val workItemAttemptResult = txn.getWorkItemByResourceId(request.workItemResourceId, request.workItemAttemptResourceId)
        val state = txn.completeWorkItemAttempt(workItemAttemptResult.workItemId, workItemAttemptResult.workItemAttemptId)
        workItemAttemptResult.workItemAttempt.copy {
          this.state = state
          this.updateTime = transactionRunner.getCommitTimestamp().toProto()
        }
        workItemAttemptResult.workItemAttempt
      } catch (e: WorkItemNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      } catch (e: QueueNotFoundForInternalIdException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }
    }
    return workItemAttempt
  }

  override suspend fun listWorkItemAttempts(request: ListWorkItemAttemptsRequest): ListWorkItemAttemptsResponse {
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
      val workItemAttempts: Flow<WorkItemAttempt> =
        txn.readWorkItemAttempts(pageSize + 1, request.workItemResourceId, after).map { it.workItemAttempt }
      listWorkItemAttemptsResponse {
        workItemAttempts.collectIndexed { index, workItemAttempt ->
          if (index == pageSize) {
            nextPageToken = listWorkItemAttemptsPageToken {
              this.after =
                ListWorkItemAttemptsPageTokenKt.after {
                  workItemAttemptResourceId = this@listWorkItemAttemptsResponse.workItemAttempts.last().workItemAttemptResourceId
                  createAfter = this@listWorkItemAttemptsResponse.workItemAttempts.last().createTime
                }
            }
          } else {
            this.workItemAttempts += workItemAttempt
          }
        }
      }
    }
  }

  companion object {
    private const val MAX_PAGE_SIZE = 100
    private const val DEFAULT_PAGE_SIZE = 50
  }

}
