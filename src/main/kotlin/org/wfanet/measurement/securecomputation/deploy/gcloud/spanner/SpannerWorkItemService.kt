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
import com.google.protobuf.Timestamp
import io.grpc.Status
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.access.deploy.gcloud.spanner.SpannerRolesService
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.RoleResult
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.getRoleByResourceId
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.readRoles
import org.wfanet.measurement.access.service.internal.InvalidFieldValueException
import org.wfanet.measurement.access.service.internal.PermissionNotFoundForRoleException
import org.wfanet.measurement.access.service.internal.RoleAlreadyExistsException
import org.wfanet.measurement.access.service.internal.RoleNotFoundException
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.securecomputation.service.internal.QueueNotFoundException
import org.wfanet.measurement.securecomputation.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.access.ListRolesPageTokenKt
import org.wfanet.measurement.internal.access.Role
import org.wfanet.measurement.internal.access.listRolesPageToken
import org.wfanet.measurement.internal.access.listRolesResponse
import org.wfanet.measurement.internal.securecomputation.controlplane.CreateWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.FailWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.GetWorkItemRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.StreamWorkItemsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.common.WorkItemNotFoundException
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.WorkItemResult
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.failWorkItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.getWorkItemByResourceId
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.insertWorkItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.workItemIdExists
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.workItemResourceIdExists
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.queries.StreamWorkItems
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.readers.WorkItemReader
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.QueueNotFoundForInternalIdException


class SpannerWorkItemsService(
  private val databaseClient: AsyncDatabaseClient,
  private val queueMapping: QueueMapping,
  private val idGenerator: IdGenerator,
) : WorkItemsCoroutineImplBase() {

  override suspend fun createWorkItem(request: CreateWorkItemRequest): WorkItem {

    if (request.workItem.queueResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("queue_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val queue = try {
      getQueueByResourceId(request.workItem.queueResourceId)
    } catch (e: QueueNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }

    val transactionRunner = databaseClient.readWriteTransaction(Options.tag("action=createWorkItem"))

    val (workItemResourceId, state, commitTimestamp) = try {
      transactionRunner.run { txn ->
        val workItemId = idGenerator.generateNewId { id -> txn.workItemIdExists(id) }
        val workItemResourceId = idGenerator.generateNewId { id -> txn.workItemResourceIdExists(id) }

        val state = txn.insertWorkItem(workItemId, workItemResourceId, queue.queueId)
        Triple(workItemResourceId, state, transactionRunner.getCommitTimestamp().toProto())
      }
    } catch (e: SpannerException) {
      if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
        throw RoleAlreadyExistsException(e).asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
      } else {
        throw e
      }
    }

    return workItem {
      this.workItemResourceId = workItemResourceId
      this.queueResourceId = request.workItem.queueResourceId
      this.state = state
      createTime = commitTimestamp
      updateTime = commitTimestamp
    }
  }

  override suspend fun getWorkItem(request: GetWorkItemRequest): WorkItem {

    if (request.workItemResourceId == 0L) {
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
      } catch (e: QueueNotFoundForInternalIdException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }

    return workItemResult.workItem
  }

  override fun streamWorkItems(request: StreamWorkItemsRequest): Flow<WorkItem> {

    if (request.limit < 0) {
      throw InvalidFieldValueException("limit") { fieldName ->
        "$fieldName cannot e less that 0"
      }
    }

    val pageSize =
      if (request.pageSize == 0) {
        SpannerRolesService.DEFAULT_PAGE_SIZE
      } else {
        request.pageSize.coerceAtMost(SpannerRolesService.MAX_PAGE_SIZE)
      }
    val after = if (request.hasPageToken()) request.pageToken.after else null

    return try {
      databaseClient.singleUse().use { txn ->
        val roles: Flow<Role> =
          txn.readRoles(permissionMapping, pageSize + 1, after).map { it.role }
        listRolesResponse {
          roles.collectIndexed { index, role ->
            if (index == pageSize) {
              nextPageToken = listRolesPageToken {
                this.after =
                  ListRolesPageTokenKt.after {
                    roleResourceId = this@listRolesResponse.roles.last().roleResourceId
                  }
              }
            } else {
              this.roles += role
            }
          }
        }
      }
    } catch (e: PermissionNotFoundForRoleException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL)
    }
  }

  override suspend fun failWorkItem(request: FailWorkItemRequest): WorkItem {

    if (request.workItemResourceId == 0L) {
      throw RequiredFieldNotSetException("work_item_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=failWorkItem"))

    val (workItem, state) = transactionRunner.run { txn ->
      val (workItemId: Long, workItem: WorkItem) =
        try {
          txn.getWorkItemByResourceId(queueMapping, request.workItemResourceId)
        } catch (e: WorkItemNotFoundException) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        } catch (e: QueueNotFoundForInternalIdException) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        }

      val state = txn.failWorkItem(workItemId)
      Pair(workItem, state)
    }
    val commitTimestamp: Timestamp = transactionRunner.getCommitTimestamp().toProto()

    return workItem {
      workItemResourceId = workItem.workItemResourceId
      queueResourceId = workItem.queueResourceId
      this.state = state
      updateTime = commitTimestamp
      createTime = workItem.createTime
    }

  }

  /**
   * Returns the [QueueMapping.Queue] with the specified [queueResourceId].
   *
   * @throws QueueNotFoundException
   */
  private fun getQueueByResourceId(
    queueResourceId: String
  ): QueueMapping.Queue {
    return queueMapping.getQueueByResourceId(queueResourceId)
      ?: throw QueueNotFoundException(queueResourceId)
  }

}
