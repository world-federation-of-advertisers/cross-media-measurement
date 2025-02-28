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
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.securecomputation.controlplane.CreateWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.internal.securecomputation.controlplane.copy
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem
import org.wfanet.measurement.internal.securecomputation.controlplane.workItemAttempt
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.WorkItemAttemptResult
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.countWorkItemAttempts
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.getWorkItemByResourceId
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.insertWorkItem
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.insertWorkItemAttempt
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.workItemAttemptExists
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.workItemAttemptResourceIdExists
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.workItemIdExists
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.workItemResourceIdExists
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.writers.CreateWorkItemAttempt
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.QueueNotFoundException
import org.wfanet.measurement.securecomputation.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemAlreadyExistsException
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

    val (workItemAttemptResourceId, workItemAttemptNumber, state) = try {
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

        val state = txn.insertWorkItemAttempt(result.workItemId, workItemAttemptId, workItemAttemptResourceId)
        Triple(
          workItemAttemptResourceId,
          workItemAttemptNumber,
          state
        )
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
    val commitTimestamp = transactionRunner.getCommitTimestamp().toProto()

    return request.workItemAttempt.copy {
      this.workItemAttemptResourceId = workItemAttemptResourceId
      this.attemptNumber = workItemAttemptNumber
      this.state = state
    }

  }

}
