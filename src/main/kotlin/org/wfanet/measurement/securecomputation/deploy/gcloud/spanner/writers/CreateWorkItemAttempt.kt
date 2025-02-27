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

package org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Key
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.copy
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.common.WorkItemInvalidStateException
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.common.WorkItemNotFoundException

class CreateWorkItemAttempt (private val workItemAttempt: WorkItemAttempt) :
  SpannerWriter<WorkItemAttempt, WorkItemAttempt>() {

  override suspend fun TransactionScope.runTransaction(): WorkItemAttempt {

    val internalWorkItemId: InternalId =
      readWorkItemId(ExternalId(workItemAttempt.workItemResourceId))

    val workItemState = readWorkItemState(ExternalId(workItemAttempt.workItemResourceId))

    if(WorkItem.State.forNumber(workItemState.value.toInt()) == WorkItem.State.FAILED ||
      WorkItem.State.forNumber(workItemState.value.toInt()) == WorkItem.State.SUCCEEDED) {
      throw WorkItemInvalidStateException(ExternalId(workItemAttempt.workItemResourceId))
    }

    val internalWorkItemAttemptId = idGenerator.generateInternalId()
    val workItemAttemptResourceId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("WorkItemAttempts") {
      set("WorkItemId" to internalWorkItemId)
      set("WorkItemAttemptId" to internalWorkItemAttemptId)
      set("WorkItemAttemptResourceId" to workItemAttemptResourceId)
      set("State").toInt64(WorkItemAttempt.State.CLAIMED)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
    }

    transactionContext.bufferUpdateMutation("WorkItems") {
      set("WorkItemId" to internalWorkItemId)
      set("State").toInt64(WorkItem.State.CLAIMED)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
    }

    return workItemAttempt.copy {
      this.workItemAttemptResourceId = workItemAttemptResourceId.value
      this.state = WorkItemAttempt.State.CLAIMED
    }

  }

  private suspend fun TransactionScope.readWorkItemId(
    workItemResourceId: ExternalId
  ): InternalId {
    val column = "WorkItemId"
    return transactionContext
      .readRowUsingIndex(
        "WorkItems",
        "WorkItemsByResourceId",
        Key.of(workItemResourceId.value),
        column,
      )
      ?.let { struct -> InternalId(struct.getLong(column)) }
      ?: throw WorkItemNotFoundException(workItemResourceId) {
        "WorkItem with external ID $workItemResourceId not found"
      }
  }

  private suspend fun TransactionScope.readWorkItemState(
    workItemResourceId: ExternalId
  ): InternalId {
    val column = "State"
    return transactionContext
      .readRowUsingIndex(
        "WorkItems",
        "WorkItemsByResourceId",
        Key.of(workItemResourceId.value),
        column,
      )
      ?.let { struct -> InternalId(struct.getLong(column)) }
      ?: throw WorkItemNotFoundException(workItemResourceId) {
        "WorkItem with external ID $workItemResourceId not found"
      }
  }

  override fun ResultScope<WorkItemAttempt>.buildResult(): WorkItemAttempt {
    return checkNotNull(this.transactionResult).copy {
      createTime = commitTimestamp.toProto()
      updateTime = commitTimestamp.toProto()
    }
  }

}
