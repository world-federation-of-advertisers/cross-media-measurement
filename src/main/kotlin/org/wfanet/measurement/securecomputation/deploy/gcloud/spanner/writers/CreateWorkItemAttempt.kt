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
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.v1alpha.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelProviderNotFoundException
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.common.WorkItemNotFoundException

class CreateWorkItemAttempt (private val workItemAttempt: WorkItemAttempt) :
  SpannerWriter<WorkItemAttempt, WorkItemAttempt>() {

  override suspend fun TransactionScope.runTransaction(): WorkItemAttempt {

    val workItemId: InternalId =
      readWorkItemId(ExternalId(workItemAttempt.externalWorkItemId))

    val internalWorkItemId = idGenerator.generateInternalId()
    val externalWorkItemId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("WorkItems") {
      set("WorkItemId" to internalWorkItemId)
      set("ExternalWorkItemId" to externalWorkItemId)
      set("Queue" to workItem.queue)
      set("State").toInt64(WorkItem.State.QUEUED)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
    }

    return workItem.copy {
      this.externalWorkItemId = externalWorkItemId.value
      this.state = WorkItem.State.QUEUED
    }

  }

  private suspend fun TransactionScope.readWorkItemId(
    externalWorkItemId: ExternalId
  ): InternalId {
    val column = "WorkItemId"
    return transactionContext
      .readRowUsingIndex(
        "WorkItems",
        "WorkItemsByExternalId",
        Key.of(externalWorkItemId.value),
        column,
      )
      ?.let { struct -> InternalId(struct.getLong(column)) }
      ?: throw WorkItemNotFoundException(externalWorkItemId) {
        "WorkItem with external ID $externalWorkItemId not found"
      }
  }
  override fun ResultScope<WorkItemAttempt>.buildResult(): WorkItemAttempt {
    return checkNotNull(this.transactionResult).copy {
      createTime = commitTimestamp.toProto()
      updateTime = commitTimestamp.toProto()
    }
  }

}
