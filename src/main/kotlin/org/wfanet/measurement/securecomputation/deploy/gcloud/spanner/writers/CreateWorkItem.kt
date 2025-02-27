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

import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import com.google.cloud.spanner.Value
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.securecomputation.controlplane.copy

class CreateWorkItem (private val workItem: WorkItem) :
  SpannerWriter<WorkItem, WorkItem>() {

  override suspend fun TransactionScope.runTransaction(): WorkItem {

    val internalWorkItemId = idGenerator.generateInternalId()
    val externalWorkItemId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("WorkItems") {
      set("WorkItemId" to internalWorkItemId)
      set("WorkItemResourceId" to externalWorkItemId)
      set("Queue" to workItem.queueResourceId)
      set("State").toInt64(WorkItem.State.QUEUED)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
    }

    return workItem.copy {
      this.workItemResourceId = externalWorkItemId.value
      this.state = WorkItem.State.QUEUED
    }

  }

  override fun ResultScope<WorkItem>.buildResult(): WorkItem {
    return checkNotNull(this.transactionResult).copy {
      createTime = commitTimestamp.toProto()
      updateTime = commitTimestamp.toProto()
    }
  }

}
