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

import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.securecomputation.controlplane.v1alpha.FailWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.v1alpha.copy
import org.wfanet.measurement.internal.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.common.WorkItemAttemptNotFoundException
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.readers.WorkItemAttemptReader


class FailWorkItemAttempt(private val request: FailWorkItemAttemptRequest) :
  SpannerWriter<WorkItemAttempt, WorkItemAttempt>() {

  override suspend fun TransactionScope.runTransaction(): WorkItemAttempt {
    val workItemAttemptResult =
      WorkItemAttemptReader()
        .readByExternalIds(
          transactionContext,
          ExternalId(request.externalWorkItemId),
          ExternalId(request.externalWorkItemAttemptId),
        )
        ?: throw WorkItemAttemptNotFoundException(
          ExternalId(request.externalWorkItemId),
          ExternalId(request.externalWorkItemAttemptId),
        )

    transactionContext.bufferUpdateMutation("WorkItemAttempts") {
      set("WorkItemId" to workItemAttemptResult.workItemId.value)
      set("WorkItemAttemptId" to workItemAttemptResult.workItemAttemptId.value)
      set("State").toInt64(WorkItem.State.FAILED)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
    }

    return workItemAttemptResult.workItemAttempt.copy {
      state = WorkItemAttempt.State.FAILED
    }
  }

  override fun ResultScope<WorkItemAttempt>.buildResult(): WorkItemAttempt {
    return checkNotNull(transactionResult).copy { updateTime = commitTimestamp.toProto() }
  }
}
