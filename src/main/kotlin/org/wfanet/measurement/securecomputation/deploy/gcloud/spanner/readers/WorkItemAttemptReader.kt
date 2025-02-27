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

package org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.workItemAttempt

class WorkItemAttemptReader : SpannerReader<WorkItemAttemptReader.Result>() {

  data class Result(
    val workItemAttempt: WorkItemAttempt,
    val workItemAttemptId: InternalId,
    val workItemId: InternalId
  )

  override val baseSql: String =
    """
    SELECT
      WorkItemAttempts.WorkItemAttemptId,
      WorkItemAttempts.WorkItemId,
      WorkItems.WorkItemResourceId,
      WorkItemAttempts.WorkItemAttemptResourceId,
      WorkItemAttempts.State,
      WorkItemAttempts.AttemptNumber,
      WorkItemAttempts.Logs,
      WorkItemAttempts.CreateTime,
      WorkItemAttempts.UpdateTime
      FROM WorkItems
      JOIN WorkItemAttempts USING (WorkItemId)
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(
      buildWorkItemAttempt(struct),
      InternalId(struct.getLong("WorkItemId")),
      InternalId(struct.getLong("WorkItemAttemptId")),
    )

  private fun buildWorkItemAttempt(struct: Struct): WorkItemAttempt = workItemAttempt {
    workItemResourceId = struct.getLong("WorkItemResourceId")
    workItemAttemptResourceId = struct.getLong("WorkItemAttemptResourceId")
    state = WorkItemAttempt.State.forNumber(struct.getLong("State").toInt())
    errorMessage = struct.getString("ErrorMessage")
    createTime = struct.getTimestamp("CreateTime").toProto()
    updateTime = struct.getTimestamp("UpdateTime").toProto()
  }

  suspend fun readByResourceIds(
    readContext: AsyncDatabaseClient.ReadContext,
    workItemResourceId: ExternalId,
    workItemAttemptResourceId: ExternalId,
  ): Result? {
    return fillStatementBuilder {
      appendClause(
        """
          WHERE WorkItems.WorkItemResourceId = @workItemResourceId
          AND
          WorkItemAttempts.WorkItemAttemptResourceId = @workItemAttemptResourceId
          """
          .trimIndent()
      )
      bind("workItemResourceId").to(workItemResourceId.value)
      bind("workItemAttemptResourceId").to(workItemAttemptResourceId.value)
    }
      .execute(readContext)
      .singleOrNull()
  }
}
