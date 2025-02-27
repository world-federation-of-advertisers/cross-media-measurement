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
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.workItem

class WorkItemReader : SpannerReader<WorkItemReader.Result>() {

  data class Result(val workItem: WorkItem, val workItemId: InternalId)

  override val baseSql: String =
    """
    SELECT
      WorkItemId,
      WorkItemResourceId,
      Queue,
      State,
      CreateTime,
      UpdateTime,
      FROM WorkItems
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(buildWorkItem(struct), InternalId(struct.getLong("ModelReleaseId")))

  private fun buildWorkItem(struct: Struct): WorkItem = workItem {
    workItemResourceId = struct.getLong("WorkItemResourceId")
    queueResourceId = struct.getString("Queue")
    state = WorkItem.State.forNumber(struct.getLong("State").toInt())
    createTime = struct.getTimestamp("CreateTime").toProto()
    updateTime = struct.getTimestamp("UpdateTime").toProto()
  }

  suspend fun readByResourceId(
    readContext: AsyncDatabaseClient.ReadContext,
    workItemResourceId: ExternalId,
  ): Result? {
    return fillStatementBuilder {
      appendClause(
        """
          WHERE WorkItemResourceId = @workItemResourceId
          """
          .trimIndent()
      )
      bind("workItemResourceId").to(workItemResourceId.value)
    }
      .execute(readContext)
      .singleOrNull()
  }
}
