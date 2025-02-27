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

package org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.queries

import com.google.cloud.spanner.Statement
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.securecomputation.controlplane.StreamWorkItemAttemptsRequest
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.readers.WorkItemAttemptReader

class StreamWorkItemAttempts(private val requestFilter: StreamWorkItemAttemptsRequest.Filter, limit: Int = 0) :
  SimpleSpannerQuery<WorkItemAttemptReader.Result>() {

  override val reader =
    WorkItemAttemptReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause(
        """
        ORDER BY CreateTime ASC,
        WorkItemResourceId ASC,
        WorkItemAttemptResourceId ASC
        """
          .trimIndent()
      )
      if (limit > 0) {
        appendClause("LIMIT @${LIMIT}")
        bind(LIMIT to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamWorkItemAttemptsRequest.Filter) {
    val conjuncts = mutableListOf<String>()

    if (filter.workItemResourceId != 0L && filter.workItemAttemptResourceIdAfter != 0L) {

      conjuncts.add(
        """
          (
            (WorkItemAttempts.WorkItemResourceId  > @${WORK_ITEM_RESOURCE_ID})
            OR (WorkItemAttempts.WorkItemResourceId  = @${WORK_ITEM_RESOURCE_ID}
            AND WorkItemAttempts.WorkItemAttemptResourceId > @${WORK_ITEM_ATTEMPT_RESOURCE_ID})
          )
        """
          .trimIndent()
      )

      conjuncts.add("WorkItemResourceId = @${WORK_ITEM_RESOURCE_ID}")
      conjuncts.add("WorkItemAttemptResourceId = @${WORK_ITEM_ATTEMPT_RESOURCE_ID}")
      bind(WORK_ITEM_RESOURCE_ID to filter.workItemResourceId)
      bind(WORK_ITEM_ATTEMPT_RESOURCE_ID to filter.workItemAttemptResourceIdAfter)
    }

    if (conjuncts.isEmpty()) {
      return
    }

    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  companion object {
    const val LIMIT = "limit"
    const val WORK_ITEM_RESOURCE_ID = "workItemResourceId"
    const val WORK_ITEM_ATTEMPT_RESOURCE_ID = "workItemAttemptResourceId"
  }
}
