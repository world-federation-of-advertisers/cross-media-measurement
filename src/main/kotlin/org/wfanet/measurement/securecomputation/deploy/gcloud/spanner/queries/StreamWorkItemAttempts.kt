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
import org.wfanet.measurement.internal.securecomputation.controlplane.v1alpha.StreamWorkItemAttemptsRequest
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.readers.WorkItemAttemptReader

class StreamWorkItemAttempts(private val requestFilter: StreamWorkItemAttemptsRequest.Filter, limit: Int = 0) :
  SimpleSpannerQuery<WorkItemAttemptReader.Result>() {

  override val reader =
    WorkItemAttemptReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause(
        """
        ORDER BY CreateTime ASC,
        ExternalWorkItemId ASC,
        ExternalWorkItemAttemptId ASC
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

    if (filter.externalWorkItemId != 0L && filter.externalWorkItemAttemptIdAfter != 0L) {

      conjuncts.add(
        """
          (
            (WorkItemAttempts.ExternalWorkItemId  > @${EXTERNAL_WORK_ITEM_ID})
            OR (WorkItemAttempts.ExternalWorkItemId  = @${EXTERNAL_WORK_ITEM_ID}
            AND WorkItemAttempts.ExternalWorkItemAttemptId > @${EXTERNAL_WORK_ITEM_ATTEMPT_ID})
          )
        """
          .trimIndent()
      )

      conjuncts.add("ExternalWorkItemId = @${EXTERNAL_WORK_ITEM_ID}")
      conjuncts.add("ExternalWorkItemAttemptId = @${EXTERNAL_WORK_ITEM_ATTEMPT_ID}")
      bind(EXTERNAL_WORK_ITEM_ID to filter.externalWorkItemId)
      bind(EXTERNAL_WORK_ITEM_ATTEMPT_ID to filter.externalWorkItemAttemptIdAfter)
    }

    if (conjuncts.isEmpty()) {
      return
    }

    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  companion object {
    const val LIMIT = "limit"
    const val EXTERNAL_WORK_ITEM_ID = "externalWorkItemId"
    const val EXTERNAL_WORK_ITEM_ATTEMPT_ID = "externalWorkItemAttemptId"
  }
}
