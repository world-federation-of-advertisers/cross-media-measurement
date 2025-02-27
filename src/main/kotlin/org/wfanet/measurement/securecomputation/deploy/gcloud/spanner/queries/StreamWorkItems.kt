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
import org.wfanet.measurement.internal.securecomputation.controlplane.StreamWorkItemsRequest
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.readers.WorkItemReader

class StreamWorkItems(private val requestFilter: StreamWorkItemsRequest.Filter, limit: Int = 0) :
  SimpleSpannerQuery<WorkItemReader.Result>() {

  override val reader =
    WorkItemReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause(
        """
        ORDER BY CreateTime ASC,
        WorkItemResourceId ASC
        """
          .trimIndent()
      )
      if (limit > 0) {
        appendClause("LIMIT @${LIMIT}")
        bind(LIMIT to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamWorkItemsRequest.Filter) {
    val conjuncts = mutableListOf<String>()

    if (filter.workItemResourceIdAfter != 0L) {
      conjuncts.add("WorkItemResourceId > @${WORK_ITEM_RESOURCE_ID}")
      bind(WORK_ITEM_RESOURCE_ID to filter.workItemResourceIdAfter)
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
  }
}
