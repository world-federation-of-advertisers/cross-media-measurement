/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import com.google.cloud.spanner.Statement
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelLineReader

class StreamModelLines(private val requestFilter: StreamModelLinesRequest.Filter, limit: Int = 0) :
  SimpleSpannerQuery<ModelLineReader.Result>() {

  override val reader =
    ModelLineReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause("ORDER BY ModelLines.CreateTime ASC")
      if (limit > 0) {
        appendClause("LIMIT @${LIMIT_PARAM}")
        bind(LIMIT_PARAM to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamModelLinesRequest.Filter) {
    val conjuncts = mutableListOf<String>()

    if (filter.externalModelProviderId != 0L) {
      conjuncts.add("ExternalModelProviderId = @${EXTERNAL_MODEL_PROVIDER_ID_PARAM}")
      bind(EXTERNAL_MODEL_PROVIDER_ID_PARAM to filter.externalModelProviderId)
    }

    if (filter.externalModelSuiteId != 0L) {
      conjuncts.add("ExternalModelSuiteId = @${EXTERNAL_MODEL_SUITE_ID_PARAM}")
      bind(EXTERNAL_MODEL_SUITE_ID_PARAM to filter.externalModelSuiteId)
    }

    if (filter.hasCreatedAfter()) {
      conjuncts.add("ModelLines.CreateTime > @${CREATED_AFTER}")
      bind(CREATED_AFTER to filter.createdAfter.toGcloudTimestamp())
    }

    if (filter.typeValueList.isNotEmpty()) {
      conjuncts.add("ModelLines.Type IN UNNEST(@${STATES_PARAM})")
      bind(STATES_PARAM).toInt64Array(filter.typeValueList.map { it.toLong() })
    }

    check(conjuncts.isNotEmpty())
    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  companion object {
    const val LIMIT_PARAM = "limit"
    const val EXTERNAL_MODEL_PROVIDER_ID_PARAM = "externalModelProviderId"
    const val EXTERNAL_MODEL_SUITE_ID_PARAM = "externalModelSuiteId"
    const val CREATED_AFTER = "createdAfter"
    const val STATES_PARAM = "states"
  }
}
