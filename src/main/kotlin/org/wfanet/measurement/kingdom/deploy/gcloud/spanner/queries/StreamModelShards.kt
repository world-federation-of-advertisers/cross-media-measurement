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
import org.wfanet.measurement.internal.kingdom.StreamModelShardsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelShardReader

class StreamModelShards(
  private val requestFilter: StreamModelShardsRequest.Filter,
  limit: Int = 0
) : SimpleSpannerQuery<ModelShardReader.Result>() {

  override val reader =
    ModelShardReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause("ORDER BY ModelShards.CreateTime ASC")
      if (limit > 0) {
        appendClause("LIMIT @${LIMIT_PARAM}")
        bind(LIMIT_PARAM to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamModelShardsRequest.Filter) {
    val conjuncts = mutableListOf<String>()

    if (filter.externalDataProviderId != 0L) {
      conjuncts.add("ExternalDataProviderId = @${EXTERNAL_DATA_PROVIDER_ID_PARAM}")
      bind(EXTERNAL_DATA_PROVIDER_ID_PARAM to filter.externalDataProviderId)
    }

    if (filter.hasCreatedAfter()) {
      conjuncts.add("ModelShards.CreateTime > @${CREATED_AFTER}")
      bind(CREATED_AFTER to filter.createdAfter.toGcloudTimestamp())
    }

    if (conjuncts.isEmpty()) {
      return
    }

    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  companion object {
    const val LIMIT_PARAM = "limit"
    const val EXTERNAL_DATA_PROVIDER_ID_PARAM = "externalDataProviderId"
    const val CREATED_AFTER = "createdAfter"
  }
}
