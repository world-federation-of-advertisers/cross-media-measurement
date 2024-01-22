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
import org.wfanet.measurement.internal.kingdom.StreamModelSuitesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelSuiteReader

class StreamModelSuites(
  private val requestFilter: StreamModelSuitesRequest.Filter,
  limit: Int = 0,
) : SimpleSpannerQuery<ModelSuiteReader.Result>() {

  override val reader =
    ModelSuiteReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause(
        "ORDER BY ModelSuites.CreateTime ASC, ModelProviders.ExternalModelProviderId ASC, ModelSuites.ExternalModelSuiteId ASC"
      )
      if (limit > 0) {
        appendClause("LIMIT @${LIMIT_PARAM}")
        bind(LIMIT_PARAM to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamModelSuitesRequest.Filter) {
    val conjuncts = mutableListOf<String>()

    if (filter.externalModelProviderId != 0L) {
      conjuncts.add("ExternalModelProviderId = @${EXTERNAL_MODEL_PROVIDER_ID}")
      bind(EXTERNAL_MODEL_PROVIDER_ID to filter.externalModelProviderId)
    }

    if (filter.hasAfter()) {
      conjuncts.add(
        """
          ((ModelSuites.CreateTime  > @${CREATED_AFTER})
          OR (ModelSuites.CreateTime  = @${CREATED_AFTER}
          AND ModelProviders.ExternalModelProviderId = @${EXTERNAL_MODEL_PROVIDER_ID}
          AND ModelSuites.ExternalModelSuiteId > @${EXTERNAL_MODEL_SUITE_ID})
          OR (ModelSuites.CreateTime  = @${CREATED_AFTER}
          AND ModelProviders.ExternalModelProviderId > @${EXTERNAL_MODEL_PROVIDER_ID}))
        """
          .trimIndent()
      )
      bind(CREATED_AFTER to filter.after.createTime.toGcloudTimestamp())
      bind(EXTERNAL_MODEL_SUITE_ID to filter.after.externalModelSuiteId)
      bind(EXTERNAL_MODEL_PROVIDER_ID to filter.after.externalModelProviderId)
    }

    if (conjuncts.isEmpty()) {
      return
    }

    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  companion object {
    const val LIMIT_PARAM = "limit"
    const val EXTERNAL_MODEL_PROVIDER_ID = "externalModelProviderId"
    const val CREATED_AFTER = "createdAfter"
    const val EXTERNAL_MODEL_SUITE_ID = "externalModelSuiteId"
  }
}
