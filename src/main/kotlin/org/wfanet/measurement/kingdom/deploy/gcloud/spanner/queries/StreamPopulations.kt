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
import org.wfanet.measurement.internal.kingdom.StreamPopulationsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.PopulationReader

class StreamPopulations(
  private val requestFilter: StreamPopulationsRequest.Filter,
  limit: Int = 0,
) : SimpleSpannerQuery<PopulationReader.Result>() {
  override val reader =
    PopulationReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause(
        """
          ORDER BY Populations.CreateTime DESC,
          DataProviders.ExternalDataProviderId ASC,
          Populations.ExternalPopulationId ASC
        """
          .trimIndent()
      )
      if (limit > 0) {
        appendClause("LIMIT @${LIMIT_PARAM}")
        bind(LIMIT_PARAM to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamPopulationsRequest.Filter) {
    val conjuncts = mutableListOf<String>()

    if (filter.externalDataProviderId != 0L) {
      conjuncts.add("ExternalDataProviderId = @${EXTERNAL_DATA_PROVIDER_ID}")
      bind(EXTERNAL_DATA_PROVIDER_ID to filter.externalDataProviderId)
    }

    if (filter.hasAfter()) {
      conjuncts.add(
        """
          Populations.CreateTime < @${CREATE_TIME} OR (
            Populations.CreateTime = @${CREATE_TIME} AND DataProviders.ExternalDataProviderId > @${AFTER_EXTERNAL_DATA_PROVIDER_ID}
          ) OR (
            Populations.CreateTime = @${CREATE_TIME} AND DataProviders.ExternalDataProviderId = @${AFTER_EXTERNAL_DATA_PROVIDER_ID} AND Populations.ExternalPopulationId > @${EXTERNAL_POPULATION_ID}
          )
        """
          .trimIndent()
      )
      bind(CREATE_TIME to filter.after.createTime.toGcloudTimestamp())
      bind(EXTERNAL_POPULATION_ID to filter.after.externalPopulationId)
      bind(AFTER_EXTERNAL_DATA_PROVIDER_ID to filter.after.externalDataProviderId)
    }

    if (conjuncts.isEmpty()) {
      return
    }

    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  companion object {
    private const val LIMIT_PARAM = "limit"
    private const val EXTERNAL_DATA_PROVIDER_ID = "externalDataProviderId"
    private const val AFTER_EXTERNAL_DATA_PROVIDER_ID = "afterExternalDataProviderId"
    private const val EXTERNAL_POPULATION_ID = "externalPopulationId"
    private const val CREATE_TIME = "createTime"
  }
}
