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
import org.wfanet.measurement.internal.kingdom.StreamModelReleasesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelReleaseReader

class StreamModelReleases(
  private val requestFilter: StreamModelReleasesRequest.Filter,
  limit: Int = 0
) : SimpleSpannerQuery<ModelReleaseReader.Result>() {

  override val reader =
    ModelReleaseReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause(
        """
          ORDER BY ModelReleases.CreateTime ASC,
          ModelProviders.ExternalModelProviderId ASC,
          ModelSuites.ExternalModelSuiteId ASC,
          ModelReleases.ExternalModelReleaseId ASC
        """
          .trimIndent()
      )
      if (limit > 0) {
        appendClause("LIMIT @${LIMIT_PARAM}")
        bind(LIMIT_PARAM to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamModelReleasesRequest.Filter) {
    val conjuncts = mutableListOf<String>()

    if (filter.externalModelProviderId != 0L) {
      conjuncts.add("ExternalModelProviderId = @${EXTERNAL_MODEL_PROVIDER_ID}")
      bind(EXTERNAL_MODEL_PROVIDER_ID to filter.externalModelProviderId)
    }

    if (filter.externalModelSuiteId != 0L) {
      conjuncts.add("ExternalModelSuiteId = @${EXTERNAL_MODEL_SUITE_ID}")
      bind(EXTERNAL_MODEL_SUITE_ID to filter.externalModelSuiteId)
    }

    if (filter.hasAfter()) {
      conjuncts.add(
        """
          ((ModelReleases.CreateTime > @${CREATED_AFTER})
          OR (ModelReleases.CreateTime = @${CREATED_AFTER}
          AND ModelProviders.ExternalModelProviderId > @${EXTERNAL_MODEL_PROVIDER_ID})
          OR (ModelReleases.CreateTime = @${CREATED_AFTER}
          AND ModelProviders.ExternalModelProviderId = @${EXTERNAL_MODEL_PROVIDER_ID}
          AND ModelSuites.ExternalModelSuiteId > @${EXTERNAL_MODEL_SUITE_ID})
          OR (ModelReleases.CreateTime = @${CREATED_AFTER}
          AND ModelProviders.ExternalModelProviderId = @${EXTERNAL_MODEL_PROVIDER_ID}
          AND ModelSuites.ExternalModelSuiteId = @${EXTERNAL_MODEL_SUITE_ID}
          AND ModelReleases.ExternalModelReleaseId > @${EXTERNAL_MODEL_RELEASE_ID}))
        """
          .trimIndent()
      )
      bind(CREATED_AFTER to filter.after.createTime.toGcloudTimestamp())
      bind(EXTERNAL_MODEL_RELEASE_ID to filter.after.externalModelReleaseId)
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
    const val EXTERNAL_MODEL_RELEASE_ID = "externalModelReleaseId"
    const val EXTERNAL_MODEL_SUITE_ID = "externalModelSuiteId"
    const val EXTERNAL_MODEL_PROVIDER_ID = "externalModelProviderId"
    const val CREATED_AFTER = "createdAfter"
  }
}
