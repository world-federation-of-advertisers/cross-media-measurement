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
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.kingdom.ModelOutage
import org.wfanet.measurement.internal.kingdom.StreamModelOutagesRequest.Filter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelOutageReader

class StreamModelOutages(private val requestFilter: Filter, limit: Int = 0) :
  SimpleSpannerQuery<ModelOutageReader.Result>() {

  override val reader =
    ModelOutageReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause(
        """
          ORDER BY ModelOutages.CreateTime ASC,
          ModelProviders.ExternalModelProviderId ASC,
          ModelSuites.ExternalModelSuiteId ASC,
          ModelLines.ExternalModelLineId ASC,
          ModelOutages.ExternalModelOutageId ASC
          """
          .trimIndent()
      )
      if (limit > 0) {
        appendClause("LIMIT @${LIMIT}")
        bind(LIMIT to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: Filter) {
    val conjuncts = mutableListOf<String>()

    if (filter.externalModelProviderId != 0L) {
      conjuncts.add("ExternalModelProviderId = @${EXTERNAL_MODEL_PROVIDER_ID}")
      bind(EXTERNAL_MODEL_PROVIDER_ID to filter.externalModelProviderId)
    }

    if (filter.externalModelSuiteId != 0L) {
      conjuncts.add("ExternalModelSuiteId = @${EXTERNAL_MODEL_SUITE_ID}")
      bind(EXTERNAL_MODEL_SUITE_ID to filter.externalModelSuiteId)
    }

    if (filter.externalModelLineId != 0L) {
      conjuncts.add("ExternalModelLineId = @${EXTERNAL_MODEL_LINE_ID}")
      bind(EXTERNAL_MODEL_LINE_ID to filter.externalModelLineId)
    }

    if (!filter.showDeleted) {
      conjuncts.add("State != @${DELETED_STATE}")
      bind(DELETED_STATE).toInt64(ModelOutage.State.DELETED)
    }

    if (filter.hasOutageInterval()) {
      conjuncts.add(
        """
          ModelOutages.OutageStartTime >= @${OUTAGE_START_TIME}
          AND ModelOutages.OutageEndTime < @${OUTAGE_END_TIME}
        """
          .trimIndent()
      )
      bind(OUTAGE_START_TIME to filter.outageInterval.modelOutageStartTime.toGcloudTimestamp())
      bind(OUTAGE_END_TIME to filter.outageInterval.modelOutageEndTime.toGcloudTimestamp())
    }

    if (filter.hasAfter()) {
      conjuncts.add(
        """
          ((ModelOutages.CreateTime > @${CREATED_AFTER})
          OR (ModelOutages.CreateTime = @${CREATED_AFTER}
          AND ModelProviders.ExternalModelProviderId > @${EXTERNAL_MODEL_PROVIDER_ID})
          OR (ModelOutages.CreateTime = @${CREATED_AFTER}
          AND ModelProviders.ExternalModelProviderId = @${EXTERNAL_MODEL_PROVIDER_ID}
          AND ModelSuites.ExternalModelSuiteId > @${EXTERNAL_MODEL_SUITE_ID})
          OR (ModelOutages.CreateTime = @${CREATED_AFTER}
          AND ModelProviders.ExternalModelProviderId = @${EXTERNAL_MODEL_PROVIDER_ID}
          AND ModelSuites.ExternalModelSuiteId = @${EXTERNAL_MODEL_SUITE_ID}
          AND ModelLines.ExternalModelLineId > @${EXTERNAL_MODEL_LINE_ID})
          OR (ModelOutages.CreateTime = @${CREATED_AFTER}
          AND ModelProviders.ExternalModelProviderId = @${EXTERNAL_MODEL_PROVIDER_ID}
          AND ModelSuites.ExternalModelSuiteId = @${EXTERNAL_MODEL_SUITE_ID}
          AND ModelLines.ExternalModelLineId = @${EXTERNAL_MODEL_LINE_ID}
          AND ModelOutages.ExternalModelOutageId > @${EXTERNAL_MODEL_OUTAGE_ID}))
        """
          .trimIndent()
      )
      bind(CREATED_AFTER to filter.after.createTime.toGcloudTimestamp())
      bind(EXTERNAL_MODEL_PROVIDER_ID to filter.after.externalModelProviderId)
      bind(EXTERNAL_MODEL_SUITE_ID to filter.after.externalModelSuiteId)
      bind(EXTERNAL_MODEL_LINE_ID to filter.after.externalModelLineId)
      bind(EXTERNAL_MODEL_OUTAGE_ID to filter.after.externalModelOutageId)
    }

    if (conjuncts.isEmpty()) {
      return
    }

    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  companion object {
    const val LIMIT = "limit"
    const val EXTERNAL_MODEL_PROVIDER_ID = "externalModelProviderId"
    const val EXTERNAL_MODEL_SUITE_ID = "externalModelSuiteId"
    const val EXTERNAL_MODEL_LINE_ID = "externalModelLineId"
    const val EXTERNAL_MODEL_OUTAGE_ID = "externalModelOutageId"
    const val OUTAGE_START_TIME = "outageStartTime"
    const val OUTAGE_END_TIME = "outageEndTime"
    const val CREATED_AFTER = "createdAfter"
    const val DELETED_STATE = "deletedState"
  }
}
