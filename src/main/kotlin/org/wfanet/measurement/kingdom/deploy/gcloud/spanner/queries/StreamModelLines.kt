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

class StreamModelLines(requestFilter: StreamModelLinesRequest.Filter, limit: Int = 0) :
  SimpleSpannerQuery<ModelLineReader.Result>() {

  override val reader =
    ModelLineReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause(
        """
        ORDER BY
          ModelLines.CreateTime ASC,
          ModelProviders.ExternalModelProviderId ASC,
          ModelSuites.ExternalModelSuiteId ASC,
          ModelLines.ExternalModelLineId ASC
        """
          .trimIndent()
      )
      if (limit > 0) {
        appendClause("LIMIT @${LIMIT}")
        bind(LIMIT to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamModelLinesRequest.Filter) {
    val conjuncts = mutableListOf<String>()

    if (filter.externalModelProviderId != 0L) {
      conjuncts.add("ExternalModelProviderId = @${EXTERNAL_MODEL_PROVIDER_ID}")
      bind(EXTERNAL_MODEL_PROVIDER_ID to filter.externalModelProviderId)
    }

    if (filter.externalModelSuiteId != 0L) {
      conjuncts.add("ExternalModelSuiteId = @${EXTERNAL_MODEL_SUITE_ID}")
      bind(EXTERNAL_MODEL_SUITE_ID to filter.externalModelSuiteId)
    }

    if (filter.hasActiveIntervalContains()) {
      require(filter.activeIntervalContains.hasStartTime())
      require(filter.activeIntervalContains.hasEndTime())
      conjuncts.add("ModelLines.ActiveStartTime <= @$ACTIVE_START_TIME")
      conjuncts.add("IFNULL(ModelLines.ActiveEndTime, @$TIMESTAMP_MAX) >= @$ACTIVE_END_TIME")
      bind(ACTIVE_START_TIME).to(filter.activeIntervalContains.startTime.toGcloudTimestamp())
      bind(ACTIVE_END_TIME).to(filter.activeIntervalContains.endTime.toGcloudTimestamp())
      bind(TIMESTAMP_MAX).to(com.google.cloud.Timestamp.MAX_VALUE)
    }

    if (filter.hasAfter()) {
      conjuncts.add(
        """
        (
          ModelLines.CreateTime > @${PageParams.CREATE_TIME}
          OR (
            ModelLines.CreateTime = @${PageParams.CREATE_TIME}
            AND (
              ModelProviders.ExternalModelProviderId > @${PageParams.EXTERNAL_MODEL_PROVIDER_ID}
              OR (
                ModelProviders.ExternalModelProviderId = @${PageParams.EXTERNAL_MODEL_PROVIDER_ID}
                AND (
                  ModelSuites.ExternalModelSuiteId > @${PageParams.EXTERNAL_MODEL_SUITE_ID}
                  OR (
                    ModelSuites.ExternalModelSuiteId = @${PageParams.EXTERNAL_MODEL_SUITE_ID}
                    AND ModelLines.ExternalModelLineId > @${PageParams.EXTERNAL_MODEL_LINE_ID}
                  )
                )
              )
            )
          )
        )
        """
          .trimIndent()
      )
      bind(PageParams.CREATE_TIME).to(filter.after.createTime.toGcloudTimestamp())
      bind(PageParams.EXTERNAL_MODEL_PROVIDER_ID).to(filter.after.externalModelProviderId)
      bind(PageParams.EXTERNAL_MODEL_SUITE_ID).to(filter.after.externalModelSuiteId)
      bind(PageParams.EXTERNAL_MODEL_LINE_ID).to(filter.after.externalModelLineId)
    }

    if (filter.typeValueList.isNotEmpty()) {
      conjuncts.add("ModelLines.Type IN UNNEST(@${TYPES})")
      bind(TYPES).toInt64Array(filter.typeValueList.map { it.toLong() })
    }

    if (conjuncts.isEmpty()) {
      return
    }

    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  companion object {
    private const val LIMIT = "limit"
    private const val EXTERNAL_MODEL_PROVIDER_ID = "externalModelProviderId"
    private const val EXTERNAL_MODEL_SUITE_ID = "externalModelSuiteId"
    private const val TYPES = "types"
    private const val ACTIVE_START_TIME = "activeStartTime"
    private const val ACTIVE_END_TIME = "activeEndTime"
    private const val TIMESTAMP_MAX = "timestampMax"

    private object PageParams {
      const val CREATE_TIME = "createTime_after"
      const val EXTERNAL_MODEL_PROVIDER_ID = "externalModelProviderId_after"
      const val EXTERNAL_MODEL_SUITE_ID = "externalModelSuiteId_after"
      const val EXTERNAL_MODEL_LINE_ID = "externalModelLineId_after"
    }
  }
}
