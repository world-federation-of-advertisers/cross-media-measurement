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
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelRolloutReader

class StreamModelRollouts(
  private val requestFilter: StreamModelRolloutsRequest.Filter,
  limit: Int = 0,
) : SimpleSpannerQuery<ModelRolloutReader.Result>() {

  override val reader =
    ModelRolloutReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause(
        """
          ORDER BY ModelRollouts.RolloutPeriodStartTime ASC,
          ModelProviders.ExternalModelProviderId ASC,
          ModelSuites.ExternalModelSuiteId ASC,
          ModelLines.ExternalModelLineId ASC,
          ModelRollouts.ExternalModelRolloutId ASC
          """
          .trimIndent()
      )
      if (limit > 0) {
        appendClause("LIMIT @${LIMIT_PARAM}")
        bind(LIMIT_PARAM to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamModelRolloutsRequest.Filter) {
    val conjuncts = buildList {
      if (filter.externalModelProviderId != 0L) {
        add("ExternalModelProviderId = @${EXTERNAL_MODEL_PROVIDER_ID}")
        bind(EXTERNAL_MODEL_PROVIDER_ID to filter.externalModelProviderId)
      }

      if (filter.externalModelSuiteId != 0L) {
        add("ExternalModelSuiteId = @${EXTERNAL_MODEL_SUITE_ID}")
        bind(EXTERNAL_MODEL_SUITE_ID to filter.externalModelSuiteId)
      }

      if (filter.externalModelLineId != 0L) {
        add("ExternalModelLineId = @${EXTERNAL_MODEL_LINE_ID}")
        bind(EXTERNAL_MODEL_LINE_ID to filter.externalModelLineId)
      }

      if (filter.externalModelReleaseIdInList.isNotEmpty()) {
        add("ExternalModelReleaseId IN UNNEST(@$EXTERNAL_MODEL_RELEASE_IDS)")
        bind(EXTERNAL_MODEL_RELEASE_IDS).toInt64Array(filter.externalModelReleaseIdInList)
      }

      if (filter.hasRolloutPeriod()) {
        add(
          """
          ModelRollouts.RolloutPeriodStartTime >= @${ROLLOUT_PERIOD_START_TIME}
          AND ModelRollouts.RolloutPeriodEndTime < @${ROLLOUT_PERIOD_END_TIME}
        """
            .trimIndent()
        )
        bind(
          ROLLOUT_PERIOD_START_TIME to
            filter.rolloutPeriod.rolloutPeriodStartTime.toGcloudTimestamp()
        )
        bind(
          ROLLOUT_PERIOD_END_TIME to filter.rolloutPeriod.rolloutPeriodEndTime.toGcloudTimestamp()
        )
      }

      if (filter.hasAfter()) {
        add(
          """
          (
            ModelRollouts.RolloutPeriodStartTime > @${PageParams.ROLLOUT_PERIOD_START_TIME}
            OR (
              ModelRollouts.RolloutPeriodStartTime = @${PageParams.ROLLOUT_PERIOD_START_TIME}
              AND (
                ModelProviders.ExternalModelProviderId > @${PageParams.EXTERNAL_MODEL_PROVIDER_ID}
                OR (
                  ModelProviders.ExternalModelProviderId = @${PageParams.EXTERNAL_MODEL_PROVIDER_ID}
                  AND (
                    ModelSuites.ExternalModelSuiteId > @${PageParams.EXTERNAL_MODEL_SUITE_ID}
                    OR (
                      ModelSuites.ExternalModelSuiteId = @${PageParams.EXTERNAL_MODEL_SUITE_ID}
                      AND ModelRollouts.ExternalModelRolloutId > @${PageParams.EXTERNAL_MODEL_ROLLOUT_ID}
                    )
                  )
                )
              )
            )
          )
          """
            .trimIndent()
        )
        bind(
          PageParams.ROLLOUT_PERIOD_START_TIME to
            filter.after.rolloutPeriodStartTime.toGcloudTimestamp()
        )
        bind(PageParams.EXTERNAL_MODEL_PROVIDER_ID to filter.after.externalModelProviderId)
        bind(PageParams.EXTERNAL_MODEL_SUITE_ID to filter.after.externalModelSuiteId)
        bind(PageParams.EXTERNAL_MODEL_LINE_ID to filter.after.externalModelLineId)
        bind(PageParams.EXTERNAL_MODEL_ROLLOUT_ID to filter.after.externalModelRolloutId)
      }
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
    const val EXTERNAL_MODEL_SUITE_ID = "externalModelSuiteId"
    const val EXTERNAL_MODEL_LINE_ID = "externalModelLineId"
    const val ROLLOUT_PERIOD_START_TIME = "rolloutPeriodStartTime"
    const val ROLLOUT_PERIOD_END_TIME = "rolloutPeriodEndTime"
    const val EXTERNAL_MODEL_RELEASE_IDS = "externalModelReleaseIds"

    object PageParams {
      const val EXTERNAL_MODEL_PROVIDER_ID = "externalModelProviderId_after"
      const val EXTERNAL_MODEL_SUITE_ID = "externalModelSuiteId_after"
      const val EXTERNAL_MODEL_LINE_ID = "externalModelLineId_after"
      const val EXTERNAL_MODEL_ROLLOUT_ID = "externalModelRolloutId_after"
      const val ROLLOUT_PERIOD_START_TIME = "rolloutPeriodStartTime_after"
    }
  }
}
