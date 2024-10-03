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
        ORDER BY
          ModelRollouts.RolloutPeriodStartTime ASC,
          ModelProviders.ExternalModelProviderId ASC,
          ModelSuites.ExternalModelSuiteId ASC,
          ModelLines.ExternalModelLineId ASC,
          ModelRollouts.ExternalModelRolloutId ASC
        """
          .trimIndent()
      )
      if (limit > 0) {
        appendClause("LIMIT @${Params.LIMIT}")
        bind(Params.LIMIT to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamModelRolloutsRequest.Filter) {
    val conjuncts = mutableListOf<String>()

    if (filter.externalModelProviderId != 0L) {
      conjuncts.add("ExternalModelProviderId = @${Params.EXTERNAL_MODEL_PROVIDER_ID}")
      bind(Params.EXTERNAL_MODEL_PROVIDER_ID to filter.externalModelProviderId)
    }

    if (filter.externalModelSuiteId != 0L) {
      conjuncts.add("ExternalModelSuiteId = @${Params.EXTERNAL_MODEL_SUITE_ID}")
      bind(Params.EXTERNAL_MODEL_SUITE_ID to filter.externalModelSuiteId)
    }

    if (filter.externalModelLineId != 0L) {
      conjuncts.add("ExternalModelLineId = @${Params.EXTERNAL_MODEL_LINE_ID}")
      bind(Params.EXTERNAL_MODEL_LINE_ID to filter.externalModelLineId)
    }

    if (filter.hasRolloutPeriod()) {
      conjuncts.add(
        """
          ModelRollouts.RolloutPeriodStartTime >= @${Params.ROLLOUT_PERIOD_START_TIME}
          AND ModelRollouts.RolloutPeriodEndTime < @${Params.ROLLOUT_PERIOD_END_TIME}
        """
          .trimIndent()
      )
      bind(
        Params.ROLLOUT_PERIOD_START_TIME to
          filter.rolloutPeriod.rolloutPeriodStartTime.toGcloudTimestamp()
      )
      bind(
        Params.ROLLOUT_PERIOD_END_TIME to
          filter.rolloutPeriod.rolloutPeriodEndTime.toGcloudTimestamp()
      )
    }

    if (filter.hasAfter()) {
      // CASE implements short-circuiting.
      conjuncts.add(
        """
        CASE
          WHEN
            ModelRollouts.RolloutPeriodStartTime > @${AfterParams.ROLLOUT_PERIOD_START_TIME}
            THEN TRUE
          WHEN
            ModelRollouts.RolloutPeriodStartTime = @${AfterParams.ROLLOUT_PERIOD_START_TIME}
            AND ModelProviders.ExternalModelProviderId > @${AfterParams.EXTERNAL_MODEL_PROVIDER_ID}
            THEN TRUE
          WHEN
            ModelRollouts.RolloutPeriodStartTime = @${AfterParams.ROLLOUT_PERIOD_START_TIME}
            AND ModelProviders.ExternalModelProviderId = @${AfterParams.EXTERNAL_MODEL_PROVIDER_ID}
            AND ModelSuites.ExternalModelSuiteId > @${AfterParams.EXTERNAL_MODEL_SUITE_ID}
            THEN TRUE
          WHEN
            ModelRollouts.RolloutPeriodStartTime = @${AfterParams.ROLLOUT_PERIOD_START_TIME}
            AND ModelProviders.ExternalModelProviderId = @${AfterParams.EXTERNAL_MODEL_PROVIDER_ID}
            AND ModelSuites.ExternalModelSuiteId = @${AfterParams.EXTERNAL_MODEL_SUITE_ID}
            AND ModelLines.ExternalModelLineId > @${AfterParams.EXTERNAL_MODEL_LINE_ID}
            THEN TRUE
          WHEN
            ModelRollouts.RolloutPeriodStartTime = @${AfterParams.ROLLOUT_PERIOD_START_TIME}
            AND ModelProviders.ExternalModelProviderId = @${AfterParams.EXTERNAL_MODEL_PROVIDER_ID}
            AND ModelSuites.ExternalModelSuiteId = @${AfterParams.EXTERNAL_MODEL_SUITE_ID}
            AND ModelLines.ExternalModelLineId = @${AfterParams.EXTERNAL_MODEL_LINE_ID}
            AND ModelRollouts.ExternalModelRolloutId > @${AfterParams.EXTERNAL_MODEL_ROLLOUT_ID}
            THEN TRUE
          ELSE
            FALSE
        END
        """
          .trimIndent()
      )
      bind(
        AfterParams.ROLLOUT_PERIOD_START_TIME to
          filter.after.rolloutPeriodStartTime.toGcloudTimestamp()
      )
      bind(AfterParams.EXTERNAL_MODEL_PROVIDER_ID to filter.after.externalModelProviderId)
      bind(AfterParams.EXTERNAL_MODEL_SUITE_ID to filter.after.externalModelSuiteId)
      bind(AfterParams.EXTERNAL_MODEL_LINE_ID to filter.after.externalModelLineId)
      bind(AfterParams.EXTERNAL_MODEL_ROLLOUT_ID to filter.after.externalModelRolloutId)
    }

    if (conjuncts.isEmpty()) {
      return
    }

    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  companion object {
    private object Params {
      const val LIMIT = "limit"
      const val EXTERNAL_MODEL_PROVIDER_ID = "externalModelProviderId"
      const val EXTERNAL_MODEL_SUITE_ID = "externalModelSuiteId"
      const val EXTERNAL_MODEL_LINE_ID = "externalModelLineId"
      const val ROLLOUT_PERIOD_START_TIME = "rolloutPeriodStartTime"
      const val ROLLOUT_PERIOD_END_TIME = "rolloutPeriodEndTime"
    }

    private object AfterParams {
      const val ROLLOUT_PERIOD_START_TIME = "after_rolloutPeriodStartTime"
      const val EXTERNAL_MODEL_PROVIDER_ID = "after_externalModelProviderId"
      const val EXTERNAL_MODEL_SUITE_ID = "after_externalModelSuiteId"
      const val EXTERNAL_MODEL_LINE_ID = "after_externalModelLineId"
      const val EXTERNAL_MODEL_ROLLOUT_ID = "after_externalModelRolloutId"
    }
  }
}
