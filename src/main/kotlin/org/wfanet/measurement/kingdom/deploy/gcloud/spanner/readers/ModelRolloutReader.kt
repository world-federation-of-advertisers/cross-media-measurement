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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.internal.kingdom.ModelRollout
import org.wfanet.measurement.internal.kingdom.modelRollout

class ModelRolloutReader : SpannerReader<ModelRolloutReader.Result>() {

  data class Result(
    val modelRollout: ModelRollout,
    val modelRolloutId: InternalId,
    val modelLineId: InternalId,
    val modelSuiteId: InternalId,
    val modelProviderId: InternalId,
  )

  override val baseSql: String =
    """
    SELECT
      ModelRollouts.ModelProviderId,
      ModelRollouts.ModelSuiteId,
      ModelRollouts.ModelLineId,
      ModelRollouts.ModelRolloutId,
      ModelRollouts.ExternalModelRolloutId,
      ModelRollouts.RolloutPeriodStartTime,
      ModelRollouts.RolloutPeriodEndTime,
      ModelRollouts.RolloutFreezeTime,
      ModelRollouts.CreateTime,
      ModelRollouts.UpdateTime,
      ModelLines.ExternalModelLineId,
      ModelSuites.ExternalModelSuiteId,
      ModelProviders.ExternalModelProviderId,
      PreviousModelRollout.ExternalModelRolloutId AS PreviousExternalModelRolloutId,
      ModelReleases.ExternalModelReleaseId
      FROM ModelRollouts
      JOIN ModelLines USING (ModelProviderId, ModelSuiteId, ModelLineId)
      JOIN ModelSuites USING (ModelProviderId, ModelSuiteId)
      JOIN ModelProviders USING (ModelProviderId)
      LEFT JOIN ModelRollouts as PreviousModelRollout ON (
        ModelRollouts.ModelProviderId = PreviousModelRollout.ModelProviderId
        AND ModelRollouts.ModelSuiteId = PreviousModelRollout.ModelSuiteId
        AND ModelRollouts.PreviousModelRolloutId = PreviousModelRollout.ModelRolloutId
      )
      JOIN ModelReleases ON (
        ModelRollouts.ModelProviderId = ModelReleases.ModelProviderId
        AND ModelRollouts.ModelSuiteId = ModelReleases.ModelSuiteId
        AND ModelRollouts.ModelReleaseId = ModelReleases.ModelReleaseId
      )
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(
      buildModelRollout(struct),
      InternalId(struct.getLong("ModelRolloutId")),
      InternalId(struct.getLong("ModelLineId")),
      InternalId(struct.getLong("ModelSuiteId")),
      InternalId(struct.getLong("ModelProviderId")),
    )

  private fun buildModelRollout(struct: Struct): ModelRollout = modelRollout {
    externalModelProviderId = struct.getLong("ExternalModelProviderId")
    externalModelSuiteId = struct.getLong("ExternalModelSuiteId")
    externalModelLineId = struct.getLong("ExternalModelLineId")
    externalModelRolloutId = struct.getLong("ExternalModelRolloutId")
    rolloutPeriodStartTime = struct.getTimestamp("RolloutPeriodStartTime").toProto()
    rolloutPeriodEndTime = struct.getTimestamp("RolloutPeriodEndTime").toProto()
    if (!struct.isNull("RolloutFreezeTime")) {
      rolloutFreezeTime = struct.getTimestamp("RolloutFreezeTime").toProto()
    }
    if (!struct.isNull("PreviousExternalModelRolloutId")) {
      externalPreviousModelRolloutId = struct.getLong("PreviousExternalModelRolloutId")
    }
    externalModelReleaseId = struct.getLong("ExternalModelReleaseId")
    createTime = struct.getTimestamp("CreateTime").toProto()
    if (!struct.isNull("UpdateTime")) {
      updateTime = struct.getTimestamp("UpdateTime").toProto()
    }
  }

  suspend fun readByExternalModelRolloutId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalModelProviderId: ExternalId,
    externalModelSuiteId: ExternalId,
    externalModelLineId: ExternalId,
    externalModelRolloutId: ExternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          "WHERE ExternalModelSuiteId = @externalModelSuiteId AND ExternalModelProviderId = @externalModelProviderId AND ExternalModelLineId = @externalModelLineId AND ModelRollouts.ExternalModelRolloutId = @externalModelRolloutId"
        )
        bind("externalModelSuiteId").to(externalModelSuiteId.value)
        bind("externalModelProviderId").to(externalModelProviderId.value)
        bind("externalModelLineId").to(externalModelLineId.value)
        bind("externalModelRolloutId").to(externalModelRolloutId.value)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }
}
