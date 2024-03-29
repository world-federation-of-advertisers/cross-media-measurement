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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import com.google.protobuf.util.Timestamps
import java.time.Clock
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.common.toInstant
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.kingdom.ModelRollout
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelReleaseNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelRolloutInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelReleaseReader

class CreateModelRollout(private val modelRollout: ModelRollout, private val clock: Clock) :
  SpannerWriter<ModelRollout, ModelRollout>() {

  private data class LatestModelRolloutResult(
    val modelRolloutId: Long,
    val externalModelRolloutId: Long,
    val rolloutPeriodStartTime: Timestamp?,
  )

  override suspend fun TransactionScope.runTransaction(): ModelRollout {
    val now = clock.instant().toProtoTime()
    if (Timestamps.compare(now, modelRollout.rolloutPeriodStartTime) >= 0) {
      throw ModelRolloutInvalidArgsException(
        ExternalId(modelRollout.externalModelProviderId),
        ExternalId(modelRollout.externalModelSuiteId),
        ExternalId(modelRollout.externalModelLineId),
      ) {
        "RolloutPeriodStartTime must be in the future."
      }
    }
    if (
      Timestamps.compare(modelRollout.rolloutPeriodStartTime, modelRollout.rolloutPeriodEndTime) > 0
    ) {
      throw ModelRolloutInvalidArgsException(
        ExternalId(modelRollout.externalModelProviderId),
        ExternalId(modelRollout.externalModelSuiteId),
        ExternalId(modelRollout.externalModelLineId),
      ) {
        "RolloutPeriodEndTime cannot precede RolloutPeriodStartTime."
      }
    }

    val latestModelRolloutData =
      readLatestModelRolloutData(
        ExternalId(modelRollout.externalModelProviderId),
        ExternalId(modelRollout.externalModelSuiteId),
        ExternalId(modelRollout.externalModelLineId),
      )

    val latestModelRolloutStartTime = latestModelRolloutData?.rolloutPeriodStartTime

    // New ModelRollout cannot have a RolloutStartTime that precedes the RolloutStartTime of the
    // current PreviousModelRollout
    if (
      latestModelRolloutStartTime != null &&
        latestModelRolloutStartTime.toInstant() > modelRollout.rolloutPeriodStartTime.toInstant()
    ) {
      throw ModelRolloutInvalidArgsException(
        ExternalId(modelRollout.externalModelProviderId),
        ExternalId(modelRollout.externalModelSuiteId),
        ExternalId(modelRollout.externalModelLineId),
      ) {
        "RolloutPeriodStartTime cannot precede that of previous ModelRollout."
      }
    }
    val modelLineData =
      readModelLineData(
        ExternalId(modelRollout.externalModelProviderId),
        ExternalId(modelRollout.externalModelSuiteId),
        ExternalId(modelRollout.externalModelLineId),
      )
        ?: throw ModelLineNotFoundException(
          ExternalId(modelRollout.externalModelProviderId),
          ExternalId(modelRollout.externalModelSuiteId),
          ExternalId(modelRollout.externalModelLineId),
        )

    val modelReleaseResult =
      ModelReleaseReader()
        .readByExternalIds(
          transactionContext,
          ExternalId(modelRollout.externalModelReleaseId),
          ExternalId(modelRollout.externalModelSuiteId),
          ExternalId(modelRollout.externalModelProviderId),
        )
        ?: throw ModelReleaseNotFoundException(
          ExternalId(modelRollout.externalModelProviderId),
          ExternalId(modelRollout.externalModelSuiteId),
          ExternalId(modelRollout.externalModelReleaseId),
        )

    val internalModelRolloutId = idGenerator.generateInternalId()
    val externalModelRolloutId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("ModelRollouts") {
      set("ModelProviderId" to InternalId(modelLineData.getLong("ModelProviderId")))
      set("ModelSuiteId" to InternalId(modelLineData.getLong("ModelSuiteId")))
      set("ModelLineId" to InternalId(modelLineData.getLong("ModelLineId")))
      set("ModelRolloutId" to internalModelRolloutId)
      set("ExternalModelRolloutId" to externalModelRolloutId)
      set("RolloutPeriodStartTime" to modelRollout.rolloutPeriodStartTime.toGcloudTimestamp())
      set("RolloutPeriodEndTime" to modelRollout.rolloutPeriodEndTime.toGcloudTimestamp())
      if (latestModelRolloutData != null) {
        set("PreviousModelRolloutId" to InternalId(latestModelRolloutData.modelRolloutId))
      }
      set("ModelReleaseId" to modelReleaseResult.modelReleaseId)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
    }

    return modelRollout.copy {
      this.externalModelRolloutId = externalModelRolloutId.value
      if (latestModelRolloutData != null) {
        this.externalPreviousModelRolloutId = latestModelRolloutData.externalModelRolloutId
      }
    }
  }

  private suspend fun TransactionScope.readModelLineData(
    externalModelProviderId: ExternalId,
    externalModelSuiteId: ExternalId,
    externalModelLineId: ExternalId,
  ): Struct? {
    val sql =
      """
    SELECT
    ModelLines.ModelProviderId,
    ModelLines.ModelSuiteId,
    ModelLines.ModelLineId
    FROM ModelSuites JOIN ModelProviders USING(ModelProviderId)
    JOIN ModelLines
      ON(
        ModelSuites.ModelSuiteId = ModelLines.ModelSuiteId
        AND ModelSuites.ModelProviderId = ModelLines.ModelProviderId
      )
    WHERE ExternalModelProviderId = @externalModelProviderId
    AND ExternalModelSuiteId = @externalModelSuiteId
    AND ExternalModelLineId = @externalModelLineId
    """
        .trimIndent()

    val statement: Statement =
      statement(sql) {
        bind("externalModelProviderId" to externalModelProviderId.value)
        bind("externalModelSuiteId" to externalModelSuiteId.value)
        bind("externalModelLineId" to externalModelLineId.value)
      }

    return transactionContext.executeQuery(statement).singleOrNull()
  }

  // Reads the ModelRollout for a given ModelLine with the latest RolloutPeriodStartTime.
  private suspend fun TransactionScope.readLatestModelRolloutData(
    externalModelProviderId: ExternalId,
    externalModelSuiteId: ExternalId,
    externalModelLineId: ExternalId,
  ): LatestModelRolloutResult? {
    val sql =
      """
    SELECT
    ModelRollouts.ModelRolloutId,
    ModelRollouts.ExternalModelRolloutId,
    ModelRollouts.RolloutPeriodStartTime
    FROM ModelRollouts JOIN ModelLines
    USING (ModelProviderId, ModelSuiteId, ModelLineId)
    JOIN ModelSuites
    USING (ModelProviderId, ModelSuiteId)
    JOIN ModelProviders USING (ModelProviderId)
    WHERE ModelProviders.ExternalModelProviderId = @externalModelProviderId AND
    ModelSuites.ExternalModelSuiteId = @externalModelSuiteId AND
    ModelLines.ExternalModelLineId = @externalModelLineId
    ORDER BY ModelRollouts.RolloutPeriodStartTime DESC
    """
        .trimIndent()

    val statement: Statement =
      statement(sql) {
        bind("externalModelProviderId" to externalModelProviderId.value)
        bind("externalModelSuiteId" to externalModelSuiteId.value)
        bind("externalModelLineId" to externalModelLineId.value)
      }

    val result = transactionContext.executeQuery(statement).firstOrNull()

    return if (result == null) null
    else
      LatestModelRolloutResult(
        result.getLong("ModelRolloutId"),
        result.getLong("ExternalModelRolloutId"),
        result.getTimestamp("RolloutPeriodStartTime"),
      )
  }

  override fun ResultScope<ModelRollout>.buildResult(): ModelRollout {
    return checkNotNull(this.transactionResult).copy {
      createTime = commitTimestamp.toProto()
      updateTime = commitTimestamp.toProto()
    }
  }
}
