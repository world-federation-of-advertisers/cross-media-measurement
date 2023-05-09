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

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import com.google.protobuf.util.Timestamps
import java.time.Clock
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.kingdom.ModelRollout
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelRolloutInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamModelRollouts

class CreateModelRollout(private val modelRollout: ModelRollout, private val clock: Clock) :
  SpannerWriter<ModelRollout, ModelRollout>() {

  override suspend fun TransactionScope.runTransaction(): ModelRollout {
    val now = clock.instant().toProtoTime()
    if (Timestamps.compare(now, modelRollout.rolloutPeriodStartTime) >= 0) {
      throw ModelRolloutInvalidArgsException(
        ExternalId(modelRollout.externalModelProviderId),
        ExternalId(modelRollout.externalModelSuiteId),
        ExternalId(modelRollout.externalModelLineId)
      ) {
        "RolloutPeriodStartTime must be in the future."
      }
    }

    if (
      Timestamps.compare(modelRollout.rolloutPeriodStartTime, modelRollout.rolloutPeriodEndTime) >=
        0
    ) {
      throw ModelRolloutInvalidArgsException(
        ExternalId(modelRollout.externalModelProviderId),
        ExternalId(modelRollout.externalModelSuiteId),
        ExternalId(modelRollout.externalModelLineId)
      ) {
        "RolloutPeriodEndTime cannot precede RolloutPeriodStartTime."
      }
    }

    val existingModelRollouts =
      StreamModelRollouts(
          filter {
            externalModelProviderId = modelRollout.externalModelProviderId
            externalModelSuiteId = modelRollout.externalModelSuiteId
            externalModelLineId = modelRollout.externalModelLineId
          }
        )
        .execute(transactionContext)
        .toList()

    val previousModelRollout =
      if (existingModelRollouts.size > 0) {

        if (
          Timestamps.compare(
            modelRollout.rolloutPeriodStartTime,
            existingModelRollouts[existingModelRollouts.size - 1]
              .modelRollout
              .rolloutPeriodStartTime
          ) >= 0
        ) {
          throw ModelRolloutInvalidArgsException(
            ExternalId(modelRollout.externalModelProviderId),
            ExternalId(modelRollout.externalModelSuiteId),
            ExternalId(modelRollout.externalModelLineId)
          ) {
            "RolloutPeriodStartTime cannot be smaller than previous ModelRollout.RolloutPeriodStartTime(s)."
          }
        }
        if (modelRollout.rolloutPeriodStartTime == modelRollout.rolloutPeriodEndTime) {
          null
        } else {
          existingModelRollouts[existingModelRollouts.size - 1].modelRolloutId
        }
      } else {
        null
      }

    val modelLineData =
      readModelLineData(
        ExternalId(modelRollout.externalModelProviderId),
        ExternalId(modelRollout.externalModelSuiteId),
        ExternalId(modelRollout.externalModelLineId)
      )
        ?: throw ModelLineNotFoundException(
          ExternalId(modelRollout.externalModelProviderId),
          ExternalId(modelRollout.externalModelSuiteId),
          ExternalId(modelRollout.externalModelLineId)
        )

    val internalModelRolloutId = idGenerator.generateInternalId()
    val externalModelRolloutId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("ModelLines") {
      set("ModelProviderId" to InternalId(modelLineData.getLong("ModelProviderId")))
      set("ModelSuiteId" to InternalId(modelLineData.getLong("ModelSuiteId")))
      set("ModelLineId" to InternalId(modelLineData.getLong("ModelLineId")))
      set("ModelRolloutId" to internalModelRolloutId)
      set("ExternalModelRolloutId" to externalModelRolloutId)
      set("RolloutPeriodStartTime" to modelRollout.rolloutPeriodStartTime.toGcloudTimestamp())
      set("RolloutPeriodEndTime" to modelRollout.rolloutPeriodEndTime.toGcloudTimestamp())
      if (previousModelRollout != null) {
        set("PreviousModelRollout" to previousModelRollout)
      }
      set("CreateTime" to Value.COMMIT_TIMESTAMP)

      // TODO(@marcopremier) Retrieve ModelRelease using ModelReleaseReader and store its internal
      // ID here

    }

    return modelRollout.copy { this.externalModelRolloutId = externalModelRolloutId.value }
  }

  private suspend fun TransactionScope.readModelLineData(
    externalModelProviderId: ExternalId,
    externalModelSuiteId: ExternalId,
    externalModelLineId: ExternalId
  ): Struct? {
    val sql =
      """
    SELECT
    ModelSuites.ModelSuiteId,
    ModelSuites.ModelProviderId,
    ModelLines.ModelLineId,
    FROM ModelSuites JOIN ModelProviders USING(ModelProviderId)
    JOIN ModelLines USING (ModelSuiteId)
    WHERE ExternalModelProviderId = @externalModelProviderId AND
    ExternalModelSuiteId = @externalModelSuiteId AND
    ExternalModelLineId = @externalModelLineId
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

  override fun ResultScope<ModelRollout>.buildResult(): ModelRollout {
    return checkNotNull(this.transactionResult).copy { createTime = commitTimestamp.toProto() }
  }
}
