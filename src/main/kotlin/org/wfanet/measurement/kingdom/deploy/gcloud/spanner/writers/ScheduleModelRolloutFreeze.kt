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
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.kingdom.ModelRollout
import org.wfanet.measurement.internal.kingdom.ScheduleModelRolloutFreezeRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelRolloutInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelRolloutNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelRolloutReader

class ScheduleModelRolloutFreeze(
  private val request: ScheduleModelRolloutFreezeRequest,
  private val clock: Clock
) : SpannerWriter<ModelRollout, ModelRollout>() {

  override suspend fun TransactionScope.runTransaction(): ModelRollout {

    val modelRolloutResult =
      readModelRollout(
        ExternalId(request.externalModelProviderId),
        ExternalId(request.externalModelSuiteId),
        ExternalId(request.externalModelLineId),
        ExternalId(request.externalModelRolloutId)
      )

    val now = clock.instant().toProtoTime()
    if (Timestamps.compare(now, request.rolloutFreezeTime) >= 0) {
      throw ModelRolloutInvalidArgsException(
        ExternalId(request.externalModelProviderId),
        ExternalId(request.externalModelSuiteId),
        ExternalId(request.externalModelLineId)
      ) {
        "RolloutFreezeTime must be in the future."
      }
    }

    if (
      Timestamps.compare(
        modelRolloutResult.modelRollout.rolloutPeriodStartTime,
        request.rolloutFreezeTime
      ) > 0
    ) {
      throw ModelRolloutInvalidArgsException(
        ExternalId(request.externalModelProviderId),
        ExternalId(request.externalModelSuiteId),
        ExternalId(request.externalModelLineId)
      ) {
        "RolloutFreezeTime cannot precede RolloutPeriodStartTime."
      }
    }

    if (
      Timestamps.compare(
        request.rolloutFreezeTime,
        modelRolloutResult.modelRollout.rolloutPeriodEndTime
      ) >= 0
    ) {
      throw ModelRolloutInvalidArgsException(
        ExternalId(request.externalModelProviderId),
        ExternalId(request.externalModelSuiteId),
        ExternalId(request.externalModelLineId)
      ) {
        "RolloutFreezeTime cannot be greater than RolloutPeriodEndTime."
      }
    }

    transactionContext.bufferUpdateMutation("ModelLines") {
      set("ModelRolloutId" to modelRolloutResult.modelRolloutId)
      set("ModelLineId" to modelRolloutResult.modelLineId)
      set("ModelSuiteId" to modelRolloutResult.modelSuiteId)
      set("ModelProviderId" to modelRolloutResult.modelProviderId)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
      set("RolloutFreezeTime" to request.rolloutFreezeTime.toGcloudTimestamp())
    }

    return modelRolloutResult.modelRollout.copy { rolloutFreezeTime = request.rolloutFreezeTime }
  }

  private suspend fun TransactionScope.readModelRollout(
    externalModelProviderId: ExternalId,
    externalModelSuiteId: ExternalId,
    externalModelLineId: ExternalId,
    externalModelRolloutId: ExternalId,
  ): ModelRolloutReader.Result =
    ModelRolloutReader()
      .readByExternalModelRolloutId(
        transactionContext,
        externalModelProviderId,
        externalModelSuiteId,
        externalModelLineId,
        externalModelRolloutId
      )
      ?: throw ModelRolloutNotFoundException(
        externalModelProviderId,
        externalModelSuiteId,
        externalModelLineId,
        externalModelRolloutId
      )

  override fun ResultScope<ModelRollout>.buildResult(): ModelRollout {
    return checkNotNull(this.transactionResult).copy { updateTime = commitTimestamp.toProto() }
  }
}
