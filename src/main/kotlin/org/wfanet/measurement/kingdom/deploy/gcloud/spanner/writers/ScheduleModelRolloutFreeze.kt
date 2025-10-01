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

import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.ModelRollout
import org.wfanet.measurement.internal.kingdom.ScheduleModelRolloutFreezeRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelRolloutFreezeScheduledException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelRolloutFreezeTimeOutOfRangeException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelRolloutNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelRolloutReader

class ScheduleModelRolloutFreeze(private val request: ScheduleModelRolloutFreezeRequest) :
  SpannerWriter<ModelRollout, ModelRollout>() {

  override suspend fun TransactionScope.runTransaction(): ModelRollout {
    val modelRolloutResult =
      readModelRollout(
        ExternalId(request.externalModelProviderId),
        ExternalId(request.externalModelSuiteId),
        ExternalId(request.externalModelLineId),
        ExternalId(request.externalModelRolloutId),
      )

    if (modelRolloutResult.modelRollout.hasRolloutFreezeTime()) {
      throw ModelRolloutFreezeScheduledException(
        modelRolloutResult.modelRollout.rolloutFreezeTime.toInstant()
      )
    }

    val requestRolloutFreezeTime = request.rolloutFreezeTime.toInstant()
    val rolloutPeriod =
      OpenEndTimeRange(
        modelRolloutResult.modelRollout.rolloutPeriodStartTime.toInstant(),
        modelRolloutResult.modelRollout.rolloutPeriodEndTime.toInstant(),
      )
    if (requestRolloutFreezeTime !in rolloutPeriod) {
      throw ModelRolloutFreezeTimeOutOfRangeException(
        rolloutPeriod.start,
        rolloutPeriod.endExclusive,
      )
    }

    transactionContext.bufferUpdateMutation("ModelRollouts") {
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
        externalModelRolloutId,
      )
      ?: throw ModelRolloutNotFoundException(
        externalModelProviderId,
        externalModelSuiteId,
        externalModelLineId,
        externalModelRolloutId,
      )

  override fun ResultScope<ModelRollout>.buildResult(): ModelRollout {
    return checkNotNull(this.transactionResult).copy { updateTime = commitTimestamp.toProto() }
  }
}
