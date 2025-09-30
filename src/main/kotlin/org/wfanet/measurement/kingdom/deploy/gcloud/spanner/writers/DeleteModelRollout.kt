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

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import java.time.Clock
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.kingdom.DeleteModelRolloutRequest
import org.wfanet.measurement.internal.kingdom.ModelRollout
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelRolloutAlreadyStartedException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelRolloutNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelRolloutReader

class DeleteModelRollout(private val request: DeleteModelRolloutRequest, private val clock: Clock) :
  SimpleSpannerWriter<ModelRollout>() {

  override suspend fun TransactionScope.runTransaction(): ModelRollout {
    val modelRolloutResult =
      readModelRollout(
        ExternalId(request.externalModelProviderId),
        ExternalId(request.externalModelSuiteId),
        ExternalId(request.externalModelLineId),
        ExternalId(request.externalModelRolloutId),
      )

    val now = clock.instant()
    val rolloutPeriodStartTime = modelRolloutResult.modelRollout.rolloutPeriodStartTime.toInstant()
    if (rolloutPeriodStartTime <= now) {
      throw ModelRolloutAlreadyStartedException(rolloutPeriodStartTime)
    }

    transactionContext.buffer(
      Mutation.delete(
        "ModelRollouts",
        KeySet.singleKey(
          Key.of(
            modelRolloutResult.modelProviderId.value,
            modelRolloutResult.modelSuiteId.value,
            modelRolloutResult.modelLineId.value,
            modelRolloutResult.modelRolloutId.value,
          )
        ),
      )
    )

    return modelRolloutResult.modelRollout
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
}
