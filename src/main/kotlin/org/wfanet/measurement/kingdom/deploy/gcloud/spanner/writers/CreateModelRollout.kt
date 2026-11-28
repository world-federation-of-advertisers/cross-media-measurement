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
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.common.toInstant
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.to
import org.wfanet.measurement.internal.kingdom.ModelRollout
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.modelLineKey
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelReleaseNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelRolloutOlderThanPreviousException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelLineInternalKey
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelLineReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelReleaseReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelRolloutReader

class CreateModelRollout(private val modelRollout: ModelRollout) :
  SpannerWriter<ModelRollout, ModelRollout>() {

  override suspend fun TransactionScope.runTransaction(): ModelRollout {
    val externalModelProviderId = ExternalId(modelRollout.externalModelProviderId)
    val externalModelSuiteId = ExternalId(modelRollout.externalModelSuiteId)
    val externalModelLineId = ExternalId(modelRollout.externalModelLineId)

    val latestModelRolloutResult: ModelRolloutReader.Result? =
      ModelRolloutReader()
        .readLatest(
          transactionContext,
          externalModelProviderId,
          externalModelSuiteId,
          externalModelLineId,
        )

    // New ModelRollout cannot have a RolloutStartTime that precedes the RolloutStartTime of the
    // current PreviousModelRollout
    if (
      latestModelRolloutResult != null &&
        modelRollout.rolloutPeriodStartTime.toInstant() <
          latestModelRolloutResult.modelRollout.rolloutPeriodStartTime.toInstant()
    ) {
      throw ModelRolloutOlderThanPreviousException(
        externalModelProviderId,
        externalModelSuiteId,
        externalModelLineId,
        ExternalId(latestModelRolloutResult.modelRollout.externalModelRolloutId),
      )
    }
    val internalKey: ModelLineInternalKey =
      ModelLineReader.readInternalKey(
        transactionContext,
        modelLineKey {
          this.externalModelProviderId = externalModelProviderId.value
          this.externalModelSuiteId = externalModelSuiteId.value
          this.externalModelLineId = externalModelLineId.value
        },
      )
        ?: throw ModelLineNotFoundException(
          externalModelProviderId,
          externalModelSuiteId,
          externalModelLineId,
        )

    val modelReleaseResult =
      ModelReleaseReader()
        .readByExternalIds(
          transactionContext,
          ExternalId(modelRollout.externalModelReleaseId),
          externalModelSuiteId,
          externalModelProviderId,
        )
        ?: throw ModelReleaseNotFoundException(
          externalModelProviderId,
          externalModelSuiteId,
          ExternalId(modelRollout.externalModelReleaseId),
        )

    val internalModelRolloutId = idGenerator.generateInternalId()
    val externalModelRolloutId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("ModelRollouts") {
      set("ModelProviderId").to(internalKey.modelProviderId)
      set("ModelSuiteId").to(internalKey.modelSuiteId)
      set("ModelLineId").to(internalKey.modelLineId)
      set("ModelRolloutId" to internalModelRolloutId)
      set("ExternalModelRolloutId" to externalModelRolloutId)
      set("RolloutPeriodStartTime" to modelRollout.rolloutPeriodStartTime.toGcloudTimestamp())
      set("RolloutPeriodEndTime" to modelRollout.rolloutPeriodEndTime.toGcloudTimestamp())
      if (latestModelRolloutResult != null) {
        set("PreviousModelRolloutId").to(latestModelRolloutResult.modelRolloutId)
      }
      set("ModelReleaseId" to modelReleaseResult.modelReleaseId)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
      set("UpdateTime" to Value.COMMIT_TIMESTAMP)
    }

    return modelRollout.copy {
      this.externalModelRolloutId = externalModelRolloutId.value
      if (latestModelRolloutResult != null) {
        this.externalPreviousModelRolloutId =
          latestModelRolloutResult.modelRollout.externalModelRolloutId
      }
    }
  }

  override fun ResultScope<ModelRollout>.buildResult(): ModelRollout {
    return checkNotNull(this.transactionResult).copy {
      createTime = commitTimestamp.toProto()
      updateTime = commitTimestamp.toProto()
    }
  }
}
