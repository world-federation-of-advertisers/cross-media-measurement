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
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.gcloud.spanner.to
import org.wfanet.measurement.internal.kingdom.ModelShard
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelReleaseNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelSuiteNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelSuiteReader

class CreateModelShard(private val modelShard: ModelShard) :
  SpannerWriter<ModelShard, ModelShard>() {

  override suspend fun TransactionScope.runTransaction(): ModelShard {
    val externalDataProviderId = ExternalId(modelShard.externalDataProviderId)
    val dataProviderId =
      DataProviderReader.readDataProviderId(transactionContext, externalDataProviderId)
        ?: throw DataProviderNotFoundException(externalDataProviderId)

    val modelSuiteResult =
      ModelSuiteReader()
        .readByExternalModelSuiteId(
          transactionContext,
          ExternalId(modelShard.externalModelProviderId),
          ExternalId(modelShard.externalModelSuiteId),
        )
        ?: throw ModelSuiteNotFoundException(
          externalDataProviderId,
          ExternalId(modelShard.externalModelSuiteId),
        )

    val modelReleaseIds: Struct =
      readModelReleaseIds(
        modelSuiteResult.modelProviderId,
        modelSuiteResult.modelSuiteId,
        ExternalId(modelShard.externalModelReleaseId),
      )
        ?: throw ModelReleaseNotFoundException(
          externalDataProviderId,
          ExternalId(modelShard.externalModelSuiteId),
          ExternalId(modelShard.externalModelReleaseId),
        ) {
          "ModelRelease with external ID $modelShard.externalModelReleaseId not found"
        }

    val internalModelShardId = idGenerator.generateInternalId()
    val externalModelShardId = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("ModelShards") {
      set("DataProviderId").to(dataProviderId)
      set("ModelShardId" to internalModelShardId)
      set("ExternalModelShardId" to externalModelShardId)
      set("ModelProviderId" to modelReleaseIds.getLong("ModelProviderId"))
      set("ModelSuiteId" to modelReleaseIds.getLong("ModelSuiteId"))
      set("ModelReleaseId" to modelReleaseIds.getLong("ModelReleaseId"))
      set("ModelBlobPath" to modelShard.modelBlobPath)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
    }

    return modelShard.copy { this.externalModelShardId = externalModelShardId.value }
  }

  private suspend fun TransactionScope.readModelReleaseIds(
    modelProviderId: InternalId,
    modelSuiteId: InternalId,
    externalModelReleaseId: ExternalId,
  ): Struct? {
    return transactionContext.readRowUsingIndex(
      "ModelReleases",
      "ModelReleasesByExternalId",
      Key.of(modelProviderId.value, modelSuiteId.value, externalModelReleaseId.value),
      "ModelProviderId",
      "ModelSuiteId",
      "ModelReleaseId",
    )
  }

  override fun ResultScope<ModelShard>.buildResult(): ModelShard {
    return checkNotNull(this.transactionResult).copy { createTime = commitTimestamp.toProto() }
  }
}
