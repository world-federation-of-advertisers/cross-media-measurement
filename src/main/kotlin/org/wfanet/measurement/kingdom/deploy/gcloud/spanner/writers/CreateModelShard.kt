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
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.ModelShard
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader

class CreateModelShard(private val modelShard: ModelShard) :
  SpannerWriter<ModelShard, ModelShard>() {

  override suspend fun TransactionScope.runTransaction(): ModelShard {
    val externalDataProviderId = ExternalId(modelShard.externalDataProviderId)
    val dataProviderResult =
      DataProviderReader().readByExternalDataProviderId(transactionContext, externalDataProviderId)
        ?: throw DataProviderNotFoundException(externalDataProviderId)

    val dataProviderId = dataProviderResult.dataProviderId

    val internalModelShardId = idGenerator.generateInternalId()
    val externalModelShardId = idGenerator.generateExternalId()

    // TODO(@jojijac0b): Add logic to get internal model release.

    transactionContext.bufferInsertMutation("ModelShards") {
      set("DataProviderId" to dataProviderId)
      set("ModelShardId" to internalModelShardId)
      set("ExternalModelShardId" to externalModelShardId)
      set("ModelRelease" to modelShard.externalModelReleaseId)
      set("ModelBlobPath" to modelShard.modelBlobPath)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
    }

    return modelShard.copy { this.externalModelShardId = externalModelShardId.value }
  }

  override fun ResultScope<ModelShard>.buildResult(): ModelShard {
    return checkNotNull(this.transactionResult).copy { createTime = commitTimestamp.toProto() }
  }
}
