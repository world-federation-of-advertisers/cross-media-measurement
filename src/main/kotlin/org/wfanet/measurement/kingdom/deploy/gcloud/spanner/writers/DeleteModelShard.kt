// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.ModelShard
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelShardInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelShardNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelShardReader

/**
 * Deletes an [ModelShard] from the database.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [ModelShardNotFoundException] ModelShard not found
 * @throws [DataProviderNotFoundException] DataProvider not found
 */
class DeleteModelShard(
  private val externalDataProviderId: ExternalId,
  private val externalModelShardId: ExternalId,
  private val externalModelProviderId: ExternalId?,
) : SimpleSpannerWriter<ModelShard>() {

  override suspend fun TransactionScope.runTransaction(): ModelShard {
    val dataProviderId = readInternalDataProviderId()
    val modelShardResult = readModelShard()

    if (
      externalModelProviderId != null &&
        externalModelProviderId.value != modelShardResult.modelShard.externalModelProviderId
    ) {
      throw ModelShardInvalidArgsException(
        externalDataProviderId,
        externalModelShardId,
        externalModelProviderId,
      )
    }

    transactionContext.buffer(
      Mutation.delete(
        "ModelShards",
        KeySet.singleKey(Key.of(dataProviderId, modelShardResult.modelShardId.value)),
      )
    )

    return modelShardResult.modelShard
  }

  private suspend fun TransactionScope.readInternalDataProviderId(): Long =
    DataProviderReader()
      .readByExternalDataProviderId(transactionContext, externalDataProviderId)
      ?.dataProviderId ?: throw DataProviderNotFoundException(externalDataProviderId)

  private suspend fun TransactionScope.readModelShard(): ModelShardReader.Result =
    ModelShardReader()
      .readByExternalModelShardId(transactionContext, externalDataProviderId, externalModelShardId)
      ?: throw ModelShardNotFoundException(externalDataProviderId, externalModelShardId)
}
