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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.DeleteModelShardRequest
import org.wfanet.measurement.internal.kingdom.ModelShard
import org.wfanet.measurement.internal.kingdom.ModelShardsGrpcKt.ModelShardsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamModelShardsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DataProviderNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelReleaseNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelShardNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelSuiteNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamModelShards
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateModelShard
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.DeleteModelShard

class SpannerModelShardsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : ModelShardsCoroutineImplBase() {

  override suspend fun createModelShard(request: ModelShard): ModelShard {
    grpcRequire(request.externalDataProviderId != 0L) {
      "DataProviderId field of ModelShard is missing."
    }
    try {
      return CreateModelShard(request).execute(client, idGenerator)
    } catch (e: DataProviderNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "DataProvider not found." }
    } catch (e: ModelSuiteNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "ModelSuite not found." }
    } catch (e: ModelReleaseNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "ModelRelease not found." }
    }
  }

  override suspend fun deleteModelShard(request: DeleteModelShardRequest): ModelShard {
    grpcRequire(request.externalDataProviderId != 0L) { "ExternalDataProviderId unspecified" }
    grpcRequire(request.externalModelShardId != 0L) { "ExternalModelShardId unspecified" }
    try {
      return DeleteModelShard(
          ExternalId(request.externalDataProviderId),
          ExternalId(request.externalModelShardId)
        )
        .execute(client, idGenerator)
    } catch (e: DataProviderNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "DataProvider not found." }
    } catch (e: ModelShardNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "ModelShard not found." }
    } catch (e: KingdomInternalException) {
      e.throwStatusRuntimeException(Status.INTERNAL) { "Unexpected internal error." }
    }
  }

  override fun streamModelShards(request: StreamModelShardsRequest): Flow<ModelShard> {
    grpcRequire(request.limit >= 0) { "Limit cannot be less than 0" }
    if (
      request.filter.hasAfter() &&
        (!request.filter.after.hasCreateTime() ||
          request.filter.after.externalDataProviderId == 0L ||
          request.filter.after.externalModelShardId == 0L)
    ) {
      failGrpc(
        Status.INVALID_ARGUMENT,
      ) {
        "Missing After filter fields"
      }
    }
    return StreamModelShards(request.filter, request.limit).execute(client.singleUse()).map {
      it.modelShard
    }
  }
}
