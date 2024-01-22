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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.internal.kingdom.ModelShard
import org.wfanet.measurement.internal.kingdom.modelShard

class ModelShardReader : SpannerReader<ModelShardReader.Result>() {

  data class Result(
    val modelShard: ModelShard,
    val modelShardId: InternalId,
    val dataProviderId: InternalId,
  )

  override val baseSql: String =
    """
   SELECT
     ModelShards.DataProviderId,
     ModelShards.ModelShardId,
     ModelShards.ExternalModelShardId,
     ModelShards.ModelBlobPath,
     ModelShards.CreateTime,
     ModelReleases.ExternalModelReleaseId,
     DataProviders.ExternalDataProviderId,
     ModelSuites.ExternalModelSuiteId,
     ModelProviders.ExternalModelProviderId
     FROM ModelShards
     JOIN DataProviders USING (DataProviderId)
     JOIN ModelReleases USING (ModelProviderId, ModelSuiteId, ModelReleaseId)
     JOIN ModelSuites USING (ModelProviderId, ModelSuiteId)
     JOIN ModelProviders USING (ModelProviderId)
   """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(
      buildModelShard(struct),
      InternalId(struct.getLong("ModelShardId")),
      InternalId(struct.getLong("DataProviderId")),
    )

  private fun buildModelShard(struct: Struct): ModelShard = modelShard {
    externalDataProviderId = struct.getLong("ExternalDataProviderId")
    externalModelShardId = struct.getLong("ExternalModelShardId")
    externalModelProviderId = struct.getLong("ExternalModelProviderId")
    externalModelSuiteId = struct.getLong("ExternalModelSuiteId")
    externalModelReleaseId = struct.getLong("ExternalModelReleaseId")
    modelBlobPath = struct.getString("ModelBlobPath")
    createTime = struct.getTimestamp("CreateTime").toProto()
  }

  suspend fun readByExternalModelShardId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalDataProviderId: ExternalId,
    externalModelShardId: ExternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          "WHERE ExternalModelShardId = @externalModelShardId AND ExternalDataProviderId = @externalDataProviderId"
        )
        bind("externalModelShardId").to(externalModelShardId.value)
        bind("externalDataProviderId").to(externalDataProviderId.value)
      }
      .execute(readContext)
      .singleOrNull()
  }
}
