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
import org.wfanet.measurement.internal.kingdom.ModelOutage
import org.wfanet.measurement.internal.kingdom.modelOutage

class ModelOutageReader : SpannerReader<ModelOutageReader.Result>() {

  data class Result(
    val modelOutage: ModelOutage,
    val modelOutageId: InternalId,
    val modelLineId: InternalId,
    val modelSuiteId: InternalId,
    val modelProviderId: InternalId,
  )

  override val baseSql: String =
    """
    SELECT
      ModelOutages.ModelProviderId,
      ModelOutages.ModelSuiteId,
      ModelOutages.ModelLineId,
      ModelOutages.ModelOutageId,
      ModelOutages.ExternalModelOutageId,
      ModelOutages.OutageStartTime,
      ModelOutages.OutageEndTime,
      ModelOutages.State,
      ModelOutages.CreateTime,
      ModelOutages.DeleteTime,
      ModelLines.ExternalModelLineId,
      ModelSuites.ExternalModelSuiteId,
      ModelProviders.ExternalModelProviderId,
      FROM ModelOutages
      JOIN ModelLines USING (ModelLineId, ModelSuiteId, ModelProviderId)
      JOIN ModelSuites USING (ModelSuiteId, ModelProviderId)
      JOIN ModelProviders USING (ModelProviderId)
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): ModelOutageReader.Result =
    ModelOutageReader.Result(
      buildModelOutage(struct),
      InternalId(struct.getLong("ModelOutageId")),
      InternalId(struct.getLong("ModelLineId")),
      InternalId(struct.getLong("ModelSuiteId")),
      InternalId(struct.getLong("ModelProviderId")),
    )

  private fun buildModelOutage(struct: Struct): ModelOutage = modelOutage {
    externalModelProviderId = struct.getLong("ExternalModelProviderId")
    externalModelSuiteId = struct.getLong("ExternalModelSuiteId")
    externalModelLineId = struct.getLong("ExternalModelLineId")
    externalModelOutageId = struct.getLong("ExternalModelOutageId")
    modelOutageStartTime = struct.getTimestamp("OutageStartTime").toProto()
    modelOutageEndTime = struct.getTimestamp("OutageEndTime").toProto()
    state = struct.getProtoEnum("State", ModelOutage.State::forNumber)
    createTime = struct.getTimestamp("CreateTime").toProto()
    if (!struct.isNull("DeleteTime")) {
      deleteTime = struct.getTimestamp("DeleteTime").toProto()
    }
  }

  suspend fun readByExternalIds(
    readContext: AsyncDatabaseClient.ReadContext,
    externalModelProviderId: ExternalId,
    externalModelSuiteId: ExternalId,
    externalModelLineId: ExternalId,
    externalModelOutageId: ExternalId,
  ): ModelOutageReader.Result? {
    return fillStatementBuilder {
        appendClause(
          """
          WHERE ExternalModelProviderId = @externalModelProviderId AND
          ExternalModelSuiteId = @externalModelSuiteId AND
          ExternalModelLineId = @externalModelLineId AND
          ExternalModelOutageId = @externalModelOutageId
          """
            .trimIndent()
        )
        bind("externalModelProviderId").to(externalModelProviderId.value)
        bind("externalModelSuiteId").to(externalModelSuiteId.value)
        bind("externalModelLineId").to(externalModelLineId.value)
        bind("externalModelOutageId").to(externalModelOutageId.value)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }
}
