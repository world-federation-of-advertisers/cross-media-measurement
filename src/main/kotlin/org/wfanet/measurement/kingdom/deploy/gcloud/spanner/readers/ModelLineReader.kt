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
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.modelLine

class ModelLineReader : SpannerReader<ModelLineReader.Result>() {

  data class Result(
    val modelLine: ModelLine,
    val modelLineId: InternalId,
    val modelSuiteId: InternalId,
    val modelProviderId: InternalId
  )

  override val baseSql: String =
    """
    SELECT
      ModelLines.ModelProviderId,
      ModelLines.ModelSuiteId,
      ModelLines.ModelLineId,
      ModelLines.ExternalModelLineId,
      ModelLines.DisplayName,
      ModelLines.Description,
      ModelLines.ActiveStartTime,
      ModelLines.ActiveEndTime,
      ModelLines.Type,
      ModelLines.HoldbackModelLine,
      ModelLines.CreateTime,
      ModelLines.UpdateTime,
      ModelSuites.ExternalModelSuiteId,
      ModelProviders.ExternalModelProviderId,
      HoldbackModelLine.ExternalModelLineId as ExternalHoldbackModelLineId,
      FROM ModelLines
      JOIN ModelSuites USING (ModelSuiteId)
      JOIN ModelProviders ON (ModelSuites.ModelProviderId = ModelProviders.ModelProviderId)
      LEFT JOIN ModelLines as HoldbackModelLine
      ON (ModelLines.HoldbackModelLine = HoldbackModelLine.ModelLineId)
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(
      buildModelLine(struct),
      InternalId(struct.getLong("ModelLineId")),
      InternalId(struct.getLong("ModelSuiteId")),
      InternalId(struct.getLong("ModelProviderId"))
    )

  private fun buildModelLine(struct: Struct): ModelLine = modelLine {
    externalModelProviderId = struct.getLong("ExternalModelProviderId")
    externalModelSuiteId = struct.getLong("ExternalModelSuiteId")
    externalModelLineId = struct.getLong("ExternalModelLineId")
    if (!struct.isNull("DisplayName")) {
      displayName = struct.getString("DisplayName")
    }
    if (!struct.isNull("Description")) {
      description = struct.getString("Description")
    }
    activeStartTime = struct.getTimestamp("ActiveStartTime").toProto()
    if (!struct.isNull("ActiveEndTime")) {
      activeEndTime = struct.getTimestamp("ActiveEndTime").toProto()
    }
    type = struct.getProtoEnum("Type", ModelLine.Type::forNumber)
    if (!struct.isNull("ExternalHoldbackModelLineId")) {
      externalHoldbackModelLineId = struct.getLong("ExternalHoldbackModelLineId")
    }
    createTime = struct.getTimestamp("CreateTime").toProto()
    if (!struct.isNull("UpdateTime")) {
      updateTime = struct.getTimestamp("UpdateTime").toProto()
    }
  }

  suspend fun readByExternalModelLineId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalModelProviderId: ExternalId,
    externalModelSuiteId: ExternalId,
    externalModelLineId: ExternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
            WHERE ExternalModelSuiteId = @externalModelSuiteId
            AND ExternalModelProviderId = @externalModelProviderId
            AND ModelLines.ExternalModelLineId = @externalModelLineId
          """
            .trimIndent()
        )
        bind("externalModelSuiteId").to(externalModelSuiteId.value)
        bind("externalModelProviderId").to(externalModelProviderId.value)
        bind("externalModelLineId").to(externalModelLineId.value)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }
}
