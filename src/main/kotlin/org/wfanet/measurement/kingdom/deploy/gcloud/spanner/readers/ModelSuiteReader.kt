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

import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.getInternalId
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.to
import org.wfanet.measurement.internal.kingdom.ModelSuite
import org.wfanet.measurement.internal.kingdom.modelSuite

class ModelSuiteReader : SpannerReader<ModelSuiteReader.Result>() {

  data class Result(
    val modelSuite: ModelSuite,
    val modelSuiteId: InternalId,
    val modelProviderId: InternalId,
  )

  override val baseSql: String =
    """
    SELECT
      ModelSuites.ModelSuiteId,
      ModelSuites.ModelProviderId,
      ModelSuites.ExternalModelSuiteId,
      ModelSuites.DisplayName,
      ModelSuites.Description,
      ModelSuites.CreateTime,
      ModelProviders.ExternalModelProviderId
      FROM ModelSuites
      JOIN ModelProviders USING (ModelProviderId)
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(
      buildModelSuite(struct),
      InternalId(struct.getLong("ModelSuiteId")),
      InternalId(struct.getLong("ModelProviderId")),
    )

  private fun buildModelSuite(struct: Struct): ModelSuite = modelSuite {
    externalModelProviderId = struct.getLong("ExternalModelProviderId")
    externalModelSuiteId = struct.getLong("ExternalModelSuiteId")
    displayName = struct.getString("DisplayName")
    description = struct.getString("Description")
    createTime = struct.getTimestamp("CreateTime").toProto()
  }

  suspend fun readByExternalModelSuiteId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalModelProviderId: ExternalId,
    externalModelSuiteId: ExternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          "WHERE ExternalModelSuiteId = @externalModelSuiteId AND ExternalModelProviderId = @externalModelProviderId"
        )
        bind("externalModelSuiteId").to(externalModelSuiteId.value)
        bind("externalModelProviderId").to(externalModelProviderId.value)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }

  companion object {
    suspend fun readModelSuiteKey(
      readContext: AsyncDatabaseClient.ReadContext,
      externalModelProviderId: ExternalId,
      externalModelSuiteId: ExternalId,
    ): ModelSuiteInternalKey? {
      val sql =
        """
        SELECT
          ModelProviderId,
          ModelSuiteId,
        FROM
          ModelProviders
          JOIN ModelSuites USING (ModelProviderId)
        WHERE
          ExternalModelProviderId = @externalModelProviderId
          AND ExternalModelSuiteId = @externalModelSuiteId
        """
          .trimIndent()
      val query =
        statement(sql) {
          bind("externalModelSuiteId").to(externalModelSuiteId)
          bind("externalModelProviderId").to(externalModelProviderId)
        }
      val row =
        readContext
          .executeQuery(query, Options.tag("reader=ModelSuiteReader,action=readModelSuiteKey"))
          .singleOrNullIfEmpty()

      return if (row == null) {
        null
      } else {
        ModelSuiteInternalKey(
          row.getInternalId("ModelProviderId"),
          row.getInternalId("ModelSuiteId"),
        )
      }
    }
  }
}

data class ModelSuiteInternalKey(val modelProviderId: InternalId, val modelSuiteId: InternalId)
