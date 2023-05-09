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
import org.wfanet.measurement.internal.kingdom.ModelRelease
import org.wfanet.measurement.internal.kingdom.modelRelease

class ModelReleaseReader : SpannerReader<ModelReleaseReader.Result>() {

  data class Result(val modelRelease: ModelRelease, val modelReleaseId: InternalId)

  override val baseSql: String =
    """
    SELECT
      ModelReleases.ModelReleaseId,
      ModelReleases.ExternalModelReleaseId,
      ModelReleases.CreateTime,
      ModelSuites.ExternalModelSuiteId,
      ModelSuites.ModelProviderId,
      ModelSuites.ModelSuiteId,
      ModelProviders.ModelProviderId,
      ModelProviders.ExternalModelProviderId,
      FROM ModelReleases
      JOIN (ModelSuites JOIN ModelProviders USING (ModelProviderId)) USING (ModelSuiteId)
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(buildModelRelease(struct), InternalId(struct.getLong("ModelReleaseId")))

  private fun buildModelRelease(struct: Struct): ModelRelease = modelRelease {
    externalModelReleaseId = struct.getLong("ExternalModelReleaseId")
    externalModelSuiteId = struct.getLong("ExternalModelSuiteId")
    externalModelProviderId = struct.getLong("ExternalModelProviderId")
    createTime = struct.getTimestamp("CreateTime").toProto()
  }

  suspend fun readByExternalModelReleaseId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalModelReleaseId: ExternalId,
    externalModelSuiteId: ExternalId,
    externalModelProviderId: ExternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          "WHERE ExternalModelReleaseId = @externalModelReleaseId AND ExternalModelSuiteId = @externalModelSuiteId AND ExternalModelProviderId = @externalModelProviderId"
        )
        bind("externalModelReleaseId").to(externalModelReleaseId.value)
        bind("externalModelSuiteId").to(externalModelSuiteId.value)
        bind("externalModelProviderId").to(externalModelProviderId.value)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }
}
