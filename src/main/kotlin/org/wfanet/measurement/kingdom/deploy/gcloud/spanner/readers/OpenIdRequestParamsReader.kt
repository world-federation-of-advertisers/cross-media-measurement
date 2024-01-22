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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause

class OpenIdRequestParamsReader : SpannerReader<OpenIdRequestParamsReader.Result>() {
  data class Result(val state: ExternalId, val nonce: ExternalId, val isExpired: Boolean)

  override val baseSql: String =
    """
    SELECT
      ExternalOpenIdRequestParamsId,
      Nonce,
      CURRENT_TIMESTAMP > TIMESTAMP_ADD(CreateTime, INTERVAL ValidSeconds SECOND) AS IsExpired,
    FROM OpenIdRequestParams
    """
      .trimIndent()

  override suspend fun translate(struct: Struct) =
    Result(
      state = ExternalId(struct.getLong("ExternalOpenIdRequestParamsId")),
      nonce = ExternalId(struct.getLong("Nonce")),
      isExpired = struct.getBoolean("IsExpired"),
    )

  suspend fun readByState(
    readContext: AsyncDatabaseClient.ReadContext,
    state: ExternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
          WHERE OpenIdRequestParams.ExternalOpenIdRequestParamsId = @state
          """
            .trimIndent()
        )
        bind("state").to(state.value)
      }
      .execute(readContext)
      .singleOrNull()
  }
}
