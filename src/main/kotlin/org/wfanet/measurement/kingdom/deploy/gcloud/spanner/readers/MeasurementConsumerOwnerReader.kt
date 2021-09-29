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
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause

class MeasurementConsumerOwnerReader() : SpannerReader<MeasurementConsumerOwnerReader.Result>() {
  data class Result(val exist: Boolean)

  override val baseSql: String =
    """
    SELECT
      MeasurementConsumerOwners.AccountId,
      MeasurementConsumerOwners.MeasurementConsumerId,
    FROM MeasurementConsumerOwners
    """.trimIndent()

  override suspend fun translate(struct: Struct): Result = Result(true)

  suspend fun checkOwnershipExist(
    readContext: AsyncDatabaseClient.ReadContext,
    internalAccountId: InternalId,
    internalMeasurementConsumerId: InternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
            WHERE MeasurementConsumerId = @internalMeasurementConsumerId
              AND AccountId = @internalAccountId
            """
        )
        bind("internalMeasurementConsumerId").to(internalMeasurementConsumerId.value)
        bind("internalAccountId").to(internalAccountId.value)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }
}
