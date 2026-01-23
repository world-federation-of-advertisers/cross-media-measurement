/*
 * Copyright 2026 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.gcloud.spanner.getInternalId
import org.wfanet.measurement.internal.kingdom.ClientAccount
import org.wfanet.measurement.internal.kingdom.clientAccount

class ClientAccountReader : SpannerReader<ClientAccountReader.Result>() {

  data class Result(
    val measurementConsumerId: InternalId,
    val clientAccountId: InternalId,
    val clientAccount: ClientAccount,
  )

  override val baseSql: String =
    """
    SELECT
      ClientAccounts.MeasurementConsumerId,
      ClientAccounts.ClientAccountId,
      ClientAccounts.ExternalClientAccountId,
      ClientAccounts.DataProviderId,
      ClientAccounts.ClientAccountReferenceId,
      ClientAccounts.CreateTime,
      MeasurementConsumers.ExternalMeasurementConsumerId,
      DataProviders.ExternalDataProviderId
    FROM ClientAccounts
    JOIN MeasurementConsumers USING (MeasurementConsumerId)
    JOIN DataProviders USING (DataProviderId)
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(
      struct.getInternalId("MeasurementConsumerId"),
      struct.getInternalId("ClientAccountId"),
      buildClientAccount(struct),
    )

  suspend fun readByExternalId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalMeasurementConsumerId: ExternalId,
    externalClientAccountId: ExternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
          WHERE ExternalMeasurementConsumerId = @externalMeasurementConsumerId
            AND ExternalClientAccountId = @externalClientAccountId
          """
            .trimIndent()
        )
        bind("externalMeasurementConsumerId").to(externalMeasurementConsumerId.value)
        bind("externalClientAccountId").to(externalClientAccountId.value)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }

  suspend fun readByDataProviderAndReferenceId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalDataProviderId: ExternalId,
    clientAccountReferenceId: String,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
          WHERE ExternalDataProviderId = @externalDataProviderId
            AND ClientAccountReferenceId = @clientAccountReferenceId
          """
            .trimIndent()
        )
        bind("externalDataProviderId").to(externalDataProviderId.value)
        bind("clientAccountReferenceId").to(clientAccountReferenceId)
        appendClause("LIMIT 1")
      }
      .execute(readContext)
      .singleOrNull()
  }

  private fun buildClientAccount(struct: Struct): ClientAccount = clientAccount {
    externalClientAccountId = struct.getLong("ExternalClientAccountId")
    externalMeasurementConsumerId = struct.getLong("ExternalMeasurementConsumerId")
    externalDataProviderId = struct.getLong("ExternalDataProviderId")
    clientAccountReferenceId = struct.getString("ClientAccountReferenceId")
  }
}
