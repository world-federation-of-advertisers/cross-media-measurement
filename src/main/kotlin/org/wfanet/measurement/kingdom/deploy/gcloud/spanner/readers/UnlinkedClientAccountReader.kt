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
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.getInternalId
import org.wfanet.measurement.internal.kingdom.EventGroupKt
import org.wfanet.measurement.internal.kingdom.UnlinkedClientAccount
import org.wfanet.measurement.internal.kingdom.unlinkedClientAccount

class UnlinkedClientAccountReader : SpannerReader<UnlinkedClientAccountReader.Result>() {
  data class Result(
    val dataProviderId: InternalId,
    val unlinkedClientAccount: UnlinkedClientAccount,
  )

  override val baseSql: String =
    """
     SELECT
       UnlinkedClientAccounts.DataProviderId,
       UnlinkedClientAccounts.ClientAccountReferenceId,
       UnlinkedClientAccounts.Brands,
       UnlinkedClientAccounts.EventGroupReferenceId,
       UnlinkedClientAccounts.EventGroupEntityKeyType,
       UnlinkedClientAccounts.EventGroupEntityKeyId,
       UnlinkedClientAccounts.FirstObservedTime,
       DataProviders.ExternalDataProviderId
     FROM UnlinkedClientAccounts
     JOIN DataProviders USING (DataProviderId)
     """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(struct.getInternalId("DataProviderId"), buildUnlinkedClientAccount(struct))

  /**
   * Reads all [UnlinkedClientAccount] rows for [dataProviderId], ordered by reference ID.
   *
   * This is unpaginated and intended for the reconcile read-before-write.
   */
  suspend fun readByDataProviderId(
    readContext: AsyncDatabaseClient.ReadContext,
    dataProviderId: InternalId,
  ): List<Result> {
    return fillStatementBuilder {
        appendClause("WHERE UnlinkedClientAccounts.DataProviderId = @dataProviderId")
        bind("dataProviderId").to(dataProviderId.value)
        appendClause("ORDER BY UnlinkedClientAccounts.ClientAccountReferenceId ASC")
      }
      .execute(readContext)
      .toList()
  }

  private fun buildUnlinkedClientAccount(struct: Struct): UnlinkedClientAccount =
    unlinkedClientAccount {
      externalDataProviderId = struct.getLong("ExternalDataProviderId")
      clientAccountReferenceId = struct.getString("ClientAccountReferenceId")
      if (!struct.isNull("Brands")) {
        brands += struct.getStringList("Brands")
      }
      if (!struct.isNull("EventGroupEntityKeyId")) {
        entityKey =
          EventGroupKt.entityKey {
            entityType = struct.getString("EventGroupEntityKeyType")
            entityId = struct.getString("EventGroupEntityKeyId")
          }
      } else if (!struct.isNull("EventGroupReferenceId")) {
        eventGroupReferenceId = struct.getString("EventGroupReferenceId")
      }
      firstObservedTime = struct.getTimestamp("FirstObservedTime").toProto()
    }
}
