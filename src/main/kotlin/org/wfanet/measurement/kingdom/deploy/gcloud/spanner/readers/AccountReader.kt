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
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.AccountKt.openIdConnectIdentity
import org.wfanet.measurement.internal.kingdom.account

class AccountReader : SpannerReader<AccountReader.Result>() {
  data class Result(val account: Account, val accountId: InternalId)

  override val baseSql: String =
    """
    SELECT
      Accounts.AccountId,
      Accounts.ExternalAccountId,
      CreatorAccounts.ExternalAccountId as ExternalCreatorAccountId,
      Accounts.ActivationState,
      MeasurementConsumers.ExternalMeasurementConsumerId,
      Accounts.ActivationToken,
      OpenIdConnectIdentities.Issuer,
      OpenIdConnectIdentities.Subject,
      ARRAY(
        SELECT
          ExternalMeasurementConsumerId,
        FROM
          MeasurementConsumers
          JOIN MeasurementConsumerOwners USING (MeasurementConsumerId)
        WHERE
          Accounts.AccountId = MeasurementConsumerOwners.AccountId
      ) AS ExternalOwnedMeasurementConsumerIds,
    FROM Accounts
    LEFT JOIN Accounts as CreatorAccounts
      ON (Accounts.CreatorAccountId = CreatorAccounts.AccountId)
    LEFT JOIN OpenIdConnectIdentities
      ON (Accounts.AccountId = OpenIdConnectIdentities.AccountId)
    LEFT JOIN MeasurementConsumers
      ON (Accounts.OwnedMeasurementConsumerId = MeasurementConsumers.MeasurementConsumerId)
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(buildAccount(struct), InternalId(struct.getLong("AccountId")))

  private fun buildAccount(struct: Struct): Account = account {
    externalAccountId = struct.getLong("ExternalAccountId")
    if (!struct.isNull("ExternalCreatorAccountId")) {
      externalCreatorAccountId = struct.getLong("ExternalCreatorAccountId")
    }
    activationState = struct.getProtoEnum("ActivationState", Account.ActivationState::forNumber)

    if (!struct.isNull("ExternalMeasurementConsumerId")) {
      externalOwnedMeasurementConsumerId = struct.getLong("ExternalMeasurementConsumerId")
    }
    activationToken = struct.getLong("ActivationToken")

    if (!struct.isNull("Issuer")) {
      openIdIdentity = openIdConnectIdentity {
        this.issuer = struct.getString("Issuer")
        this.subject = struct.getString("Subject")
      }
    }

    externalOwnedMeasurementConsumerIds += struct.getLongList("ExternalOwnedMeasurementConsumerIds")
  }

  suspend fun readByExternalAccountId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalAccountId: ExternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause("WHERE Accounts.ExternalAccountId = @externalAccountId")
        bind("externalAccountId").to(externalAccountId.value)
      }
      .execute(readContext)
      .singleOrNull()
  }

  suspend fun readByInternalAccountId(
    readContext: AsyncDatabaseClient.ReadContext,
    internalAccountId: InternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause("WHERE Accounts.AccountId = @internalAccountId")
        bind("internalAccountId").to(internalAccountId.value)
      }
      .execute(readContext)
      .singleOrNull()
  }
}
