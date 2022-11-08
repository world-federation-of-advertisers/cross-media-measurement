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

class OpenIdConnectIdentityReader : SpannerReader<OpenIdConnectIdentityReader.Result>() {
  data class Result(
    val issuer: String,
    val subject: String,
    val accountId: InternalId,
    val openIdConnectIdentityId: InternalId
  )

  override val baseSql: String =
    """
    SELECT
      OpenIdConnectIdentities.OpenIdConnectIdentityId,
      OpenIdConnectIdentities.Issuer,
      OpenIdConnectIdentities.Subject,
      OpenIdConnectIdentities.AccountId,
    FROM OpenIdConnectIdentities
    """
      .trimIndent()

  override suspend fun translate(struct: Struct) =
    Result(
      issuer = struct.getString("Issuer"),
      subject = struct.getString("Subject"),
      accountId = InternalId(struct.getLong("AccountId")),
      openIdConnectIdentityId = InternalId(struct.getLong("OpenIdConnectIdentityId"))
    )

  suspend fun readByAccountId(
    readContext: AsyncDatabaseClient.ReadContext,
    accountId: InternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause("WHERE OpenIdConnectIdentities.AccountId = @accountId")
        bind("accountId").to(accountId.value)
      }
      .execute(readContext)
      .singleOrNull()
  }

  suspend fun readByIssuerAndSubject(
    readContext: AsyncDatabaseClient.ReadContext,
    issuer: String,
    subject: String
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
          WHERE OpenIdConnectIdentities.Issuer = @issuer AND OpenIdConnectIdentities.Subject = @subject
          """
            .trimIndent()
        )
        bind("issuer").to(issuer)
        bind("subject").to(subject)
      }
      .execute(readContext)
      .singleOrNull()
  }
}
