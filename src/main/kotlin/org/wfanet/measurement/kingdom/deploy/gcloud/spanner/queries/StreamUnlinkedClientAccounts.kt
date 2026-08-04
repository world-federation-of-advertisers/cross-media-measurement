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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import com.google.cloud.spanner.Statement
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.ListUnlinkedClientAccountsPageToken
import org.wfanet.measurement.internal.kingdom.ListUnlinkedClientAccountsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.UnlinkedClientAccountReader

class StreamUnlinkedClientAccounts(
  private val requestFilter: ListUnlinkedClientAccountsRequest.Filter,
  limit: Int = 0,
  private val after: ListUnlinkedClientAccountsPageToken.After? = null,
) : SimpleSpannerQuery<UnlinkedClientAccountReader.Result>() {
  override val reader =
    UnlinkedClientAccountReader().fillStatementBuilder {
      appendWhereClause()
      appendClause(
        "ORDER BY DataProviders.ExternalDataProviderId ASC, " +
          "UnlinkedClientAccounts.ClientAccountReferenceId ASC"
      )
      if (limit > 0) {
        appendClause("LIMIT @${LIMIT_PARAM}")
        bind(LIMIT_PARAM to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause() {
    val conjuncts = mutableListOf<String>()

    if (requestFilter.externalDataProviderId != 0L) {
      conjuncts.add("ExternalDataProviderId = @${EXTERNAL_DATA_PROVIDER_ID}")
      bind(EXTERNAL_DATA_PROVIDER_ID to requestFilter.externalDataProviderId)
    }

    if (after != null) {
      // Compound keyset cursor matching the (ExternalDataProviderId,
      // ClientAccountReferenceId) sort order. ClientAccountReferenceId is unique only
      // within a DataProvider, so the cursor must also key on ExternalDataProviderId to
      // page correctly when listing across DataProviders.
      conjuncts.add(
        "(DataProviders.ExternalDataProviderId > @${AFTER_EXTERNAL_DATA_PROVIDER_ID} OR " +
          "(DataProviders.ExternalDataProviderId = @${AFTER_EXTERNAL_DATA_PROVIDER_ID} AND " +
          "UnlinkedClientAccounts.ClientAccountReferenceId > @${CLIENT_ACCOUNT_REFERENCE_ID}))"
      )
      bind(AFTER_EXTERNAL_DATA_PROVIDER_ID to after.externalDataProviderId)
      bind(CLIENT_ACCOUNT_REFERENCE_ID to after.clientAccountReferenceId)
    }

    if (conjuncts.isEmpty()) {
      return
    }

    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  companion object {
    private const val LIMIT_PARAM = "limit"
    private const val EXTERNAL_DATA_PROVIDER_ID = "externalDataProviderId"
    private const val AFTER_EXTERNAL_DATA_PROVIDER_ID = "afterExternalDataProviderId"
    private const val CLIENT_ACCOUNT_REFERENCE_ID = "clientAccountReferenceId"
  }
}
