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
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.ListClientAccountsPageToken
import org.wfanet.measurement.internal.kingdom.ListClientAccountsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ClientAccountReader

class StreamClientAccounts(
  private val requestFilter: ListClientAccountsRequest.Filter,
  limit: Int = 0,
  private val after: ListClientAccountsPageToken.After? = null,
) : SimpleSpannerQuery<ClientAccountReader.Result>() {
  override val reader =
    ClientAccountReader().fillStatementBuilder {
      appendWhereClause()
      appendClause(
        """
          ORDER BY ClientAccounts.CreateTime DESC,
          MeasurementConsumers.ExternalMeasurementConsumerId ASC,
          ClientAccounts.ExternalClientAccountId ASC
        """
          .trimIndent()
      )
      if (limit > 0) {
        appendClause("LIMIT @${LIMIT_PARAM}")
        bind(LIMIT_PARAM to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause() {
    val conjuncts = mutableListOf<String>()

    if (requestFilter.externalMeasurementConsumerId != 0L) {
      conjuncts.add("ExternalMeasurementConsumerId = @${EXTERNAL_MEASUREMENT_CONSUMER_ID}")
      bind(EXTERNAL_MEASUREMENT_CONSUMER_ID to requestFilter.externalMeasurementConsumerId)
    }

    if (requestFilter.externalDataProviderId != 0L) {
      conjuncts.add("ExternalDataProviderId = @${EXTERNAL_DATA_PROVIDER_ID}")
      bind(EXTERNAL_DATA_PROVIDER_ID to requestFilter.externalDataProviderId)
    }

    if (requestFilter.clientAccountReferenceId.isNotEmpty()) {
      conjuncts.add("ClientAccountReferenceId = @${CLIENT_ACCOUNT_REFERENCE_ID}")
      bind(CLIENT_ACCOUNT_REFERENCE_ID to requestFilter.clientAccountReferenceId)
    }

    if (after != null) {
      conjuncts.add(
        """
          (ClientAccounts.CreateTime < @${CREATE_TIME} OR (
            ClientAccounts.CreateTime = @${CREATE_TIME} AND MeasurementConsumers.ExternalMeasurementConsumerId > @${AFTER_EXTERNAL_MEASUREMENT_CONSUMER_ID}
          ) OR (
            ClientAccounts.CreateTime = @${CREATE_TIME} AND MeasurementConsumers.ExternalMeasurementConsumerId = @${AFTER_EXTERNAL_MEASUREMENT_CONSUMER_ID} AND ClientAccounts.ExternalClientAccountId > @${EXTERNAL_CLIENT_ACCOUNT_ID}
          ))
        """
          .trimIndent()
      )
      bind(CREATE_TIME to after.createTime.toGcloudTimestamp())
      bind(EXTERNAL_CLIENT_ACCOUNT_ID to after.externalClientAccountId)
      bind(AFTER_EXTERNAL_MEASUREMENT_CONSUMER_ID to after.externalMeasurementConsumerId)
    }

    if (conjuncts.isEmpty()) {
      return
    }

    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  companion object {
    private const val LIMIT_PARAM = "limit"
    private const val EXTERNAL_MEASUREMENT_CONSUMER_ID = "externalMeasurementConsumerId"
    private const val AFTER_EXTERNAL_MEASUREMENT_CONSUMER_ID = "afterExternalMeasurementConsumerId"
    private const val EXTERNAL_DATA_PROVIDER_ID = "externalDataProviderId"
    private const val EXTERNAL_CLIENT_ACCOUNT_ID = "externalClientAccountId"
    private const val CLIENT_ACCOUNT_REFERENCE_ID = "clientAccountReferenceId"
    private const val CREATE_TIME = "createTime"
  }
<<<<<<< HEAD
}
=======
}
>>>>>>> de213432e (fix: run linter)
