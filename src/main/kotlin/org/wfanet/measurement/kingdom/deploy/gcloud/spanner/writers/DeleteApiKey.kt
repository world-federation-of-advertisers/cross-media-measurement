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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.ApiKey
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerApiKeyReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader

/**
 * Creates an account in the database.
 *
 * Throws a [KingdomInternalException] on [execute] with the following codes/conditions:
 * * [KingdomInternalException.Code.API_KEY_NOT_FOUND]
 * * [KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND]
 */
class DeleteApiKey(
  private val externalMeasurementConsumerId: ExternalId,
  private val externalApiKeyId: ExternalId,
) : SimpleSpannerWriter<ApiKey>() {

  override suspend fun TransactionScope.runTransaction(): ApiKey {
    val apiKeyResult = readApiKey(externalApiKeyId)

    transactionContext.bufferUpdateMutation("MeasurementConsumerApiKeys") {
      set(
        "MeasurementConsumerId" to readInternalMeasurementConsumerId(externalMeasurementConsumerId)
      )
      set("ApiKeyId" to apiKeyResult.apiKeyId)
      set("RevocationState" to ApiKey.RevocationState.REVOKED)
    }

    return apiKeyResult.apiKey.copy { revocationState = ApiKey.RevocationState.REVOKED }
  }

  private suspend fun TransactionScope.readInternalMeasurementConsumerId(
    externalMeasurementConsumerId: ExternalId
  ): Long =
    MeasurementConsumerReader()
      .readByExternalMeasurementConsumerId(transactionContext, externalMeasurementConsumerId)
      ?.measurementConsumerId
      ?: throw KingdomInternalException(
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND
      )

  private suspend fun TransactionScope.readApiKey(
    externalApiKeyId: ExternalId
  ): MeasurementConsumerApiKeyReader.Result =
    MeasurementConsumerApiKeyReader().readByExternalId(transactionContext, externalApiKeyId)
      ?: throw KingdomInternalException(KingdomInternalException.Code.API_KEY_NOT_FOUND)
}
