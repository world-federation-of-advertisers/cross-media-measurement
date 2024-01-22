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

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.kingdom.ApiKey
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ApiKeyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerApiKeyReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader

/**
 * Deletes an [ApiKey] from the database.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [ApiKeyNotFoundException] Api key not found
 * @throws [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 */
class DeleteApiKey(
  private val externalMeasurementConsumerId: ExternalId,
  private val externalApiKeyId: ExternalId,
) : SimpleSpannerWriter<ApiKey>() {

  override suspend fun TransactionScope.runTransaction(): ApiKey {
    val apiKeyResult = readApiKey(externalApiKeyId)
    val measurementConsumerId: InternalId =
      readInternalMeasurementConsumerId(externalMeasurementConsumerId)

    transactionContext.buffer(
      Mutation.delete(
        "MeasurementConsumerApiKeys",
        KeySet.singleKey(Key.of(measurementConsumerId.value, apiKeyResult.apiKeyId)),
      )
    )

    return apiKeyResult.apiKey
  }

  private suspend fun TransactionScope.readInternalMeasurementConsumerId(
    externalMeasurementConsumerId: ExternalId
  ): InternalId =
    MeasurementConsumerReader.readMeasurementConsumerId(
      transactionContext,
      externalMeasurementConsumerId,
    ) ?: throw MeasurementConsumerNotFoundException(externalMeasurementConsumerId)

  private suspend fun TransactionScope.readApiKey(
    externalApiKeyId: ExternalId
  ): MeasurementConsumerApiKeyReader.Result =
    MeasurementConsumerApiKeyReader().readByExternalId(transactionContext, externalApiKeyId)
      ?: throw ApiKeyNotFoundException(externalApiKeyId)
}
