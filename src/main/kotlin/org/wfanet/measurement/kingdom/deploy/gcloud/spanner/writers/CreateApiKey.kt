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

import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.ApiKey
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader

/**
 * Creates an [ApiKey] in the database.
 *
 * Throws a subclass of [KingdomInternalException] on [execute].
 *
 * @throws [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 */
class CreateApiKey(
  private val apiKey: ApiKey,
) : SimpleSpannerWriter<ApiKey>() {

  override suspend fun TransactionScope.runTransaction(): ApiKey {
    val internalApiKeyId = idGenerator.generateInternalId()
    val externalApiKeyId = idGenerator.generateExternalId()
    val authenticationKey = idGenerator.generateExternalId()
    val authenticationKeyHash = hashSha256(authenticationKey.value)

    transactionContext.bufferInsertMutation("MeasurementConsumerApiKeys") {
      set(
        "MeasurementConsumerId" to
          readInternalMeasurementConsumerId(ExternalId(apiKey.externalMeasurementConsumerId))
      )
      set("ApiKeyId" to internalApiKeyId)
      set("ExternalMeasurementConsumerApiKeyId" to externalApiKeyId)
      set("Nickname" to apiKey.nickname)
      set("Description" to apiKey.description)
      set("AuthenticationKeyHash" to authenticationKeyHash.toGcloudByteArray())
    }

    return apiKey.copy {
      this.externalApiKeyId = externalApiKeyId.value
      this.authenticationKey = authenticationKey.value
    }
  }

  private suspend fun TransactionScope.readInternalMeasurementConsumerId(
    externalMeasurementConsumerId: ExternalId
  ): Long =
    MeasurementConsumerReader()
      .readByExternalMeasurementConsumerId(transactionContext, externalMeasurementConsumerId)
      ?.measurementConsumerId
      ?: throw MeasurementConsumerNotFoundException(externalMeasurementConsumerId)
}
