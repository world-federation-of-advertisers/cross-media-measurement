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
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.ApiKey
import org.wfanet.measurement.internal.kingdom.apiKey

class MeasurementConsumerApiKeyReader : SpannerReader<MeasurementConsumerApiKeyReader.Result>() {
  data class Result(val apiKey: ApiKey, val apiKeyId: Long)

  override val baseSql: String =
    """
    SELECT
      MeasurementConsumerApiKeys.ApiKeyId,
      MeasurementConsumerApiKeys.ExternalMeasurementConsumerApiKeyId,
      MeasurementConsumerApiKeys.Nickname,
      MeasurementConsumerApiKeys.Description,
      MeasurementConsumers.ExternalMeasurementConsumerId,
    FROM MeasurementConsumerApiKeys
    LEFT JOIN MeasurementConsumers
      ON (MeasurementConsumerApiKeys.MeasurementConsumerId = MeasurementConsumers.MeasurementConsumerId)
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(buildApiKey(struct), struct.getLong("ApiKeyId"))

  private fun buildApiKey(struct: Struct): ApiKey = apiKey {
    externalMeasurementConsumerId = struct.getLong("ExternalMeasurementConsumerId")
    externalApiKeyId = struct.getLong("ExternalMeasurementConsumerApiKeyId")
    nickname = struct.getString("Nickname")
    if (!struct.isNull("Description")) {
      description = struct.getString("Description")
    }
  }

  suspend fun readByExternalId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalMeasurementConsumerApiKeyId: ExternalId,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          "WHERE ExternalMeasurementConsumerApiKeyId = @externalMeasurementConsumerApiKeyId"
        )
        bind("externalMeasurementConsumerApiKeyId").to(externalMeasurementConsumerApiKeyId.value)
      }
      .execute(readContext)
      .singleOrNull()
  }

  suspend fun readByAuthenticationKeyHash(
    readContext: AsyncDatabaseClient.ReadContext,
    authenticationKeyHash: ByteString,
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          "WHERE MeasurementConsumerApiKeys.AuthenticationKeyHash = @authenticationKeyHash"
        )
        bind("authenticationKeyHash" to authenticationKeyHash.toGcloudByteArray())
      }
      .execute(readContext)
      .singleOrNull()
  }
}
