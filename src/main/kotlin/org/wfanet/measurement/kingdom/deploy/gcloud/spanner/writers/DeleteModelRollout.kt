package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.ApiKey
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ApiKeyNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.MeasurementConsumerNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerApiKeyReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementConsumerReader

class DeleteModelRollout(
  private val externalModelProviderId: ExternalId,
  private val externalApiKeyId: ExternalId,
) : SimpleSpannerWriter<ApiKey>() {

  override suspend fun TransactionScope.runTransaction(): ApiKey {
    val apiKeyResult = readApiKey(externalApiKeyId)

    transactionContext.buffer(
      Mutation.delete(
        "MeasurementConsumerApiKeys",
        KeySet.singleKey(
          Key.of(
            readInternalMeasurementConsumerId(externalMeasurementConsumerId),
            apiKeyResult.apiKeyId
          )
        )
      )
    )

    return apiKeyResult.apiKey
  }

  private suspend fun TransactionScope.readInternalMeasurementConsumerId(
    externalMeasurementConsumerId: ExternalId
  ): Long =
    MeasurementConsumerReader()
      .readByExternalMeasurementConsumerId(transactionContext, externalMeasurementConsumerId)
      ?.measurementConsumerId
      ?: throw MeasurementConsumerNotFoundException(externalMeasurementConsumerId)

  private suspend fun TransactionScope.readApiKey(
    externalApiKeyId: ExternalId
  ): MeasurementConsumerApiKeyReader.Result =
    MeasurementConsumerApiKeyReader().readByExternalId(transactionContext, externalApiKeyId)
      ?: throw ApiKeyNotFoundException(externalApiKeyId)
}
