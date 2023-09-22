package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.Population
import org.wfanet.measurement.internal.kingdom.population

class PopulationReader : SpannerReader<PopulationReader.Result>() {
  data class Result(val population: Population, val populationId: Long)

  override val baseSql: String =
    """
    SELECT
      Populations.DataProviderId,
      Populations.PopulationId,
      Populations.ExternalPopulationId,
      Populations.Description,
      Populations.CreateTime,
      Populations.ModelBlobUri,
      Populations.EventTemplateType,
      DataProviders.ExternalDataProviderId
    FROM Populations
    JOIN DataProviders USING (DataProviderId)
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(buildPopulation(struct), struct.getLong("PopulationId"))

  suspend fun readByExternalPopulationId(
    readContext: AsyncDatabaseClient.ReadContext,
    externalDataProviderId: ExternalId,
    externalPopulationId: ExternalId
  ): Result? {
    return fillStatementBuilder {
      appendClause("WHERE ExternalPopulationId = @externalPopulationId AND ExternalDataProviderId = @externalDataProviderId")
      bind("externalPopulationId").to(externalPopulationId.value)
      bind("externalDataProviderId").to(externalDataProviderId.value)
      appendClause("LIMIT 1")
    }
      .execute(readContext)
      .singleOrNull()
  }

  private fun buildPopulation(struct: Struct): Population =
    population {
      externalDataProviderId = struct.getLong("ExternalDataProviderId")
      externalPopulationId = struct.getLong("ExternalPopulationId")
      description = struct.getString("Description")
      createTime = struct.getTimestamp("CreateTime").toProto()
      populationBlob = populationBlob.newBuilderForType().apply { modelBlobUri = struct.getString("ModelBlobUri") }.build()
      eventTemplate = eventTemplate.newBuilderForType().apply { fullyQualifiedType = struct.getString("EventTemplateType") }.build()
    }
}
