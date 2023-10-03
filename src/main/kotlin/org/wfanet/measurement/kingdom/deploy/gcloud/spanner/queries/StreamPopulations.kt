package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import com.google.cloud.spanner.Statement
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.StreamPopulationsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.PopulationReader

class StreamPopulations(
  private val requestFilter: StreamPopulationsRequest.Filter,
  limit: Int = 0
) : SimpleSpannerQuery<PopulationReader.Result>() {
  override val reader =
    PopulationReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause(
        """
          ORDER BY Populations.CreateTime DESC,
          Populations.ExternalDataProviderId ASC,
          Populations.ExternalPopulationId ASC,
        """
          .trimIndent()
      )
      if (limit > 0) {
        appendClause("LIMIT @${LIMIT_PARAM}")
        bind(LIMIT_PARAM to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamPopulationsRequest.Filter) {
    val conjuncts = mutableListOf<String>()

    if (filter.externalDataProviderId != 0L) {
      conjuncts.add("ExternalDataProviderId = @${EXTERNAL_DATA_PROVIDER_ID}")
      bind(EXTERNAL_DATA_PROVIDER_ID to filter.externalDataProviderId)
    }

    if (filter.hasAfter()) {
      conjuncts.add(
        """
          Populations.CreateTime > @${CREATED_AFTER} OR (
            Populations.CreateTime = @${CREATED_AFTER} AND (
              DataProviders.ExternalDataProviderId = @${AFTER_EXTERNAL_DATA_PROVIDER_ID} AND
                Populations.ExternalPopulationId > @${EXTERNAL_POPULATION_ID}
            ) OR (
              DataProviders.ExternalDataProviderId > @${AFTER_EXTERNAL_DATA_PROVIDER_ID}
            )
          )
        """
          .trimIndent()
      )
      bind(CREATED_AFTER to filter.after.createTime.toGcloudTimestamp())
      bind(EXTERNAL_POPULATION_ID to filter.after.externalPopulationId)
      bind(AFTER_EXTERNAL_DATA_PROVIDER_ID to filter.after.externalDataProviderId)
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
    private const val EXTERNAL_POPULATION_ID = "externalPopulationId"
    private const val CREATED_AFTER = "createdAfter"
  }
}
