package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import com.google.cloud.spanner.Statement
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.StreamModelProvidersRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelProviderReader

class StreamModelProviders(
  private val requestFilter: StreamModelProvidersRequest.Filter,
  limit: Int = 0,
) : SimpleSpannerQuery<ModelProviderReader.Result>() {

  override val reader =
    ModelProviderReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause("ORDER BY ModelProviders.ExternalModelProviderId")
      if (limit > 0) {
        appendClause("LIMIT @${LIMIT_PARAM}")
        bind(LIMIT_PARAM to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamModelProvidersRequest.Filter) {
    if (filter.hasAfter()) {
      bind(EXTERNAL_MODEL_PROVIDER_ID to filter.after.externalModelProviderId)
      appendClause("WHERE ")
      append("ModelProviders.ExternalModelProviderId > @${EXTERNAL_MODEL_PROVIDER_ID}")
    }
  }

  companion object {
    const val LIMIT_PARAM = "limit"
    const val EXTERNAL_MODEL_PROVIDER_ID = "externalModelProviderId"
  }
}
