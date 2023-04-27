package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import com.google.cloud.spanner.Statement
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.StreamModelSuitesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelSuiteReader

class StreamModelSuites(
  private val requestFilter: StreamModelSuitesRequest.Filter,
  limit: Int = 0
) : SimpleSpannerQuery<ModelSuiteReader.Result>() {

  override val reader =
    ModelSuiteReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause("ORDER BY ModelSuites.CreateTime ASC")
      if (limit > 0) {
        appendClause("LIMIT @${LIMIT_PARAM}")
        bind(LIMIT_PARAM to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamModelSuitesRequest.Filter) {
    val conjuncts = mutableListOf<String>()

    if (filter.externalModelProviderId != 0L) {
      conjuncts.add("ExternalDataProviderId = @${EXTERNAL_MODEL_PROVIDER_ID_PARAM}")
      bind(EXTERNAL_MODEL_PROVIDER_ID_PARAM to filter.externalModelProviderId)
    }

    if (filter.hasCreatedAfter()) {
      conjuncts.add("ModelSuites.CreateTime > @${CREATED_AFTER}")
      bind(CREATED_AFTER to filter.createdAfter.toGcloudTimestamp())
    }

    check(conjuncts.isNotEmpty())
    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  companion object {
    const val LIMIT_PARAM = "limit"
    const val EXTERNAL_MODEL_PROVIDER_ID_PARAM = "externalModelProviderId"
    const val CREATED_AFTER = "createdAfter"
  }
}
