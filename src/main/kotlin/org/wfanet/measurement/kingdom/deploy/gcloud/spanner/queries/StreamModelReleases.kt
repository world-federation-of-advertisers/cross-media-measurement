package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import com.google.cloud.spanner.Statement
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.StreamModelReleasesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelReleaseReader

class StreamModelReleases(
  private val requestFilter: StreamModelReleasesRequest.Filter,
  limit: Int = 0
) : SimpleSpannerQuery<ModelReleaseReader.Result>() {

  override val reader =
    ModelReleaseReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause("ORDER BY ModelReleases.CreateTime ASC")
      if (limit > 0) {
        appendClause("LIMIT @${LIMIT_PARAM}")
        bind(LIMIT_PARAM to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamModelReleasesRequest.Filter) {
    val conjuncts = mutableListOf<String>()

    if (filter.externalModelProviderId != 0L) {
      conjuncts.add("ExternalModelProviderId = @${EXTERNAL_MODEL_PROVIDER_ID_PARAM}")
      bind(EXTERNAL_MODEL_PROVIDER_ID_PARAM to filter.externalModelProviderId)
    }

    if (filter.externalModelSuiteId != 0L) {
      conjuncts.add("ExternalModelSuiteId = @${EXTERNAL_MODEL_SUITE_ID_PARAM}")
      bind(EXTERNAL_MODEL_SUITE_ID_PARAM to filter.externalModelSuiteId)
    }

    if (filter.hasCreatedAfter()) {
      conjuncts.add("ModelReleases.CreateTime > @${CREATED_AFTER}")
      bind(CREATED_AFTER to filter.createdAfter.toGcloudTimestamp())
    }

    if (conjuncts.isEmpty()) {
      return
    }

    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  companion object {
    const val LIMIT_PARAM = "limit"
    const val EXTERNAL_MODEL_SUITE_ID_PARAM = "externalModelSuiteId"
    const val EXTERNAL_MODEL_PROVIDER_ID_PARAM = "externalModelProviderId"
    const val CREATED_AFTER = "createdAfter"
  }
}
