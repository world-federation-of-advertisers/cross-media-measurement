package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader

class StreamDataProviders(externalDataProviderIds: Iterable<ExternalId>) :
  SimpleSpannerQuery<DataProviderReader.Result>() {
  override val reader =
    DataProviderReader().fillStatementBuilder {
      appendClause(
        """
        WHERE ExternalDataProviderId in UNNEST(@externalDataProviderIds)
      """
          .trimIndent()
      )
      bind("externalDataProviderIds").toInt64Array(externalDataProviderIds.map { it.value })
    }
}
