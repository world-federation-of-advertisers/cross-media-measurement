package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader

class StreamDataProviders(externalDataProviderIds: List<Long>) :
  SimpleSpannerQuery<DataProviderReader.Result>() {
  override val reader =
    DataProviderReader().fillStatementBuilder {
      appendClause(
        """
        WHERE ExternalDataProviderId in UNNEST(@externalDataProviderIds)
      """
          .trimIndent()
      )
      bind("externalDataProviderIds").toInt64Array(externalDataProviderIds)
    }
}
