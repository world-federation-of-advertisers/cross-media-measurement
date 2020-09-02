package org.wfanet.measurement.db.kingdom.gcp.writers

import com.google.cloud.spanner.Mutation
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.bufferTo
import org.wfanet.measurement.internal.kingdom.DataProvider

class CreateDataProvider : SpannerWriter<ExternalId, DataProvider>() {
  override suspend fun TransactionScope.runTransaction(): ExternalId {
    val internalId = idGenerator.generateInternalId()
    val externalId = idGenerator.generateExternalId()
    Mutation.newInsertBuilder("DataProviders")
      .set("DataProviderId").to(internalId.value)
      .set("ExternalDataProviderId").to(externalId.value)
      .set("DataProviderDetails").to("")
      .set("DataProviderDetailsJson").to("")
      .build()
      .bufferTo(transactionContext)
    return externalId
  }

  override fun ResultScope<ExternalId>.buildResult(): DataProvider {
    val externalDataProviderId = checkNotNull(transactionResult).value
    return DataProvider.newBuilder().setExternalDataProviderId(externalDataProviderId).build()
  }
}
