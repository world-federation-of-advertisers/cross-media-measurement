package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.TransactionContext
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.internal.kingdom.DataProvider

class CreateDataProviderTransaction(private val idGenerator: IdGenerator) {
  fun execute(transactionContext: TransactionContext): DataProvider {
    val internalId = idGenerator.generateInternalId()
    val externalId = idGenerator.generateExternalId()
    transactionContext.buffer(
      Mutation.newInsertBuilder("DataProviders")
        .set("DataProviderId").to(internalId.value)
        .set("ExternalDataProviderId").to(externalId.value)
        .set("DataProviderDetails").to("")
        .set("DataProviderDetailsJson").to("")
        .build()
    )
    return DataProvider.newBuilder().setExternalDataProviderId(externalId.value).build()
  }
}
