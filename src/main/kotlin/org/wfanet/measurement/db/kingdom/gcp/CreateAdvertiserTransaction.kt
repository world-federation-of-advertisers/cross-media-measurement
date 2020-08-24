package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.TransactionContext
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.internal.kingdom.Advertiser

class CreateAdvertiserTransaction(private val idGenerator: IdGenerator) {
  fun execute(transactionContext: TransactionContext): Advertiser {
    val internalId = idGenerator.generateInternalId()
    val externalId = idGenerator.generateExternalId()
    transactionContext.buffer(
      Mutation.newInsertBuilder("Advertisers")
        .set("AdvertiserId").to(internalId.value)
        .set("ExternalAdvertiserId").to(externalId.value)
        .set("AdvertiserDetails").to("")
        .set("AdvertiserDetailsJson").to("")
        .build()
    )
    return Advertiser.newBuilder().setExternalAdvertiserId(externalId.value).build()
  }
}
