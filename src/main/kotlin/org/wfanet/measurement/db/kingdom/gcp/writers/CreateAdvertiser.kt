package org.wfanet.measurement.db.kingdom.gcp.writers

import com.google.cloud.spanner.Mutation
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.bufferTo
import org.wfanet.measurement.internal.kingdom.Advertiser

class CreateAdvertiser : SpannerWriter<ExternalId, Advertiser>() {
  override suspend fun TransactionScope.runTransaction(): ExternalId {
    val internalId = idGenerator.generateInternalId()
    val externalId = idGenerator.generateExternalId()
    Mutation.newInsertBuilder("Advertisers")
      .set("AdvertiserId").to(internalId.value)
      .set("ExternalAdvertiserId").to(externalId.value)
      .set("AdvertiserDetails").to("")
      .set("AdvertiserDetailsJson").to("")
      .build()
      .bufferTo(transactionContext)
    return externalId
  }

  override fun ResultScope<ExternalId>.buildResult(): Advertiser {
    val externalAdvertiserId = checkNotNull(transactionResult).value
    return Advertiser.newBuilder().setExternalAdvertiserId(externalAdvertiserId).build()
  }
}
