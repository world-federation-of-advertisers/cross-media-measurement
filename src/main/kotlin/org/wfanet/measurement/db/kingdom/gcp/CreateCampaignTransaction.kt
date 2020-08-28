package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.ReadContext
import com.google.cloud.spanner.TransactionContext
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.InternalId
import org.wfanet.measurement.internal.kingdom.Campaign

class CreateCampaignTransaction(private val idGenerator: IdGenerator) {
  fun execute(
    transactionContext: TransactionContext,
    externalDataProviderId: ExternalId,
    externalAdvertiserId: ExternalId,
    providedCampaignId: String
  ): Campaign = runBlocking {
    // TODO: do we need validation that providedCampaignId is unique for the data provider?
    val dataProviderId = async { readDataProviderId(transactionContext, externalDataProviderId) }
    val advertiserId = async { readAdvertiserId(transactionContext, externalAdvertiserId) }
    val internalId = idGenerator.generateInternalId()
    val externalId = idGenerator.generateExternalId()
    transactionContext.buffer(
      Mutation.newInsertBuilder("Campaigns")
        .set("DataProviderId").to(dataProviderId.await().value)
        .set("CampaignId").to(internalId.value)
        .set("AdvertiserId").to(advertiserId.await().value)
        .set("ExternalCampaignId").to(externalId.value)
        .set("ProvidedCampaignId").to(providedCampaignId)
        .set("CampaignDetails").to("")
        .set("CampaignDetailsJson").to("")
        .build()
    )
    Campaign.newBuilder()
      .setExternalAdvertiserId(externalAdvertiserId.value)
      .setExternalDataProviderId(externalDataProviderId.value)
      .setExternalCampaignId(externalId.value)
      .setProvidedCampaignId(providedCampaignId)
      .build()
  }

  private suspend fun readDataProviderId(
    readContext: ReadContext,
    externalDataProviderId: ExternalId
  ): InternalId {
    val readResult = DataProviderReader().readExternalId(readContext, externalDataProviderId)
    return InternalId(readResult.dataProviderId)
  }

  private suspend fun readAdvertiserId(
    readContext: ReadContext,
    externalAdvertiserId: ExternalId
  ): InternalId {
    val readResult = AdvertiserReader().readExternalId(readContext, externalAdvertiserId)
    return InternalId(readResult.advertiserId)
  }
}
