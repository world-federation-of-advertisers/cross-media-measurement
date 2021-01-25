// Copyright 2020 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import com.google.cloud.spanner.Mutation
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.bufferTo
import org.wfanet.measurement.internal.kingdom.Campaign
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.AdvertiserReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.DataProviderReader

class CreateCampaign(
  private val externalDataProviderId: ExternalId,
  private val externalAdvertiserId: ExternalId,
  private val providedCampaignId: String
) : SpannerWriter<ExternalId, Campaign>() {
  override suspend fun TransactionScope.runTransaction(): ExternalId {
    val (dataProviderId, advertiserId) = coroutineScope {
      val dataProviderIed = async { readDataProviderId() }
      val advertiserId = async { readAdvertiserId() }
      Pair(dataProviderIed.await(), advertiserId.await())
    }
    val internalId = idGenerator.generateInternalId()
    val externalId = idGenerator.generateExternalId()
    Mutation.newInsertBuilder("Campaigns")
      .set("DataProviderId").to(dataProviderId.value)
      .set("CampaignId").to(internalId.value)
      .set("AdvertiserId").to(advertiserId.value)
      .set("ExternalCampaignId").to(externalId.value)
      .set("ProvidedCampaignId").to(providedCampaignId)
      .set("CampaignDetails").to("")
      .set("CampaignDetailsJson").to("")
      .build()
      .bufferTo(transactionContext)
    return externalId
  }

  override fun ResultScope<ExternalId>.buildResult(): Campaign {
    val externalCampaignId = checkNotNull(transactionResult).value
    return Campaign.newBuilder()
      .setExternalAdvertiserId(externalAdvertiserId.value)
      .setExternalDataProviderId(externalDataProviderId.value)
      .setExternalCampaignId(externalCampaignId)
      .setProvidedCampaignId(providedCampaignId)
      .build()
  }

  private suspend fun TransactionScope.readDataProviderId(): InternalId {
    val readResult = DataProviderReader().readExternalId(transactionContext, externalDataProviderId)
    return InternalId(readResult.dataProviderId)
  }

  private suspend fun TransactionScope.readAdvertiserId(): InternalId {
    val readResult = AdvertiserReader().readExternalId(transactionContext, externalAdvertiserId)
    return InternalId(readResult.advertiserId)
  }
}
