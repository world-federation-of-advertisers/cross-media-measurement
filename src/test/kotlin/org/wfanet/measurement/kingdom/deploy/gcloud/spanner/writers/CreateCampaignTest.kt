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

import com.google.cloud.ByteArray
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TimestampBound
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.test.assertFails
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.internal.kingdom.Campaign
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.testing.KingdomDatabaseTestBase

private const val DATA_PROVIDER_ID = 1L
private const val EXTERNAL_DATA_PROVIDER_ID = 2L
private const val ADVERTISER_ID = 3L
private const val EXTERNAL_ADVERTISER_ID = 4L
private const val CAMPAIGN_ID = 5L
private const val EXTERNAL_CAMPAIGN_ID = 6L
private const val PROVIDED_CAMPAIGN_ID = "some-provided-campaign-id"

@RunWith(JUnit4::class)
class CreateCampaignTest : KingdomDatabaseTestBase() {
  private val idGenerator =
    FixedIdGenerator(InternalId(CAMPAIGN_ID), ExternalId(EXTERNAL_CAMPAIGN_ID))

  private suspend fun createCampaign(
    externalDataProviderId: Long,
    externalAdvertiserId: Long
  ): Campaign {
    return CreateCampaign(
      ExternalId(externalDataProviderId),
      ExternalId(externalAdvertiserId),
      PROVIDED_CAMPAIGN_ID
    ).execute(databaseClient, idGenerator)
  }

  @Before
  fun populateDatabase() = runBlocking {
    insertDataProvider(DATA_PROVIDER_ID, EXTERNAL_DATA_PROVIDER_ID)
    insertAdvertiser(ADVERTISER_ID, EXTERNAL_ADVERTISER_ID)
  }

  private suspend fun readCampaignStructs(): List<Struct> =
    databaseClient
      .singleUse(TimestampBound.strong())
      .executeQuery(Statement.of("SELECT * FROM Campaigns"))
      .toList()

  @Test
  fun success() = runBlocking<Unit> {
    val campaign = createCampaign(EXTERNAL_DATA_PROVIDER_ID, EXTERNAL_ADVERTISER_ID)

    assertThat(campaign)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        Campaign.newBuilder().apply {
          externalAdvertiserId = EXTERNAL_ADVERTISER_ID
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
          externalCampaignId = EXTERNAL_CAMPAIGN_ID
          providedCampaignId = PROVIDED_CAMPAIGN_ID
        }.build()
      )

    assertThat(readCampaignStructs())
      .containsExactly(
        Struct.newBuilder()
          .set("DataProviderId").to(DATA_PROVIDER_ID)
          .set("CampaignId").to(CAMPAIGN_ID)
          .set("AdvertiserId").to(ADVERTISER_ID)
          .set("ExternalCampaignId").to(EXTERNAL_CAMPAIGN_ID)
          .set("ProvidedCampaignId").to(PROVIDED_CAMPAIGN_ID)
          .set("CampaignDetails").to(ByteArray.copyFrom(""))
          .set("CampaignDetailsJson").to("")
          .build()
      )
  }

  @Test
  fun `invalid data provider id`() = runBlocking {
    assertFails {
      createCampaign(99999L, EXTERNAL_ADVERTISER_ID)
    }

    assertThat(readCampaignStructs()).isEmpty()
  }

  @Test
  fun `invalid campaign id`() = runBlocking {
    assertFails {
      createCampaign(EXTERNAL_DATA_PROVIDER_ID, 999999L)
    }

    assertThat(readCampaignStructs()).isEmpty()
  }
}
