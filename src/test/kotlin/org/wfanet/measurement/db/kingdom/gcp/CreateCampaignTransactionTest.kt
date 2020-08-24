// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.ByteArray
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.TimestampBound
import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFails
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.InternalId
import org.wfanet.measurement.common.testing.FixedIdGenerator
import org.wfanet.measurement.db.gcp.asSequence
import org.wfanet.measurement.db.gcp.runReadWriteTransaction
import org.wfanet.measurement.db.kingdom.gcp.testing.KingdomDatabaseTestBase

private const val DATA_PROVIDER_ID = 1L
private const val EXTERNAL_DATA_PROVIDER_ID = 2L
private const val ADVERTISER_ID = 3L
private const val EXTERNAL_ADVERTISER_ID = 4L
private const val CAMPAIGN_ID = 5L
private const val EXTERNAL_CAMPAIGN_ID = 6L
private const val PROVIDED_CAMPAIGN_ID = "some-provided-campaign-id"

@RunWith(JUnit4::class)
class CreateCampaignTransactionTest : KingdomDatabaseTestBase() {
  private val idGenerator =
    FixedIdGenerator(InternalId(CAMPAIGN_ID), ExternalId(EXTERNAL_CAMPAIGN_ID))
  private val transaction = CreateCampaignTransaction(idGenerator)

  @Before
  fun populateDatabase() {
    insertDataProvider(DATA_PROVIDER_ID, EXTERNAL_DATA_PROVIDER_ID)
    insertAdvertiser(ADVERTISER_ID, EXTERNAL_ADVERTISER_ID)
  }

  private fun readCampaignStructs(): List<Struct> =
    databaseClient
      .singleUse(TimestampBound.strong())
      .executeQuery(Statement.of("SELECT * FROM Campaigns"))
      .asSequence()
      .toList()

  @Test
  fun success() = runBlocking<Unit> {
    databaseClient.runReadWriteTransaction { transactionContext ->
      transaction.execute(
        transactionContext,
        ExternalId(EXTERNAL_DATA_PROVIDER_ID),
        ExternalId(EXTERNAL_ADVERTISER_ID),
        PROVIDED_CAMPAIGN_ID
      )
    }

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
  fun `invalid data provider id`() = runBlocking<Unit> {
    assertFails {
      databaseClient.runReadWriteTransaction { transactionContext ->
        transaction.execute(
          transactionContext,
          ExternalId(99999L),
          ExternalId(EXTERNAL_ADVERTISER_ID),
          PROVIDED_CAMPAIGN_ID
        )
      }
    }

    assertThat(readCampaignStructs()).isEmpty()
  }

  @Test
  fun `invalid campaign id`() = runBlocking<Unit> {
    assertFails {
      databaseClient.runReadWriteTransaction { transactionContext ->
        transaction.execute(
          transactionContext,
          ExternalId(EXTERNAL_DATA_PROVIDER_ID),
          ExternalId(999999L),
          PROVIDED_CAMPAIGN_ID
        )
      }
    }

    assertThat(readCampaignStructs()).isEmpty()
  }
}
