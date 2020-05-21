package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.ByteArray
import com.google.cloud.Timestamp
import com.google.cloud.spanner.Mutation
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.test.assertNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.db.gcp.executeSqlQuery
import org.wfanet.measurement.db.gcp.runReadWriteTransaction
import org.wfanet.measurement.db.gcp.testing.UsingSpannerEmulator
import org.wfanet.measurement.db.gcp.toJson
import org.wfanet.measurement.db.gcp.toSpannerByteArray
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.RequisitionState

@RunWith(JUnit4::class)
class CreateRequisitionTransactionTest :
  UsingSpannerEmulator("/src/main/db/gcp/measurement_provider.sdl") {

  companion object {
    const val DATA_PROVIDER_ID = 1L
    const val EXTERNAL_DATA_PROVIDER_ID = 101L
    const val CAMPAIGN_ID = 2L
    const val EXTERNAL_CAMPAIGN_ID = 102L
    const val REQUISITION_ID = 3L
    const val EXTERNAL_REQUISITION_ID = 103L
    const val IRRELEVANT_ADVERTISER_ID = 99L

    val START_TIME: Timestamp = Timestamp.ofTimeSecondsAndNanos(123, 0)
    val END_TIME: Timestamp = Timestamp.ofTimeSecondsAndNanos(456, 0)
    val DETAILS: RequisitionDetails = RequisitionDetails.newBuilder().apply {
      metricDefinitionBuilder.sketchBuilder.sketchConfigId = 10101
    }.build()

    val NEW_TIMESTAMP: Timestamp = Timestamp.ofTimeSecondsAndNanos(999, 0)
    val NEW_REQUISITION_ID = 555L
    val NEW_DETAILS: RequisitionDetails = RequisitionDetails.newBuilder().apply {
      metricDefinitionBuilder.sketchBuilder.sketchConfigId = 20202
    }.build()

    val REQUISITION: Requisition = Requisition.newBuilder().apply {
      dataProviderId = DATA_PROVIDER_ID
      campaignId = CAMPAIGN_ID
      requisitionId = REQUISITION_ID
      windowStartTime = START_TIME.toProto()
      windowEndTime = END_TIME.toProto()
      state = RequisitionState.UNFULFILLED
      requisitionDetails = DETAILS
      requisitionDetailsJson = DETAILS.toJson()
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
      externalCampaignId = EXTERNAL_CAMPAIGN_ID
      externalRequisitionId = EXTERNAL_REQUISITION_ID
    }.build()
  }

  @Before
  fun populateDatabase() {
    spanner.client.runReadWriteTransaction { transactionContext ->
      transactionContext.buffer(
        Mutation.newInsertBuilder("DataProviders").apply {
          set("DataProviderId").to(DATA_PROVIDER_ID)
          set("ExternalDataProviderId").to(EXTERNAL_DATA_PROVIDER_ID)
          set("DataProviderDetails").to(ByteArray.copyFrom(""))
          set("DataProviderDetailsJson").to("")
        }.build()
      )

      transactionContext.buffer(
        Mutation.newInsertBuilder("Campaigns").apply {
          set("DataProviderId").to(DATA_PROVIDER_ID)
          set("CampaignId").to(CAMPAIGN_ID)
          set("AdvertiserId").to(IRRELEVANT_ADVERTISER_ID)
          set("ExternalCampaignId").to(EXTERNAL_CAMPAIGN_ID)
          set("ProvidedCampaignId").to("irrelevant")
          set("CampaignDetails").to(ByteArray.copyFrom(""))
          set("CampaignDetailsJson").to("")
        }.build()
      )

      transactionContext.buffer(
        Mutation.newInsertBuilder("Requisitions").apply {
          set("DataProviderId").to(DATA_PROVIDER_ID)
          set("CampaignId").to(CAMPAIGN_ID)
          set("RequisitionId").to(REQUISITION_ID)
          set("ExternalRequisitionId").to(EXTERNAL_REQUISITION_ID)
          set("WindowStartTime").to(START_TIME)
          set("WindowEndTime").to(END_TIME)
          set("State").to(RequisitionState.UNFULFILLED.ordinal.toLong())
          set("RequisitionDetails").to(DETAILS.toSpannerByteArray())
          set("RequisitionDetailsJson").to(DETAILS.toJson())
        }.build()
      )
    }
  }

  private fun readAllRequisitions(): List<Requisition> = runBlocking {
    spanner
      .client
      .singleUse()
      .executeSqlQuery(REQUISITION_READ_QUERY)
      .map { it.toRequisition() }
      .toList()
  }

  @Test
  fun `requisition already exists`() {
    val existing: Requisition? = spanner.client.runReadWriteTransaction {
      CreateRequisitionTransaction().execute(it, REQUISITION)
    }
    assertThat(existing).isEqualTo(REQUISITION)
    assertThat(readAllRequisitions()).containsExactly(REQUISITION)
  }

  @Test
  fun `start time used in idempotency`() {
    val newRequisition = REQUISITION.toBuilder().apply {
      requisitionId = NEW_REQUISITION_ID
      windowStartTime = NEW_TIMESTAMP.toProto()
    }.build()

    val existing: Requisition? = spanner.client.readWriteTransaction().run {
      CreateRequisitionTransaction().execute(it, newRequisition)
    }
    assertNull(existing)
    assertThat(readAllRequisitions()).containsExactly(
      REQUISITION, newRequisition
    )
  }

  @Test
  fun `end time used in idempotency`() {
    val newRequisition = REQUISITION.toBuilder().apply {
      requisitionId = NEW_REQUISITION_ID
      windowEndTime = NEW_TIMESTAMP.toProto()
    }.build()

    val existing: Requisition? = spanner.client.readWriteTransaction().run {
      CreateRequisitionTransaction().execute(it, newRequisition)
    }
    assertNull(existing)
    assertThat(readAllRequisitions()).containsExactly(
      REQUISITION, newRequisition
    )
  }

  @Test
  fun `details used in idempotency`() {
    val newRequisition = REQUISITION.toBuilder().apply {
      requisitionId = NEW_REQUISITION_ID
      requisitionDetails = NEW_DETAILS
    }.build()

    val existing: Requisition? = spanner.client.readWriteTransaction().run {
      CreateRequisitionTransaction().execute(it, newRequisition)
    }
    assertNull(existing)
    assertThat(readAllRequisitions()).containsExactly(
      REQUISITION, newRequisition
    )
  }
}
