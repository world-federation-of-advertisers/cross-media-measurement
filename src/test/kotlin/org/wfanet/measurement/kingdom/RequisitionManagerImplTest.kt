package org.wfanet.measurement.kingdom

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Instant
import kotlin.test.assertFails
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.Pagination
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.RequisitionState

@RunWith(JUnit4::class)
class RequisitionManagerImplTest {
  companion object {
    const val EXISTING_REQUISITION_ID = 999L
    val NOW: Instant = Instant.ofEpochSecond(9999999)

    val REQUISITION: Requisition = Requisition.newBuilder().apply {
      dataProviderId = 1
      campaignId = 2
      requisitionId = 3

      externalDataProviderId = 4
      externalCampaignId = 5
      externalRequisitionId = 6

      createTimeBuilder.seconds = 123
      windowStartTimeBuilder.seconds = 456
      windowEndTimeBuilder.seconds = 789

      state = RequisitionState.FULFILLED

      requisitionDetails = RequisitionDetails.getDefaultInstance()
      requisitionDetailsJson = "irrelevant-requisition-details-json"
    }.build()

    // A Requisition to try to create.
    val NEW_REQUISITION: Requisition =
      REQUISITION.toBuilder().apply {
        // The ids should be generated from the fake RandomIdGenerator:
        requisitionId = 999
        externalRequisitionId = 999

        // There should be no create time.
        clearCreateTime()

        // State should be unfulfilled:
        state = RequisitionState.UNFULFILLED

        // There should be no internal Data Provider or Campaign identifiers:
        clearDataProviderId()
        clearCampaignId()
      }.build()
  }

  object FakeKingdomRelationalDatabase : KingdomRelationalDatabase {
    override suspend fun writeNewRequisition(requisition: Requisition): Requisition {
      assertThat(requisition).isEqualTo(NEW_REQUISITION)
      return REQUISITION
    }

    override suspend fun fulfillRequisition(externalRequisitionId: ExternalId): Requisition {
      assertThat(externalRequisitionId).isEqualTo(ExternalId(REQUISITION.externalRequisitionId))
      return REQUISITION
    }

    override suspend fun listRequisitions(
      externalCampaignId: ExternalId,
      states: Set<RequisitionState>,
      pagination: Pagination
    ): KingdomRelationalDatabase.ListResult {
      assertThat(externalCampaignId).isEqualTo(ExternalId(REQUISITION.externalCampaignId))
      assertThat(states).containsExactly(RequisitionState.FULFILLED)
      assertThat(pagination).isEqualTo(Pagination(10, "some-page-token"))
      return KingdomRelationalDatabase.ListResult(
        listOf(REQUISITION), "next-page-token"
      )
    }
  }

  private val requisitionManager = RequisitionManagerImpl(
    object : RandomIdGenerator {
      override fun generate(): Long = 999
    },
    FakeKingdomRelationalDatabase
  )

  @Test
  fun `createRequisition adds ids`() = runBlocking {
    val requisition = REQUISITION.toBuilder().apply {
      clearDataProviderId()
      clearCampaignId()
      clearRequisitionId()
      clearCreateTime()
      clearExternalRequisitionId()
      state = RequisitionState.UNFULFILLED
    }.build()
    val result = requisitionManager.createRequisition(requisition)
    assertThat(result).isEqualTo(REQUISITION)
  }

  @Test
  fun `createRequisition rejects invalid requisitions`() = runBlocking {
    repeat(6) {
      val requisition = REQUISITION.toBuilder().apply {
        // Each time, forget to clear a field that should be empty or otherwise invalidate the input
        when {
          it != 0 -> clearDataProviderId()
          it != 1 -> clearCampaignId()
          it != 2 -> clearRequisitionId()
          it != 3 -> clearCreateTime()
          it != 4 -> clearExternalRequisitionId()
          it != 5 -> state = RequisitionState.FULFILLED
        }
      }.build()
      assertFails { requisitionManager.createRequisition(requisition) }
    }
  }

  @Test
  fun fulfillRequisition() = runBlocking {
    val key = RequisitionExternalKey(
      ExternalId(REQUISITION.externalDataProviderId),
      ExternalId(REQUISITION.externalCampaignId),
      ExternalId(REQUISITION.externalRequisitionId)
    )
    assertThat(requisitionManager.fulfillRequisition(key)).isEqualTo(REQUISITION)
  }

  @Test
  fun listRequisitions() = runBlocking {
    val key = CampaignExternalKey(
      ExternalId(REQUISITION.externalDataProviderId),
      ExternalId(REQUISITION.externalCampaignId)
    )
    val states = setOf(RequisitionState.FULFILLED)
    val pagination = Pagination(10, "some-page-token")

    assertThat(requisitionManager.listRequisitions(key, states, pagination)).isEqualTo(
      RequisitionManager.ListResult(
        listOf(REQUISITION),
        "next-page-token"
      )
    )
  }
}
