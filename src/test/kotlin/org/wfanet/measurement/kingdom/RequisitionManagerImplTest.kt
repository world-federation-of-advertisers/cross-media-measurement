package org.wfanet.measurement.kingdom

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.test.assertFails
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.db.kingdom.StreamRequisitionsFilter
import org.wfanet.measurement.db.kingdom.streamRequisitionsFilter
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.RequisitionState

@RunWith(JUnit4::class)
class RequisitionManagerImplTest {
  companion object {
    val REQUISITION: Requisition = Requisition.newBuilder().apply {
      externalDataProviderId = 1
      externalCampaignId = 2
      externalRequisitionId = 3

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
        clearExternalRequisitionId()
        clearCreateTime()
        state = RequisitionState.UNFULFILLED
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

    override suspend fun streamRequisitions(
      filter: StreamRequisitionsFilter,
      limit: Long
    ): Flow<Requisition> = flowOf(REQUISITION)
  }

  private val requisitionManager = RequisitionManagerImpl(
    RequisitionManagerImplTest.FakeKingdomRelationalDatabase
  )

  @Test
  fun `createRequisition normal`() = runBlocking {
    val result = requisitionManager.createRequisition(NEW_REQUISITION)
    assertThat(result).isEqualTo(REQUISITION)
  }

  @Test
  fun `createRequisition rejects invalid requisitions`() = runBlocking {
    repeat(3) {
      val requisition = REQUISITION.toBuilder().apply {
        // Each time, forget to clear a field that should be empty or otherwise invalidate the input
        when {
          it != 0 -> clearCreateTime()
          it != 1 -> clearExternalRequisitionId()
          it != 2 -> state = RequisitionState.FULFILLED
        }
      }.build()
      assertFails { requisitionManager.createRequisition(requisition) }
    }
  }

  @Test
  fun fulfillRequisition() = runBlocking {
    assertThat(requisitionManager.fulfillRequisition(ExternalId(REQUISITION.externalRequisitionId)))
      .isEqualTo(REQUISITION)
  }

  @Test
  fun streamRequisitions() = runBlocking<Unit> {
    val requisitions =
      requisitionManager
        .streamRequisitions(streamRequisitionsFilter(), 10)
        .toList()

    assertThat(requisitions).containsExactly(REQUISITION)
  }
}
