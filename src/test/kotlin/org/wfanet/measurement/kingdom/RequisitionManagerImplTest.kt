package org.wfanet.measurement.kingdom

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.test.assertFails
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.kingdom.StreamRequisitionsFilter
import org.wfanet.measurement.db.kingdom.streamRequisitionsFilter
import org.wfanet.measurement.db.kingdom.testing.FakeKingdomRelationalDatabase
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

  private val fakeKingdomRelationalDatabase = FakeKingdomRelationalDatabase()
  private val requisitionManager = RequisitionManagerImpl(fakeKingdomRelationalDatabase)

  @Test
  fun `createRequisition normal`() = runBlocking {
    var capturedRequisition: Requisition? = null
    fakeKingdomRelationalDatabase.writeNewRequisitionFn = {
      capturedRequisition = it
      REQUISITION
    }

    val result = requisitionManager.createRequisition(NEW_REQUISITION)

    assertThat(result).isEqualTo(REQUISITION)
    assertThat(capturedRequisition).isEqualTo(NEW_REQUISITION)
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
    var capturedExternalRequisitionId: ExternalId? = null
    fakeKingdomRelationalDatabase.fulfillRequisitionFn = {
      capturedExternalRequisitionId = it
      REQUISITION
    }

    assertThat(requisitionManager.fulfillRequisition(ExternalId(REQUISITION.externalRequisitionId)))
      .isEqualTo(REQUISITION)

    assertThat(capturedExternalRequisitionId)
      .isEqualTo(ExternalId(REQUISITION.externalRequisitionId))
  }

  @Test
  fun streamRequisitions() = runBlocking<Unit> {
    var capturedFilter: StreamRequisitionsFilter? = null
    var capturedLimit: Long? = null
    fakeKingdomRelationalDatabase.streamRequisitionsFn = { filter, limit ->
      capturedFilter = filter
      capturedLimit = limit
      flowOf(REQUISITION)
    }

    val filter = streamRequisitionsFilter()
    val requisitions =
      requisitionManager
        .streamRequisitions(filter, 10)
        .toList()

    assertThat(requisitions).containsExactly(REQUISITION)
    assertThat(capturedFilter).isSameInstanceAs(filter)
    assertThat(capturedLimit).isEqualTo(10)
  }
}
