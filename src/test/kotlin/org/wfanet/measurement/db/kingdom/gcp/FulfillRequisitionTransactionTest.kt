package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.kingdom.gcp.testing.RequisitionTestBase
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionState

@RunWith(JUnit4::class)
class FulfillRequisitionTransactionTest : RequisitionTestBase() {

  private fun updateExistingRequisitionState(state: RequisitionState) {
    spanner.client.write(
      listOf(
        Mutation
          .newUpdateBuilder("Requisitions")
          .addPrimaryKey(REQUISITION)
          .set("State").to(state.ordinal.toLong())
          .build()
      )
    )
  }

  @Test
  fun success() {
    val expectedRequisition: Requisition =
      REQUISITION
        .toBuilder()
        .setState(RequisitionState.FULFILLED)
        .build()

    for (state in listOf(RequisitionState.FULFILLED, RequisitionState.UNFULFILLED)) {
      updateExistingRequisitionState(state)
      val requisition: Requisition? =
        spanner.client.readWriteTransaction().run { transactionContext ->
          FulfillRequisitionTransaction()
            .execute(transactionContext, ExternalId(EXTERNAL_REQUISITION_ID))
        }
      assertThat(requisition).isEqualTo(expectedRequisition)
      assertThat(readAllRequisitions()).containsExactly(expectedRequisition)
    }
  }

  @Test
  fun `missing requisition`() {
    spanner.client.readWriteTransaction().run { transactionContext ->
      assertFailsWith<IllegalArgumentException> {
        FulfillRequisitionTransaction()
          .execute(transactionContext, ExternalId(EXTERNAL_REQUISITION_ID + 1))
      }
    }
  }
}
