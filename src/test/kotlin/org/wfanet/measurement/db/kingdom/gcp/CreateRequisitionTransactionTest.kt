package org.wfanet.measurement.db.kingdom.gcp

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.test.assertNull
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.db.gcp.runReadWriteTransaction
import org.wfanet.measurement.db.kingdom.gcp.testing.RequisitionTestBase
import org.wfanet.measurement.internal.kingdom.Requisition

@RunWith(JUnit4::class)
class CreateRequisitionTransactionTest : RequisitionTestBase() {
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
