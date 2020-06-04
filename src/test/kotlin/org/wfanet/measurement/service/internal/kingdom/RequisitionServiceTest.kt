package org.wfanet.measurement.service.internal.kingdom

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Instant
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.kingdom.StreamRequisitionsFilter
import org.wfanet.measurement.db.kingdom.streamRequisitionsFilter
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionServiceGrpcKt
import org.wfanet.measurement.internal.kingdom.RequisitionState
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest
import org.wfanet.measurement.kingdom.RequisitionManager
import org.wfanet.measurement.service.testing.GrpcTestServerRule

@RunWith(JUnit4::class)
class RequisitionServiceTest {

  companion object {
    val REQUISITION: Requisition = Requisition.newBuilder().apply {
      externalDataProviderId = 1
      externalCampaignId = 2
      externalRequisitionId = 3
      createTimeBuilder.seconds = 456
      state = RequisitionState.FULFILLED
    }.build()
  }

  object FakeRequisitionManager : RequisitionManager {
    var streamRequisitionsFn: (StreamRequisitionsFilter, Long) -> Flow<Requisition> =
      { _, _ -> emptyFlow() }

    override suspend fun createRequisition(requisition: Requisition): Requisition =
      REQUISITION

    override suspend fun fulfillRequisition(externalRequisitionId: ExternalId): Requisition =
      REQUISITION

    override fun streamRequisitions(
      filter: StreamRequisitionsFilter,
      limit: Long
    ): Flow<Requisition> =
      streamRequisitionsFn(filter, limit)
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    listOf(RequisitionService(FakeRequisitionManager))
  }

  private val stub: RequisitionServiceGrpcKt.RequisitionServiceCoroutineStub by lazy {
    RequisitionServiceGrpcKt.RequisitionServiceCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun createRequisition() = runBlocking<Unit> {
    assertThat(stub.createRequisition(REQUISITION))
      .isEqualTo(REQUISITION)
  }

  @Test
  fun fulfillRequisition() = runBlocking<Unit> {
    val request: FulfillRequisitionRequest =
      FulfillRequisitionRequest.newBuilder()
        .setExternalRequisitionId(12345)
        .build()

    assertThat(stub.fulfillRequisition(request))
      .isEqualTo(REQUISITION)
  }

  @Test
  fun streamRequisitions() = runBlocking<Unit> {
    val request: StreamRequisitionsRequest =
      StreamRequisitionsRequest.newBuilder().apply {
        limit = 10
        filterBuilder.apply {
          addExternalDataProviderIds(1)
          addExternalDataProviderIds(2)
          addStates(RequisitionState.FULFILLED)
          createdAfterBuilder.seconds = 12345
        }
      }.build()

    var capturedFilter: StreamRequisitionsFilter? = null
    var capturedLimit: Long = 0

    FakeRequisitionManager.streamRequisitionsFn = { filter, limit ->
      capturedFilter = filter
      capturedLimit = limit
      flowOf(REQUISITION, REQUISITION)
    }

    assertThat(stub.streamRequisitions(request).toList())
      .containsExactly(REQUISITION, REQUISITION)

    val expectedFilter = streamRequisitionsFilter(
      externalDataProviderIds = listOf(ExternalId(1), ExternalId(2)),
      states = listOf(RequisitionState.FULFILLED),
      createdAfter = Instant.ofEpochSecond(12345)
    )

    assertThat(capturedFilter?.clauses).containsExactlyElementsIn(expectedFilter.clauses)
    assertThat(capturedLimit).isEqualTo(10)
  }
}
