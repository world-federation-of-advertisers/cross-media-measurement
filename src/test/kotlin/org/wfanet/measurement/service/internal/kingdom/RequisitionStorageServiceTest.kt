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

package org.wfanet.measurement.service.internal.kingdom

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Instant
import kotlin.test.assertFails
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
import org.wfanet.measurement.db.kingdom.testing.FakeKingdomRelationalDatabase
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest
import org.wfanet.measurement.service.testing.GrpcTestServerRule

@RunWith(JUnit4::class)
class RequisitionStorageServiceTest {

  companion object {
    val REQUISITION: Requisition = Requisition.newBuilder().apply {
      externalDataProviderId = 1
      externalCampaignId = 2
      externalRequisitionId = 3
      createTimeBuilder.seconds = 456
      state = RequisitionState.UNFULFILLED
    }.build()
  }

  val fakeKingdomRelationalDatabase = FakeKingdomRelationalDatabase()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    listOf(RequisitionStorageService(fakeKingdomRelationalDatabase))
  }

  private val stub: RequisitionStorageGrpcKt.RequisitionStorageCoroutineStub by lazy {
    RequisitionStorageGrpcKt.RequisitionStorageCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun `createRequisition fails with id`() = runBlocking<Unit> {
    val requisition: Requisition = Requisition.newBuilder().apply {
      externalDataProviderId = 1
      externalCampaignId = 2
      externalRequisitionId = 3 // <-- should be unset
      state = RequisitionState.UNFULFILLED
    }.build()

    assertFails { stub.createRequisition(requisition) }
  }

  @Test
  fun `createRequisition fails with wrong state`() = runBlocking<Unit> {
    val requisition: Requisition = Requisition.newBuilder().apply {
      externalDataProviderId = 1
      externalCampaignId = 2
      state = RequisitionState.FULFILLED
    }.build()

    assertFails { stub.createRequisition(requisition) }
  }

  @Test
  fun `createRequisition success`() = runBlocking<Unit> {
    val inputRequisition: Requisition = Requisition.newBuilder().apply {
      externalDataProviderId = 1
      externalCampaignId = 2
      state = RequisitionState.UNFULFILLED
    }.build()

    var capturedRequisition: Requisition? = null
    fakeKingdomRelationalDatabase.writeNewRequisitionFn = {
      capturedRequisition = it
      REQUISITION
    }

    assertThat(stub.createRequisition(inputRequisition))
      .isEqualTo(REQUISITION)

    assertThat(capturedRequisition)
      .isEqualTo(inputRequisition)
  }

  @Test
  fun fulfillRequisition() = runBlocking<Unit> {
    val request: FulfillRequisitionRequest =
      FulfillRequisitionRequest.newBuilder()
        .setExternalRequisitionId(12345)
        .setDuchyId("some-duchy")
        .build()

    var capturedExternalRequisitionId: ExternalId? = null
    var capturedDuchyId: String? = null
    fakeKingdomRelationalDatabase.fulfillRequisitionFn = { externalRequisitionId, duchyId ->
      capturedExternalRequisitionId = externalRequisitionId
      capturedDuchyId = duchyId
      REQUISITION
    }

    assertThat(stub.fulfillRequisition(request))
      .isEqualTo(REQUISITION)

    assertThat(capturedExternalRequisitionId)
      .isEqualTo(ExternalId(12345))

    assertThat(capturedDuchyId)
      .isEqualTo("some-duchy")
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
    var capturedLimit: Long? = null

    fakeKingdomRelationalDatabase.streamRequisitionsFn = { filter, limit ->
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
