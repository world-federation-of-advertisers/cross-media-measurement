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

package org.wfanet.measurement.kingdom.service.internal

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.check
import com.nhaarman.mockitokotlin2.eq
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.stub
import com.nhaarman.mockitokotlin2.verify
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest
import org.wfanet.measurement.kingdom.db.KingdomRelationalDatabase
import org.wfanet.measurement.kingdom.db.streamRequisitionsFilter
import org.wfanet.measurement.kingdom.db.to

private val REQUISITION: Requisition = Requisition.newBuilder().apply {
  externalDataProviderId = 1
  externalCampaignId = 2
  externalRequisitionId = 3
  createTimeBuilder.seconds = 456
  state = RequisitionState.UNFULFILLED
}.build()
private val FULFILLED_REQUISITION: Requisition = REQUISITION.toBuilder().apply {
  state = RequisitionState.FULFILLED
}.build()

@RunWith(JUnit4::class)
class RequisitionsServiceTest {
  private val kingdomRelationalDatabase: KingdomRelationalDatabase = mock() {
    onBlocking { createRequisition(any()) }.thenReturn(REQUISITION)
    on { streamRequisitions(any(), any()) }.thenReturn(flowOf(REQUISITION, REQUISITION))
  }

  private val service = RequisitionsService(kingdomRelationalDatabase)

  @Test
  fun `createRequisition fails with id`() = runBlocking<Unit> {
    val requisition: Requisition = Requisition.newBuilder().apply {
      externalDataProviderId = 1
      externalCampaignId = 2
      externalRequisitionId = 3 // <-- should be unset
      state = RequisitionState.UNFULFILLED
    }.build()

    assertFails { service.createRequisition(requisition) }
  }

  @Test
  fun `createRequisition fails with wrong state`() = runBlocking<Unit> {
    val requisition: Requisition = Requisition.newBuilder().apply {
      externalDataProviderId = 1
      externalCampaignId = 2
      state = RequisitionState.FULFILLED
    }.build()

    assertFails { service.createRequisition(requisition) }
  }

  @Test
  fun `createRequisition success`() = runBlocking<Unit> {
    val inputRequisition: Requisition = Requisition.newBuilder().apply {
      externalDataProviderId = 1
      externalCampaignId = 2
      state = RequisitionState.UNFULFILLED
    }.build()

    assertThat(service.createRequisition(inputRequisition))
      .isEqualTo(REQUISITION)

    verify(kingdomRelationalDatabase)
      .createRequisition(inputRequisition)
  }

  @Test
  fun fulfillRequisition() = runBlocking<Unit> {
    kingdomRelationalDatabase.stub {
      onBlocking {
        fulfillRequisition(any(), any())
      }.thenReturn(REQUISITION to FULFILLED_REQUISITION)
    }

    val request: FulfillRequisitionRequest =
      FulfillRequisitionRequest.newBuilder()
        .setExternalRequisitionId(12345)
        .setDuchyId("some-duchy")
        .build()

    assertThat(service.fulfillRequisition(request))
      .isEqualTo(FULFILLED_REQUISITION)

    verify(kingdomRelationalDatabase)
      .fulfillRequisition(ExternalId(12345), "some-duchy")
  }

  @Test
  fun `fulfillRequisition fails with FAILED_PRECONDITION with incorrect state`() = runBlocking {
    kingdomRelationalDatabase.stub {
      onBlocking {
        fulfillRequisition(any(), any())
      }.thenReturn(FULFILLED_REQUISITION to FULFILLED_REQUISITION)
    }

    val exception = assertFailsWith<StatusRuntimeException> {
      service.fulfillRequisition(
        FulfillRequisitionRequest.newBuilder()
          .setExternalRequisitionId(12345)
          .setDuchyId("some-duchy")
          .build()
      )
    }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
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

    assertThat(service.streamRequisitions(request).toList())
      .containsExactly(REQUISITION, REQUISITION)

    val expectedFilter = streamRequisitionsFilter(
      externalDataProviderIds = listOf(ExternalId(1), ExternalId(2)),
      states = listOf(RequisitionState.FULFILLED),
      createdAfter = Instant.ofEpochSecond(12345)
    )

    verify(kingdomRelationalDatabase)
      .streamRequisitions(
        check { assertThat(it.clauses).containsExactlyElementsIn(expectedFilter.clauses) },
        eq(10)
      )
  }
}
