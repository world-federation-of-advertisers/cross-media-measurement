// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.system.v1alpha

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.nhaarman.mockitokotlin2.UseConstructor
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase as RequisitionsCoroutineService
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.system.v1alpha.FulfillMetricRequisitionRequest
import org.wfanet.measurement.system.v1alpha.FulfillMetricRequisitionResponse
import org.wfanet.measurement.system.v1alpha.MetricRequisitionKey

private const val DUCHY_ID: String = "some-duchy-id"

@RunWith(JUnit4::class)
class RequisitionServiceTest {
  @get:Rule
  val duchyIdSetter = DuchyIdSetter(DUCHY_ID)

  private val duchyIdProvider = { DuchyIdentity(DUCHY_ID) }

  private val requisitionsServiceMock: RequisitionsCoroutineService =
    mock(useConstructor = UseConstructor.parameterless())

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(requisitionsServiceMock) }

  private val service =
    RequisitionService.forTesting(
      RequisitionsCoroutineStub(grpcTestServerRule.channel),
      duchyIdProvider
    )

  @Test
  fun `fulfillMetricRequisition delegates to Requisitions service`() = runBlocking {
    val requisition = Requisition.newBuilder().apply {
      externalDataProviderId = 123L
      externalCampaignId = 456L
      externalRequisitionId = 789L
    }.build()
    val metricRequisitionKey = MetricRequisitionKey.newBuilder().apply {
      dataProviderId = ExternalId(requisition.externalDataProviderId).apiId.value
      campaignId = ExternalId(requisition.externalCampaignId).apiId.value
      metricRequisitionId = ExternalId(requisition.externalRequisitionId).apiId.value
    }.build()
    whenever(requisitionsServiceMock.fulfillRequisition(any())).thenReturn(requisition)

    val request = FulfillMetricRequisitionRequest.newBuilder().apply {
      key = metricRequisitionKey
    }.build()
    val result = service.fulfillMetricRequisition(request)

    assertThat(result).isEqualTo(FulfillMetricRequisitionResponse.getDefaultInstance())
    verifyProtoArgument(requisitionsServiceMock, RequisitionsCoroutineService::fulfillRequisition)
      .isEqualTo(
        FulfillRequisitionRequest.newBuilder().apply {
          externalRequisitionId = requisition.externalRequisitionId
          duchyId = DUCHY_ID
        }.build()
      )
  }
}
