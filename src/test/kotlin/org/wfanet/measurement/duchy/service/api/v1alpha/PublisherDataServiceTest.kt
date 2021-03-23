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

package org.wfanet.measurement.duchy.service.api.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.nhaarman.mockitokotlin2.UseConstructor
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.stub
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verifyBlocking
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v1alpha.DataProviderRegistrationGrpcKt.DataProviderRegistrationCoroutineImplBase as DataProviderRegistrationCoroutineService
import org.wfanet.measurement.api.v1alpha.DataProviderRegistrationGrpcKt.DataProviderRegistrationCoroutineStub
import org.wfanet.measurement.api.v1alpha.GetCombinedPublicKeyRequest
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsRequest
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsResponse
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.RefuseMetricRequisitionRequest
import org.wfanet.measurement.api.v1alpha.RequisitionGrpcKt.RequisitionCoroutineImplBase as RequisitionCoroutineService
import org.wfanet.measurement.api.v1alpha.RequisitionGrpcKt.RequisitionCoroutineStub
import org.wfanet.measurement.api.v1alpha.UploadMetricValueRequest
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.duchy.testing.DUCHY_PUBLIC_KEYS
import org.wfanet.measurement.internal.duchy.MetricValue as InternalMetricValue
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineImplBase as MetricValuesCoroutineService
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineStub
import org.wfanet.measurement.internal.duchy.StoreMetricValueRequest
import org.wfanet.measurement.system.v1alpha.FulfillMetricRequisitionResponse
import org.wfanet.measurement.system.v1alpha.RequisitionGrpcKt.RequisitionCoroutineImplBase as SystemRequisitionCoroutineService
import org.wfanet.measurement.system.v1alpha.RequisitionGrpcKt.RequisitionCoroutineStub as SystemRequisitionCoroutineStub

/** Test for [PublisherDataService]. */
@RunWith(JUnit4::class)
class PublisherDataServiceTest {
  private val metricValuesServiceMock: MetricValuesCoroutineService =
    mock(useConstructor = UseConstructor.parameterless())
  private val requisitionServiceMock: RequisitionCoroutineService =
    mock(useConstructor = UseConstructor.parameterless())
  private val systemRequisitionServiceMock: SystemRequisitionCoroutineService =
    mock(useConstructor = UseConstructor.parameterless())
  private val registrationServiceMock: DataProviderRegistrationCoroutineService =
    mock(useConstructor = UseConstructor.parameterless())

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(metricValuesServiceMock)
    addService(requisitionServiceMock)
    addService(systemRequisitionServiceMock)
    addService(registrationServiceMock)
  }

  private val service: PublisherDataService

  init {
    val channel = grpcTestServerRule.channel

    service = PublisherDataService(
      MetricValuesCoroutineStub(channel),
      RequisitionCoroutineStub(channel),
      SystemRequisitionCoroutineStub(channel),
      DataProviderRegistrationCoroutineStub(channel),
      DUCHY_PUBLIC_KEYS
    )
  }

  @Test fun `listMetricRequisitions delegates to Requisition service`() {
    val metricRequisitionKey = MetricRequisition.Key.newBuilder().apply {
      dataProviderId = "dataProviderId"
      campaignId = "campaign"
      metricRequisitionId = "metricRequisition"
    }.build()
    val expectedResponse = ListMetricRequisitionsResponse.newBuilder().apply {
      addMetricRequisitionsBuilder().key = metricRequisitionKey
    }.build()
    requisitionServiceMock.stub {
      onBlocking { listMetricRequisitions(any()) }.thenReturn(expectedResponse)
    }

    val request = ListMetricRequisitionsRequest.newBuilder().apply {
      parentBuilder.dataProviderId = metricRequisitionKey.dataProviderId
      parentBuilder.campaignId = metricRequisitionKey.campaignId
    }.build()
    val response = runBlocking { service.listMetricRequisitions(request) }

    assertThat(response).isEqualTo(expectedResponse)
    argumentCaptor<ListMetricRequisitionsRequest> {
      verifyBlocking(requisitionServiceMock, times(1)) {
        listMetricRequisitions(capture())
      }
      assertThat(firstValue).isEqualTo(request)
    }
  }

  @Test fun `refuseMetricRequisition delegates to Requisition service`() {
    val metricRequisitionKey = MetricRequisition.Key.newBuilder().apply {
      dataProviderId = "dataProviderId"
      campaignId = "campaign"
      metricRequisitionId = "metricRequisition"
    }.build()
    val expectedResponse = MetricRequisition.newBuilder().apply {
      key = metricRequisitionKey
      state = MetricRequisition.State.PERMANENTLY_UNFILLABLE
    }.build()
    requisitionServiceMock.stub {
      onBlocking { refuseMetricRequisition(any()) }.thenReturn(expectedResponse)
    }

    val request = RefuseMetricRequisitionRequest.newBuilder().apply {
      key = metricRequisitionKey
      refusalBuilder.apply {
        justification = MetricRequisition.Refusal.Justification.DATA_UNAVAILABLE
        message = "Disk corrupted"
      }
    }.build()
    val response = runBlocking { service.refuseMetricRequisition(request) }

    assertThat(response).isEqualTo(expectedResponse)
    argumentCaptor<RefuseMetricRequisitionRequest> {
      verifyBlocking(requisitionServiceMock, times(1)) {
        refuseMetricRequisition(capture())
      }
      assertThat(firstValue).isEqualTo(request)
    }
  }

  @Test fun `uploadMetricValue stores metric and fulfills requisition`() {
    val metricValueKey = MetricRequisition.Key.newBuilder().apply {
      dataProviderId = "dataProviderId"
      campaignId = "campaign"
      metricRequisitionId = "metricRequisition"
    }.build()

    lateinit var storeRequests: List<StoreMetricValueRequest>
    metricValuesServiceMock.stub {
      onBlocking { storeMetricValue(any()) }.thenAnswer {
        val requests: Flow<StoreMetricValueRequest> = it.getArgument(0)
        storeRequests = runBlocking { requests.toList() }
        InternalMetricValue.newBuilder().apply {
          resourceKey = metricValueKey.toResourceKey()
        }.build()
      }
    }
    systemRequisitionServiceMock.stub {
      onBlocking {
        fulfillMetricRequisition(any())
      }.thenReturn(FulfillMetricRequisitionResponse.getDefaultInstance())
    }

    val response = runBlocking {
      service.uploadMetricValue(
        flowOf(
          UploadMetricValueRequest.newBuilder().apply {
            headerBuilder.key = metricValueKey
          }.build(),
          UploadMetricValueRequest.newBuilder().apply {
            chunkBuilder.data = testMetricValueData
          }.build()
        )
      )
    }

    assertThat(response.state).isEqualTo(MetricRequisition.State.FULFILLED)
    assertThat(storeRequests).containsExactly(
      StoreMetricValueRequest.newBuilder().apply {
        headerBuilder.resourceKey = metricValueKey.toResourceKey()
      }.build(),
      StoreMetricValueRequest.newBuilder().apply {
        chunkBuilder.data = testMetricValueData
      }.build()
    ).inOrder()
  }

  @Test fun `getCombinedPublicKey throws NOT_FOUND for unknown ID`() {
    val request = GetCombinedPublicKeyRequest.newBuilder()
      .apply { keyBuilder.combinedPublicKeyId = "unknown-id" }
      .build()

    val exception = assertFailsWith<StatusRuntimeException> {
      runBlocking { service.getCombinedPublicKey(request) }
    }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test fun `getCombinedPublicKey throws INVALID_ARGUMENT for missing ID`() {
    val request = GetCombinedPublicKeyRequest.newBuilder()
      .apply { keyBuilder }
      .build()

    val exception = assertFailsWith<StatusRuntimeException> {
      runBlocking { service.getCombinedPublicKey(request) }
    }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test fun `getCombinedPublicKey returns CombinedPublicKey`() {
    val latestPublicKeys = DUCHY_PUBLIC_KEYS.latest
    val combinedPublicKeyId = latestPublicKeys.combinedPublicKeyId
    val request = GetCombinedPublicKeyRequest.newBuilder()
      .apply { keyBuilder.combinedPublicKeyId = combinedPublicKeyId }
      .build()

    val response = runBlocking { service.getCombinedPublicKey(request) }

    assertThat(response.key).isEqualTo(request.key)
    assertThat(response.version).isEqualTo(latestPublicKeys.combinedPublicKeyVersion)

    val combinedPublicKey = latestPublicKeys.combinedPublicKey
    assertThat(response.encryptionKey.ellipticCurveId).isEqualTo(latestPublicKeys.curveId)
    assertThat(response.encryptionKey.generator).isEqualTo(combinedPublicKey.generator)
    assertThat(response.encryptionKey.element).isEqualTo(combinedPublicKey.element)
  }

  companion object {
    private val random = Random.Default
    private val testMetricValueData = random.nextBytes(1024 * 1024 * 2).toByteString()
  }
}
