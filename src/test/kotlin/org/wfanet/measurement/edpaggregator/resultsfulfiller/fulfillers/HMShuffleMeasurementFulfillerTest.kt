/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.resultsfulfiller.fulfillers

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.wfanet.frequencycount.frequencyVector
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionResponse
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.fulfillRequisitionRequest
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.testing.Requisitions.HMSS_REQUISITION

@RunWith(JUnit4::class)
class HMShuffleMeasurementFulfillerTest {

  private class FakeRequisitionFulfillmentService : RequisitionFulfillmentCoroutineImplBase() {
    data class FulfillRequisitionInvocation(val requests: List<FulfillRequisitionRequest>)

    private val _fullfillRequisitionInvocations = mutableListOf<FulfillRequisitionInvocation>()
    val fullfillRequisitionInvocations: List<FulfillRequisitionInvocation>
      get() = _fullfillRequisitionInvocations

    override suspend fun fulfillRequisition(
      requests: Flow<FulfillRequisitionRequest>
    ): FulfillRequisitionResponse {
      // Consume flow before returning.
      _fullfillRequisitionInvocations.add(FulfillRequisitionInvocation(requests.toList()))
      return FulfillRequisitionResponse.getDefaultInstance()
    }
  }

  private val requisitionFulfillmentMock = FakeRequisitionFulfillmentService()

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(requisitionFulfillmentMock) }

  private val requisitionsStub: RequisitionFulfillmentCoroutineStub by lazy {
    RequisitionFulfillmentCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun `fulfillRequisition calls stub with correct parameters`() = runBlocking {
    val requisitionNonce = Random.Default.nextLong()
    val sampledFrequencyVector = frequencyVector { data += listOf(4, 5, 6) }
    val requisition = HMSS_REQUISITION.copy { this.nonce = requisitionNonce }
    val fulfiller =
      HMShuffleMeasurementFulfiller(
        requisition = requisition,
        requisitionNonce = requisitionNonce,
        sampledFrequencyVector = sampledFrequencyVector,
        dataProviderSigningKeyHandle = EDP_SIGNING_KEY,
        dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
        requisitionFulfillmentStubMap =
          mapOf(
            "duchies/worker2" to RequisitionFulfillmentCoroutineStub(grpcTestServerRule.channel)
          ),
      )
    fulfiller.fulfillRequisition()
    val fulfilledRequisitions =
      requisitionFulfillmentMock.fullfillRequisitionInvocations.single().requests
    assertThat(fulfilledRequisitions).hasSize(2)
    ProtoTruth.assertThat(fulfilledRequisitions[0])
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        fulfillRequisitionRequest {
          header =
            FulfillRequisitionRequestKt.header {
              name = requisition.name
              nonce = requisitionNonce
            }
        }
      )
    assertThat(fulfilledRequisitions[0].header.honestMajorityShareShuffle.dataProviderCertificate)
      .isEqualTo(DATA_PROVIDER_CERTIFICATE_NAME)
    assertThat(fulfilledRequisitions[1].bodyChunk.data).isNotEmpty()
  }

  @Test
  fun `fulfillRequisition throws on gRPC error`() {
    runBlocking {
      val requisitionNonce = Random.Default.nextLong()
      val sampledFrequencyVector = frequencyVector { data += listOf(4, 5, 6) }
      val stubWithError: RequisitionFulfillmentCoroutineStub = mock()
      whenever(stubWithError.fulfillRequisition(any(), any()))
        .thenThrow(StatusRuntimeException(Status.INTERNAL))

      val requisition = HMSS_REQUISITION.copy { this.nonce = requisitionNonce }
      val fulfiller =
        HMShuffleMeasurementFulfiller(
          requisition = requisition,
          requisitionNonce = requisitionNonce,
          sampledFrequencyVector = sampledFrequencyVector,
          dataProviderSigningKeyHandle = EDP_SIGNING_KEY,
          dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
          requisitionFulfillmentStubMap = mapOf("duchies/worker2" to stubWithError),
        )
      assertFailsWith<StatusRuntimeException> { fulfiller.fulfillRequisition() }
    }
  }

  companion object {
    private const val EDP_ID = "someDataProvider"
    private const val EDP_NAME = "dataProviders/$EDP_ID"
    private const val EDP_DISPLAY_NAME = "edp1"
    private val SECRET_FILES_PATH: Path =
      checkNotNull(
        getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )
      )
    private val EDP_SIGNING_KEY =
      loadSigningKey("${EDP_DISPLAY_NAME}_cs_cert.der", "${EDP_DISPLAY_NAME}_cs_private.der")
    private val DATA_PROVIDER_CERTIFICATE_NAME = "$EDP_NAME/certificates/AAAAAAAAAAg"
    private val DATA_PROVIDER_CERTIFICATE_KEY =
      DataProviderCertificateKey(EDP_ID, externalIdToApiId(8L))

    private fun loadSigningKey(
      certDerFileName: String,
      privateKeyDerFileName: String,
    ): SigningKeyHandle {
      return org.wfanet.measurement.common.crypto.testing.loadSigningKey(
        SECRET_FILES_PATH.resolve(certDerFileName).toFile(),
        SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile(),
      )
    }
  }
}
