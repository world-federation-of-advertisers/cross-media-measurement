// Copyright 2021 The Cross-Media Measurement Authors
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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertThrows
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.HexString
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.CertificateKt as InternalCertificateKt
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequestKt.computedRequisitionParams
import org.wfanet.measurement.internal.kingdom.Requisition as InternalRequisition
import org.wfanet.measurement.internal.kingdom.RequisitionKt as InternalRequisitionKt
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase as InternalRequisitionsCoroutineService
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub as InternalRequisitionsCoroutineStub
import org.wfanet.measurement.internal.kingdom.certificate as internalCertificate
import org.wfanet.measurement.internal.kingdom.fulfillRequisitionRequest as internalFulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.requisition as internalRequisition
import org.wfanet.measurement.system.v1alpha.FulfillRequisitionRequest
import org.wfanet.measurement.system.v1alpha.Requisition

private const val PUBLIC_API_VERSION = "v2alpha"
private const val DUCHY_ID: String = "some-duchy-id"

private const val EXTERNAL_COMPUTATION_ID = 123L
private const val EXTERNAL_REQUISITION_ID = 456L
private const val EXTERNAL_DATA_PROVIDER_ID = 789L
private const val EXTERNAL_DATA_PROVIDER_CERTIFICATE_ID = 321L
private const val NONCE = -7452112597811743614 // Hex: 9894C7134537B482
/** SHA-256 hash of [NONCE] */
private val NONCE_HASH =
  HexString("A4EA9C2984AE1D0F7D0B026B0BB41C136FC0767E29DF40951CFE019B7D9F1CE1")
private val EXTERNAL_COMPUTATION_ID_STRING = externalIdToApiId(EXTERNAL_COMPUTATION_ID)
private val EXTERNAL_REQUISITION_ID_STRING = externalIdToApiId(EXTERNAL_REQUISITION_ID)
private val SYSTEM_REQUISITION_NAME =
  "computations/$EXTERNAL_COMPUTATION_ID_STRING/requisitions/$EXTERNAL_REQUISITION_ID_STRING"
private val DATA_PROVIDER_CERTIFICATE_DER = ByteString.copyFromUtf8("DataProvider certificate")
private val REQUISITION_SPEC_HASH =
  HexString("2C26B46B68FFC68FF99B453C1D30413413422D706483BFA0F98A5E886266E7AE")

private val INTERNAL_REQUISITION = internalRequisition {
  externalComputationId = EXTERNAL_COMPUTATION_ID
  externalRequisitionId = EXTERNAL_REQUISITION_ID
  externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
  externalFulfillingDuchyId = DUCHY_ID
  state = InternalRequisition.State.FULFILLED
  details =
    InternalRequisitionKt.details {
      encryptedRequisitionSpec = ByteString.copyFromUtf8("foo")
      nonceHash = NONCE_HASH.bytes
      nonce = NONCE
    }
  dataProviderCertificate = internalCertificate {
    externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
    externalCertificateId = EXTERNAL_DATA_PROVIDER_CERTIFICATE_ID
    details = InternalCertificateKt.details { x509Der = DATA_PROVIDER_CERTIFICATE_DER }
  }
  parentMeasurement = InternalRequisitionKt.parentMeasurement { apiVersion = PUBLIC_API_VERSION }
}

@RunWith(JUnit4::class)
class RequisitionsServiceTest {
  @get:Rule val duchyIdSetter = DuchyIdSetter(DUCHY_ID)

  private val duchyIdProvider = { DuchyIdentity(DUCHY_ID) }

  private val internalRequisitionsServiceMock: InternalRequisitionsCoroutineService = mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(internalRequisitionsServiceMock) }

  private val service =
    RequisitionsService(
      InternalRequisitionsCoroutineStub(grpcTestServerRule.channel),
      duchyIdProvider
    )

  @Test
  fun `fulfill requisition successfully`() = runBlocking {
    whenever(internalRequisitionsServiceMock.fulfillRequisition(any()))
      .thenReturn(INTERNAL_REQUISITION)

    val request =
      FulfillRequisitionRequest.newBuilder()
        .apply {
          name = SYSTEM_REQUISITION_NAME
          nonce = NONCE
        }
        .build()

    val response = service.fulfillRequisition(request)

    assertThat(response)
      .isEqualTo(
        Requisition.newBuilder()
          .apply {
            name = SYSTEM_REQUISITION_NAME
            state = Requisition.State.FULFILLED
            requisitionSpecHash = REQUISITION_SPEC_HASH.bytes
            nonceHash = NONCE_HASH.bytes
            fulfillingComputationParticipant =
              "computations/$EXTERNAL_COMPUTATION_ID_STRING/participants/$DUCHY_ID"
            nonce = NONCE
          }
          .build()
      )
    verifyProtoArgument(
        internalRequisitionsServiceMock,
        InternalRequisitionsCoroutineService::fulfillRequisition
      )
      .isEqualTo(
        internalFulfillRequisitionRequest {
          externalRequisitionId = EXTERNAL_REQUISITION_ID
          nonce = NONCE
          computedParams = computedRequisitionParams {
            externalComputationId = EXTERNAL_COMPUTATION_ID
            externalFulfillingDuchyId = DUCHY_ID
          }
        }
      )
  }

  @Test
  fun `resource name missing should throw`() {
    val e =
      assertThrows(StatusRuntimeException::class.java) {
        runBlocking { service.fulfillRequisition(FulfillRequisitionRequest.getDefaultInstance()) }
      }
    assertThat(e.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(e.localizedMessage).contains("Resource name unspecified or invalid.")
  }
}
