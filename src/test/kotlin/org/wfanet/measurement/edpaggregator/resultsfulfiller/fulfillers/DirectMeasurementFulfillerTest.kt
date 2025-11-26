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

import io.grpc.Status
import io.grpc.StatusException
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.random.Random
import kotlin.test.assertFails
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.result
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.direct
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionResponse
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism

@RunWith(JUnit4::class)
class DirectMeasurementFulfillerTest {

  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { fulfillDirectRequisition(any()) }.thenReturn(fulfillDirectRequisitionResponse {})
    onBlocking { getRequisition(any()) }
      .thenReturn(requisition { state = Requisition.State.UNFULFILLED })
  }
  private val throwingUnfulfilledRequisitionsServiceMock: RequisitionsCoroutineImplBase =
    mockService {
      onBlocking { fulfillDirectRequisition(any()) }
        .thenAnswer { throw StatusException(Status.INTERNAL) }
      onBlocking { getRequisition(any()) }
        .thenReturn(requisition { state = Requisition.State.UNFULFILLED })
    }
  private val throwingTerminalRequisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { fulfillDirectRequisition(any()) }
      .thenAnswer { throw StatusException(Status.INTERNAL) }
    onBlocking { getRequisition(any()) }
      .thenReturn(requisition { state = Requisition.State.WITHDRAWN })
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(requisitionsServiceMock) }

  @get:Rule
  val throwingUnfulfilledGrpcTestServerRule = GrpcTestServerRule {
    addService(throwingUnfulfilledRequisitionsServiceMock)
  }

  @get:Rule
  val throwingTerminalGrpcTestServerRule = GrpcTestServerRule {
    addService(throwingTerminalRequisitionsServiceMock)
  }

  private val requisitionsStub: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }
  private val throwingUnfulfilledRequisitionsStub: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(throwingUnfulfilledGrpcTestServerRule.channel)
  }
  private val throwingTerminalRequisitionsStub: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(throwingTerminalGrpcTestServerRule.channel)
  }

  @Test
  fun `fulfillRequisition creates correct proto for direct reach requisition fulfillment`() =
    runBlocking {
      val result = result { MeasurementKt.ResultKt.reach { value = 100L } }
      val directMeasurementFulfiller =
        DirectMeasurementFulfiller(
          requisitionName = REQUISITION_NAME,
          requisitionDataProviderCertificateName = DATA_PROVIDER_CERTIFICATE_NAME,
          measurementResult = result,
          requisitionNonce = NONCE,
          measurementEncryptionPublicKey = MC_PUBLIC_KEY,
          directProtocolConfig = DIRECT_PROTOCOL,
          directNoiseMechanism = DirectNoiseMechanism.CONTINUOUS_GAUSSIAN,
          dataProviderSigningKeyHandle = EDP_SIGNING_KEY,
          dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
          requisitionsStub = requisitionsStub,
        )

      directMeasurementFulfiller.fulfillRequisition()

      // Verify the stub was called with the correct parameters
      verifyProtoArgument(
          requisitionsServiceMock,
          RequisitionsCoroutineImplBase::fulfillDirectRequisition,
        )
        .comparingExpectedFieldsOnly()
        .isEqualTo(
          fulfillDirectRequisitionRequest {
            name = REQUISITION_NAME
            nonce = NONCE
            certificate = DATA_PROVIDER_CERTIFICATE_NAME
          }
        )
    }

  @Test
  fun `fulfillRequisition rethrows unfulfilled state requisitions`() {

    val result = result { MeasurementKt.ResultKt.reach { value = 100L } }
    val directMeasurementFulfiller =
      DirectMeasurementFulfiller(
        requisitionName = REQUISITION_NAME,
        requisitionDataProviderCertificateName = DATA_PROVIDER_CERTIFICATE_NAME,
        measurementResult = result,
        requisitionNonce = NONCE,
        measurementEncryptionPublicKey = MC_PUBLIC_KEY,
        directProtocolConfig = DIRECT_PROTOCOL,
        directNoiseMechanism = DirectNoiseMechanism.CONTINUOUS_GAUSSIAN,
        dataProviderSigningKeyHandle = EDP_SIGNING_KEY,
        dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
        requisitionsStub = throwingUnfulfilledRequisitionsStub,
      )
    assertFails { runBlocking { directMeasurementFulfiller.fulfillRequisition() } }
  }

  @Test
  fun `fulfillRequisition does not throw an exception for terminal state requisitions`() =
    runBlocking {
      val result = result { MeasurementKt.ResultKt.reach { value = 100L } }
      val directMeasurementFulfiller =
        DirectMeasurementFulfiller(
          requisitionName = REQUISITION_NAME,
          requisitionDataProviderCertificateName = DATA_PROVIDER_CERTIFICATE_NAME,
          measurementResult = result,
          requisitionNonce = NONCE,
          measurementEncryptionPublicKey = MC_PUBLIC_KEY,
          directProtocolConfig = DIRECT_PROTOCOL,
          directNoiseMechanism = DirectNoiseMechanism.CONTINUOUS_GAUSSIAN,
          dataProviderSigningKeyHandle = EDP_SIGNING_KEY,
          dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
          requisitionsStub = throwingTerminalRequisitionsStub,
        )

      directMeasurementFulfiller.fulfillRequisition()
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
    private const val REQUISITION_NAME = "$EDP_NAME/requisitions/foo"

    private val MC_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val NOISE_MECHANISM = NoiseMechanism.CONTINUOUS_GAUSSIAN
    private val DIRECT_PROTOCOL = direct {
      noiseMechanisms += NOISE_MECHANISM
      deterministicCountDistinct =
        ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
      deterministicDistribution =
        ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
    }
    private val DATA_PROVIDER_CERTIFICATE_NAME = "$EDP_NAME/certificates/AAAAAAAAAAg"
    private val DATA_PROVIDER_CERTIFICATE_KEY =
      DataProviderCertificateKey(EDP_ID, externalIdToApiId(8L))
    private val NONCE = Random.Default.nextLong()

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
