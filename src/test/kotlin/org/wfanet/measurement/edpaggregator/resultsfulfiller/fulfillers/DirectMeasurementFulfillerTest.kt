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

import java.nio.file.Path
import java.nio.file.Paths
import java.security.SecureRandom
import kotlin.random.Random
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionResponse
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DeterministicCountDistinct
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.result
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.direct
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.protocol
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism

@RunWith(JUnit4::class)
class DirectMeasurementFulfillerTest {

  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { fulfillDirectRequisition(any()) }.thenReturn(fulfillDirectRequisitionResponse {})
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(requisitionsServiceMock) }

  private val requisitionsStub: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun `fulfillRequisition creates correct proto`() = runBlocking {
    val result = result {
      MeasurementKt.ResultKt.reach { value = 100L }
    }

    val directMeasurementFulfiller = DirectMeasurementFulfiller(
      requisitionName = REQUISITION_NAME,
      dataProviderCertificateName = DATA_PROVIDER_CERTIFICATE_NAME,
      measurementResult = result,
      nonce = Random.Default.nextLong(),
      measurementEncryptionPublicKey = MC_PUBLIC_KEY,
      sampledVids = flow {
        for (i in 1..100) {
          emit(i.toLong())
        }
      },
      directProtocolConfig = DIRECT_PROTOCOL,
      directNoiseMechanism = DirectNoiseMechanism.CONTINUOUS_GAUSSIAN,
      dataProviderSigningKeyHandle = EDP_SIGNING_KEY,
      dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
      requisitionsStub = requisitionsStub
    )

    directMeasurementFulfiller.fulfillRequisition()

    // Verify the stub was called with the correct parameters
    verifyProtoArgument(requisitionsServiceMock, RequisitionsCoroutineImplBase::fulfillDirectRequisition)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        fulfillDirectRequisitionRequest {
          name = REQUISITION_NAME
          nonce = directMeasurementFulfiller.nonce
          certificate = DATA_PROVIDER_CERTIFICATE_NAME
        }
      )
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
    private const val MEASUREMENT_CONSUMER_ID = "mc"
    private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
    private const val REQUISITION_NAME = "$EDP_NAME/requisitions/foo"
    private val MC_SIGNING_KEY = loadSigningKey("${MEASUREMENT_CONSUMER_ID}_cs_cert.der", "${MEASUREMENT_CONSUMER_ID}_cs_private.der")
    private val DATA_PROVIDER_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRET_FILES_PATH.resolve("${EDP_DISPLAY_NAME}_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val MC_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()
    private val REQUISITION_SPEC = requisitionSpec { }
    private val ENCRYPTED_REQUISITION_SPEC =
      encryptRequisitionSpec(
        signRequisitionSpec(REQUISITION_SPEC, MC_SIGNING_KEY),
        DATA_PROVIDER_PUBLIC_KEY,
      )
    private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-12
    }
    private val MEASUREMENT_SPEC = measurementSpec {
      measurementPublicKey = MC_PUBLIC_KEY.pack()
      reachAndFrequency = reachAndFrequency {
        reachPrivacyParams = OUTPUT_DP_PARAMS
        frequencyPrivacyParams = OUTPUT_DP_PARAMS
        maximumFrequency = 10
      }
      vidSamplingInterval = vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }
      nonceHashes += Hashing.hashSha256(REQUISITION_SPEC.nonce)
    }

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
