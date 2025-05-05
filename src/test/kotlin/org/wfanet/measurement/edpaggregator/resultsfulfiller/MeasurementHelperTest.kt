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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteString
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.slot
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionResponse
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import com.google.common.hash.Hashing
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DeterministicCountDistinct
import org.wfanet.measurement.api.v2alpha.DeterministicDistribution
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.EncryptedMessage
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.SignedMessage
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.testing.TestData
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.testing.verifyProto
import kotlin.reflect.full.declaredFunctions
import kotlin.reflect.jvm.isAccessible
import org.wfanet.measurement.consent.client.dataprovider.encryptResult
import org.wfanet.measurement.consent.client.dataprovider.signResult
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism

@RunWith(JUnit4::class)
class MeasurementHelperTest {

  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { fulfillDirectRequisition(any()) }.thenReturn(fulfillDirectRequisitionResponse {})
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(requisitionsServiceMock) }
  
  private val requisitionsStub: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(grpcTestServerRule.channel)
  }
  
  private val dataProviderSigningKeyHandle: SigningKeyHandle = mockk()
  private val random: Random = mockk()
  
  private lateinit var measurementHelper: MeasurementHelper

  @Before
  fun setUp() {
    measurementHelper = MeasurementHelper(
      requisitionsStub,
      dataProviderSigningKeyHandle,
      random,
      DATA_PROVIDER_CERTIFICATE_KEY
    )
  }

  @Test
  fun `fulfillDirectMeasurement succeeds`() = runBlocking {
    // Use the requisition from the companion object
    val requisition = REQUISITION
    
    val measurementSpec = MEASUREMENT_SPEC
    
    val measurementResult = MeasurementKt.result {
      reach = MeasurementKt.ResultKt.reach {
        value = 100L
        noiseMechanism = NoiseMechanism.CONTINUOUS_GAUSSIAN
        deterministicCountDistinct = DeterministicCountDistinct.getDefaultInstance()
      }
    }
    
    val nonce = Random.Default.nextLong()
    
    // Call the method under test
    measurementHelper.fulfillDirectMeasurement(requisition, measurementSpec, nonce, measurementResult)
    
    // Verify the stub was called with the correct parameters
    verifyProtoArgument(requisitionsStub, RequisitionsCoroutineStub::fulfillDirectRequisition)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        fulfillDirectRequisitionRequest {
          name = REQUISITION_NAME
          this.nonce = nonce
          certificate = DATA_PROVIDER_NAME + "/certificates/AAAAAAAAAcg"
        }
      )
  }
  
  
  companion object {
    private const val DATA_PROVIDER_ID = "someDataProvider"
    private const val DATA_PROVIDER_NAME = "dataProviders/$DATA_PROVIDER_ID"
    private const val MEASUREMENT_CONSUMER_ID = "mc"
    private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
    private const val REQUISITION_NAME = "$DATA_PROVIDER_NAME/requisitions/foo"
    
    private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-12
    }
    
    private val NOISE_MECHANISM = ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN
    
    private val DATA_PROVIDER_CERTIFICATE_KEY = 
      DataProviderCertificateKey(DATA_PROVIDER_ID, externalIdToApiId(8L))
    
    private val MEASUREMENT_SPEC = measurementSpec {
      measurementPublicKey = com.google.protobuf.Any.pack(
        EncryptionPublicKey.newBuilder()
          .setData("test-public-key".toByteString())
          .build()
      )
      reachAndFrequency = MeasurementSpec.ReachAndFrequency.newBuilder()
        .setMaximumFrequency(10)
        .setReachPrivacyParams(OUTPUT_DP_PARAMS)
        .setFrequencyPrivacyParams(OUTPUT_DP_PARAMS)
        .build()
      vidSamplingInterval = com.google.type.Interval.newBuilder()
        .setStartTime(com.google.protobuf.Timestamp.getDefaultInstance())
        .setEndTime(com.google.protobuf.Timestamp.getDefaultInstance())
        .build()
      nonceHashes += Hashing.sha256().hashLong(12345L).asBytes().toByteString()
    }
    
    private val REQUISITION = requisition {
      name = REQUISITION_NAME
      measurement = "$MEASUREMENT_CONSUMER_NAME/measurements/BBBBBBBBBHs"
      state = Requisition.State.UNFULFILLED
      measurementConsumerCertificate = "$MEASUREMENT_CONSUMER_NAME/certificates/AAAAAAAAAcg"
      dataProviderCertificate = "$DATA_PROVIDER_NAME/certificates/AAAAAAAAAcg"
      protocolConfig = protocolConfig {
        protocols += ProtocolConfigKt.protocol {
          direct = ProtocolConfigKt.direct {
            noiseMechanisms += NOISE_MECHANISM
            deterministicCountDistinct = ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
            deterministicDistribution = ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
          }
        }
      }
    }
  }
}
