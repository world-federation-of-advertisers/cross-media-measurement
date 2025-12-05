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
import io.grpc.StatusException
import java.nio.file.Path
import java.nio.file.Paths
import java.security.SecureRandom
import kotlin.random.Random
import kotlin.test.assertFails
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
import org.wfanet.frequencycount.FrequencyVector
import org.wfanet.frequencycount.SecretShareGeneratorRequest
import org.wfanet.frequencycount.frequencyVector
import org.wfanet.frequencycount.secretShare
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionRequestKt
import org.wfanet.measurement.api.v2alpha.FulfillRequisitionResponse
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.fulfillRequisitionRequest
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.computation.KAnonymityParams
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.FrequencyVectorBuilder
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

  private val unfulfilledRequisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { getRequisition(any()) }
      .thenReturn(requisition { state = Requisition.State.UNFULFILLED })
  }
  private val terminalRequisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { getRequisition(any()) }
      .thenReturn(requisition { state = Requisition.State.WITHDRAWN })
  }

  @get:Rule
  val unfulfilledGrpcTestServerRule = GrpcTestServerRule {
    addService(unfulfilledRequisitionsServiceMock)
  }

  @get:Rule
  val terminalGrpcTestServerRule = GrpcTestServerRule {
    addService(terminalRequisitionsServiceMock)
  }

  private val unfulfilledRequisitionsStub: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(unfulfilledGrpcTestServerRule.channel)
  }
  private val terminalRequisitionsStub: RequisitionsCoroutineStub by lazy {
    RequisitionsCoroutineStub(terminalGrpcTestServerRule.channel)
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
        requisitionsStub = unfulfilledRequisitionsStub,
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

      whenever(stubWithError.fulfillRequisition(any(), any())).thenAnswer {
        throw StatusException(Status.INTERNAL)
      }

      val requisition = HMSS_REQUISITION.copy { this.nonce = requisitionNonce }
      val fulfiller =
        HMShuffleMeasurementFulfiller(
          requisition = requisition,
          requisitionNonce = requisitionNonce,
          sampledFrequencyVector = sampledFrequencyVector,
          dataProviderSigningKeyHandle = EDP_SIGNING_KEY,
          dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
          requisitionFulfillmentStubMap = mapOf("duchies/worker2" to stubWithError),
          requisitionsStub = unfulfilledRequisitionsStub,
        )
      assertFails { fulfiller.fulfillRequisition() }
    }
  }

  @Test
  fun `fulfillRequisition does not throw on gRPC error for terminal state requisitions`() {
    runBlocking {
      val requisitionNonce = Random.Default.nextLong()
      val sampledFrequencyVector = frequencyVector { data += listOf(4, 5, 6) }
      val stubWithError: RequisitionFulfillmentCoroutineStub = mock()
      whenever(stubWithError.fulfillRequisition(any(), any())).thenAnswer {
        throw StatusException(Status.INTERNAL)
      }

      val requisition = HMSS_REQUISITION.copy { this.nonce = requisitionNonce }
      val fulfiller =
        HMShuffleMeasurementFulfiller(
          requisition = requisition,
          requisitionNonce = requisitionNonce,
          sampledFrequencyVector = sampledFrequencyVector,
          dataProviderSigningKeyHandle = EDP_SIGNING_KEY,
          dataProviderCertificateKey = DATA_PROVIDER_CERTIFICATE_KEY,
          requisitionFulfillmentStubMap = mapOf("duchies/worker2" to stubWithError),
          requisitionsStub = terminalRequisitionsStub,
        )
      fulfiller.fulfillRequisition()
    }
  }

  @Test
  fun `fulfillRequisition with non-empty frequency vector with sufficient k-anonymity`() {
    runBlocking {
      val requisitionNonce = Random.Default.nextLong()
      val frequencyVectorBuilder =
        FrequencyVectorBuilder(
          measurementSpec = MEASUREMENT_SPEC,
          populationSpec = POPULATION_SPEC,
          overrideImpressionMaxFrequencyPerUser = null,
          strict = false,
        )
      listOf(4, 5, 6).forEach { frequencyVectorBuilder.increment(it) }
      val requisition = HMSS_REQUISITION.copy { this.nonce = requisitionNonce }
      val fulfiller =
        HMShuffleMeasurementFulfiller.buildKAnonymized(
          requisition,
          requisitionNonce,
          MEASUREMENT_SPEC,
          POPULATION_SPEC,
          frequencyVectorBuilder,
          EDP_SIGNING_KEY,
          DATA_PROVIDER_CERTIFICATE_KEY,
          requisitionFulfillmentStubMap =
            mapOf(
              "duchies/worker2" to RequisitionFulfillmentCoroutineStub(grpcTestServerRule.channel)
            ),
          requisitionsStub = unfulfilledRequisitionsStub,
          KAnonymityParams(minImpressions = 1, minUsers = 1),
          maxPopulation = null,
          ::echoFrequencyVector,
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
      val parsedData = FrequencyVector.parseFrom(fulfilledRequisitions[1].bodyChunk.data)
      assertThat(parsedData).isNotEqualTo(EMPTY_FREQUENCY_VECTOR)
    }
  }

  @Test
  fun `fulfillRequisition with empty frequency vector with insufficient k-anonymity`() {
    runBlocking {
      val requisitionNonce = Random.Default.nextLong()
      val frequencyVectorBuilder =
        FrequencyVectorBuilder(
          measurementSpec = MEASUREMENT_SPEC,
          populationSpec = POPULATION_SPEC,
          overrideImpressionMaxFrequencyPerUser = null,
          strict = false,
        )
      listOf(4, 5, 6).forEach { frequencyVectorBuilder.increment(it) }
      val requisition = HMSS_REQUISITION.copy { this.nonce = requisitionNonce }
      val fulfiller =
        HMShuffleMeasurementFulfiller.buildKAnonymized(
          requisition,
          requisitionNonce,
          MEASUREMENT_SPEC,
          POPULATION_SPEC,
          frequencyVectorBuilder,
          EDP_SIGNING_KEY,
          DATA_PROVIDER_CERTIFICATE_KEY,
          requisitionFulfillmentStubMap =
            mapOf(
              "duchies/worker2" to RequisitionFulfillmentCoroutineStub(grpcTestServerRule.channel)
            ),
          requisitionsStub = unfulfilledRequisitionsStub,
          KAnonymityParams(minImpressions = 1000, minUsers = 1000),
          maxPopulation = null,
          ::echoFrequencyVector,
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
      val parsedData = FrequencyVector.parseFrom(fulfilledRequisitions[1].bodyChunk.data)
      assertThat(parsedData).isEqualTo(EMPTY_FREQUENCY_VECTOR)
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
    private val POPULATION_SPEC = populationSpec {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 1
              endVidInclusive = 1000
            }
        }
    }
    private val NONCE = SecureRandom.getInstance("SHA1PRNG").nextLong()
    private val MC_PUBLIC_KEY: EncryptionPublicKey =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()

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
      nonceHashes += Hashing.hashSha256(NONCE)
      modelLine = "some-model-line"
    }

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

  private val EMPTY_FREQUENCY_VECTOR: FrequencyVector =
    FrequencyVectorBuilder(
        measurementSpec = MEASUREMENT_SPEC,
        populationSpec = POPULATION_SPEC,
        overrideImpressionMaxFrequencyPerUser = null,
        strict = false,
      )
      .build()

  private fun echoFrequencyVector(data: ByteArray): ByteArray {
    val request = SecretShareGeneratorRequest.parseFrom(data)
    val data = secretShare { shareVector += request.dataList }
    return data.toByteString().toByteArray()
  }
}
