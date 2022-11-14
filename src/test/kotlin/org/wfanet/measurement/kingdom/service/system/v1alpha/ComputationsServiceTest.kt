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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.inOrder
import org.mockito.kotlin.times
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.HexString
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.ComputationParticipant as InternalComputationParticipant
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfigKt
import org.wfanet.measurement.internal.kingdom.DuchyProtocolConfigKt.LiquidLegionsV2Kt.mpcNoise
import org.wfanet.measurement.internal.kingdom.GetMeasurementByComputationIdRequest
import org.wfanet.measurement.internal.kingdom.Measurement as InternalMeasurement
import org.wfanet.measurement.internal.kingdom.MeasurementKt.details
import org.wfanet.measurement.internal.kingdom.MeasurementKt.resultInfo
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase as InternalMeasurementsCoroutineService
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ProtocolConfigKt
import org.wfanet.measurement.internal.kingdom.Requisition as InternalRequisition
import org.wfanet.measurement.internal.kingdom.SetMeasurementResultRequest
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.differentialPrivacyParams
import org.wfanet.measurement.internal.kingdom.duchyProtocolConfig
import org.wfanet.measurement.internal.kingdom.liquidLegionsSketchParams
import org.wfanet.measurement.internal.kingdom.measurement as internalMeasurement
import org.wfanet.measurement.internal.kingdom.protocolConfig
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest
import org.wfanet.measurement.system.v1alpha.Computation
import org.wfanet.measurement.system.v1alpha.ComputationKey
import org.wfanet.measurement.system.v1alpha.ComputationParticipant
import org.wfanet.measurement.system.v1alpha.GetComputationRequest
import org.wfanet.measurement.system.v1alpha.Requisition
import org.wfanet.measurement.system.v1alpha.SetComputationResultRequest
import org.wfanet.measurement.system.v1alpha.StreamActiveComputationsRequest
import org.wfanet.measurement.system.v1alpha.StreamActiveComputationsResponse

private const val DUCHY_ID: String = "some-duchy-id"
private const val MILL_ID: String = "some-mill-id"
private const val STAGE_ATTEMPT_STAGE = 9
private const val STAGE_ATTEMPT_STAGE_NAME = "a stage"
private const val STAGE_ATTEMPT_ATTEMPT_NUMBER = 1L
private const val DUCHY_ERROR_MESSAGE = "something is wrong."

private const val PUBLIC_API_VERSION = "v2alpha"

private const val EXTERNAL_COMPUTATION_ID = 1L
private const val EXTERNAL_REQUISITION_ID = 2L
private const val EXTERNAL_DATA_PROVIDER_ID = 3L
private const val EXTERNAL_DUCHY_CERTIFICATE_ID = 4L
private const val NONCE = -7452112597811743614 // Hex: 9894C7134537B482
/** SHA-256 hash of [NONCE] */
private val NONCE_HASH =
  HexString("A4EA9C2984AE1D0F7D0B026B0BB41C136FC0767E29DF40951CFE019B7D9F1CE1")
private val EXTERNAL_COMPUTATION_ID_STRING = externalIdToApiId(EXTERNAL_COMPUTATION_ID)
private val EXTERNAL_REQUISITION_ID_STRING = externalIdToApiId(EXTERNAL_REQUISITION_ID)
private val EXTERNAL_DUCHY_CERTIFICATE_ID_STRING = externalIdToApiId(EXTERNAL_DUCHY_CERTIFICATE_ID)
private val DUCHY_CERTIFICATE_PUBLIC_API_NAME =
  "duchies/$DUCHY_ID/certificates/$EXTERNAL_DUCHY_CERTIFICATE_ID_STRING"
private val SYSTEM_COMPUTATION_NAME = "computations/$EXTERNAL_COMPUTATION_ID_STRING"
private val SYSTEM_COMPUTATION_PARTICIPATE_NAME =
  "computations/$EXTERNAL_COMPUTATION_ID_STRING/participants/$DUCHY_ID"
private val SYSTEM_REQUISITION_NAME =
  "computations/$EXTERNAL_COMPUTATION_ID_STRING/requisitions/$EXTERNAL_REQUISITION_ID_STRING"
private val ENCRYPTED_REQUISITION_SPEC = ByteString.copyFromUtf8("foo")
/** The hash of the above ENCRYPTED_REQUISITION_SPEC. */
private val ENCRYPTED_REQUISITION_SPEC_HASH =
  HexString("2C26B46B68FFC68FF99B453C1D30413413422D706483BFA0F98A5E886266E7AE")

private val MEASUREMENT_SPEC = ByteString.copyFromUtf8("a measurement spec.")
private val DUCHY_ELGAMAL_KEY = ByteString.copyFromUtf8("an elgamal key.")
private val DUCHY_ELGAMAL_KEY_SIGNATURE = ByteString.copyFromUtf8("an elgamal key signature.")
private val RESULT_PUBLIC_KEY = ByteString.copyFromUtf8("result public key.")
private val ENCRYPTED_RESULT = ByteString.copyFromUtf8("encrypted result.")

private val INTERNAL_REQUISITION =
  InternalRequisition.newBuilder()
    .apply {
      externalComputationId = EXTERNAL_COMPUTATION_ID
      externalRequisitionId = EXTERNAL_REQUISITION_ID
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
      externalFulfillingDuchyId = DUCHY_ID
      state = InternalRequisition.State.FULFILLED
      detailsBuilder.apply {
        encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
        nonceHash = NONCE_HASH.bytes
        nonce = NONCE
      }
    }
    .build()

private val INTERNAL_COMPUTATION_PARTICIPANT =
  InternalComputationParticipant.newBuilder()
    .apply {
      externalDuchyId = DUCHY_ID
      externalComputationId = EXTERNAL_COMPUTATION_ID
      state = InternalComputationParticipant.State.FAILED
      updateTimeBuilder.apply {
        seconds = 123
        nanos = 456
      }
      detailsBuilder.apply {
        liquidLegionsV2Builder.apply {
          elGamalPublicKey = DUCHY_ELGAMAL_KEY
          elGamalPublicKeySignature = DUCHY_ELGAMAL_KEY_SIGNATURE
        }
      }
      apiVersion = PUBLIC_API_VERSION
      duchyCertificateBuilder.apply { externalCertificateId = EXTERNAL_DUCHY_CERTIFICATE_ID }
      failureLogEntryBuilder.apply {
        externalDuchyId = DUCHY_ID
        detailsBuilder.apply {
          duchyChildReferenceId = MILL_ID
          stageAttemptBuilder.apply {
            stage = STAGE_ATTEMPT_STAGE
            stageName = STAGE_ATTEMPT_STAGE_NAME
            attemptNumber = STAGE_ATTEMPT_ATTEMPT_NUMBER
            stageStartTimeBuilder.apply {
              seconds = 100
              nanos = 200
            }
          }
          logEntryBuilder.apply {
            detailsBuilder.apply {
              logMessage = DUCHY_ERROR_MESSAGE
              errorBuilder.errorTimeBuilder.apply {
                seconds = 1001
                nanos = 2002
              }
            }
          }
        }
      }
    }
    .build()

private val INTERNAL_MEASUREMENT = internalMeasurement {
  externalComputationId = EXTERNAL_COMPUTATION_ID
  state = InternalMeasurement.State.FAILED
  details = details {
    apiVersion = PUBLIC_API_VERSION
    measurementSpec = MEASUREMENT_SPEC
    duchyProtocolConfig = duchyProtocolConfig {
      liquidLegionsV2 =
        DuchyProtocolConfigKt.liquidLegionsV2 {
          mpcNoise = mpcNoise {
            blindedHistogramNoise = differentialPrivacyParams {
              epsilon = 1.1
              delta = 2.1
            }
            noiseForPublisherNoise = differentialPrivacyParams {
              epsilon = 3.1
              delta = 4.1
            }
          }
        }
    }
    protocolConfig = protocolConfig {
      liquidLegionsV2 =
        ProtocolConfigKt.liquidLegionsV2 {
          sketchParams = liquidLegionsSketchParams {
            decayRate = 10.0
            maxSize = 100
            samplingIndicatorSize = 1000
          }
          ellipticCurveId = 123
          maximumFrequency = 12
        }
    }
  }
  results += resultInfo {
    externalAggregatorDuchyId = DUCHY_ID
    externalCertificateId = EXTERNAL_DUCHY_CERTIFICATE_ID
    encryptedResult = ENCRYPTED_RESULT
  }
  computationParticipants += INTERNAL_COMPUTATION_PARTICIPANT
  requisitions += INTERNAL_REQUISITION
}

@RunWith(JUnit4::class)
class ComputationsServiceTest {
  @get:Rule val duchyIdSetter = DuchyIdSetter(DUCHY_ID)

  private val duchyIdProvider = { DuchyIdentity(DUCHY_ID) }

  private val internalMeasurementsServiceMock: InternalMeasurementsCoroutineService = mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(internalMeasurementsServiceMock) }

  private val service =
    ComputationsService(
      InternalMeasurementsCoroutineStub(grpcTestServerRule.channel),
      duchyIdProvider
    )

  @Test
  fun `get computation successfully`() = runBlocking {
    whenever(internalMeasurementsServiceMock.getMeasurementByComputationId(any()))
      .thenReturn(INTERNAL_MEASUREMENT)

    val request =
      GetComputationRequest.newBuilder().apply { name = SYSTEM_COMPUTATION_NAME }.build()

    val response = service.getComputation(request)

    assertThat(response)
      .isEqualTo(
        Computation.newBuilder()
          .apply {
            name = SYSTEM_COMPUTATION_NAME
            publicApiVersion = PUBLIC_API_VERSION
            measurementSpec = MEASUREMENT_SPEC
            state = Computation.State.FAILED
            aggregatorCertificate = DUCHY_CERTIFICATE_PUBLIC_API_NAME
            encryptedResult = ENCRYPTED_RESULT
            mpcProtocolConfigBuilder.liquidLegionsV2Builder.apply {
              sketchParamsBuilder.apply {
                decayRate = 10.0
                maxSize = 100
              }
              maximumFrequency = 12
              mpcNoiseBuilder.apply {
                blindedHistogramNoiseBuilder.apply {
                  epsilon = 1.1
                  delta = 2.1
                }
                noiseForPublisherNoiseBuilder.apply {
                  epsilon = 3.1
                  delta = 4.1
                }
              }
              ellipticCurveId = 123
              maximumFrequency = 12
            }
            addRequisitions(
              Requisition.newBuilder().apply {
                name = SYSTEM_REQUISITION_NAME
                state = Requisition.State.FULFILLED
                requisitionSpecHash = ENCRYPTED_REQUISITION_SPEC_HASH.bytes
                nonceHash = NONCE_HASH.bytes
                fulfillingComputationParticipant = SYSTEM_COMPUTATION_PARTICIPATE_NAME
                nonce = NONCE
              }
            )
            addComputationParticipants(
              ComputationParticipant.newBuilder().apply {
                name = SYSTEM_COMPUTATION_PARTICIPATE_NAME
                state = ComputationParticipant.State.FAILED
                updateTimeBuilder.apply {
                  seconds = 123
                  nanos = 456
                }
                requisitionParamsBuilder.apply {
                  duchyCertificate = DUCHY_CERTIFICATE_PUBLIC_API_NAME
                  liquidLegionsV2Builder.apply {
                    elGamalPublicKey = DUCHY_ELGAMAL_KEY
                    elGamalPublicKeySignature = DUCHY_ELGAMAL_KEY_SIGNATURE
                  }
                }
                failureBuilder.apply {
                  participantChildReferenceId = MILL_ID
                  errorMessage = DUCHY_ERROR_MESSAGE
                  errorTimeBuilder.apply {
                    seconds = 1001
                    nanos = 2002
                  }
                  stageAttemptBuilder.apply {
                    stage = STAGE_ATTEMPT_STAGE
                    stageName = STAGE_ATTEMPT_STAGE_NAME
                    attemptNumber = STAGE_ATTEMPT_ATTEMPT_NUMBER
                    stageStartTimeBuilder.apply {
                      seconds = 100
                      nanos = 200
                    }
                  }
                }
              }
            )
          }
          .build()
      )
    verifyProtoArgument(
        internalMeasurementsServiceMock,
        InternalMeasurementsCoroutineService::getMeasurementByComputationId
      )
      .isEqualTo(
        GetMeasurementByComputationIdRequest.newBuilder()
          .apply { externalComputationId = EXTERNAL_COMPUTATION_ID }
          .build()
      )
  }

  @Test
  fun `stream active computations successfully`() = runBlocking {
    var calls = 0L
    fun nextMeasurement() =
      INTERNAL_MEASUREMENT.toBuilder()
        .apply {
          externalComputationId = 100 + calls
          updateTimeBuilder.seconds = 1000 + calls
          ++calls
        }
        .build()

    fun expectedResponse(id: Long) =
      StreamActiveComputationsResponse.newBuilder()
        .apply { computationBuilder.name = ComputationKey(externalIdToApiId(id)).toName() }
        .build()

    whenever(internalMeasurementsServiceMock.streamMeasurements(any())).thenAnswer {
      flowOf(nextMeasurement(), nextMeasurement())
    }

    val flow =
      service.streamActiveComputations(StreamActiveComputationsRequest.getDefaultInstance())

    assertThat(flow.take(5).toList())
      .comparingExpectedFieldsOnly()
      .containsExactly(
        expectedResponse(100),
        expectedResponse(101),
        expectedResponse(102),
        expectedResponse(103),
        expectedResponse(104)
      )
      .inOrder()

    fun expectedStreamMeasurementsRequest(
      updatedAfterSeconds: Long,
      lastSeenExternalComputationId: Long
    ): StreamMeasurementsRequest = streamMeasurementsRequest {
      measurementView = InternalMeasurement.View.COMPUTATION
      filter = filter {
        states +=
          listOf(
            InternalMeasurement.State.PENDING_REQUISITION_PARAMS,
            InternalMeasurement.State.PENDING_PARTICIPANT_CONFIRMATION,
            InternalMeasurement.State.PENDING_COMPUTATION,
            InternalMeasurement.State.FAILED,
            InternalMeasurement.State.CANCELLED,
            InternalMeasurement.State.SUCCEEDED
          )
        updatedAfter = timestamp { seconds = updatedAfterSeconds }
        externalComputationIdAfter = lastSeenExternalComputationId
        externalDuchyId = DUCHY_ID
      }
    }

    inOrder(internalMeasurementsServiceMock) {
      argumentCaptor<StreamMeasurementsRequest> {
        verify(internalMeasurementsServiceMock, times(3)).streamMeasurements(capture())
        assertThat(allValues)
          .ignoringRepeatedFieldOrder()
          .containsExactly(
            expectedStreamMeasurementsRequest(0, 0),
            expectedStreamMeasurementsRequest(1001, 101),
            expectedStreamMeasurementsRequest(1003, 103)
          )
          .inOrder()
      }
    }
  }

  @Test
  fun `set computation result successfully`() = runBlocking {
    whenever(internalMeasurementsServiceMock.setMeasurementResult(any()))
      .thenReturn(INTERNAL_MEASUREMENT)

    val request =
      SetComputationResultRequest.newBuilder()
        .apply {
          name = SYSTEM_COMPUTATION_NAME
          aggregatorCertificate = DUCHY_CERTIFICATE_PUBLIC_API_NAME
          resultPublicKey = RESULT_PUBLIC_KEY
          encryptedResult = ENCRYPTED_RESULT
        }
        .build()

    service.setComputationResult(request)

    verifyProtoArgument(
        internalMeasurementsServiceMock,
        InternalMeasurementsCoroutineService::setMeasurementResult
      )
      .isEqualTo(
        SetMeasurementResultRequest.newBuilder()
          .apply {
            externalComputationId = EXTERNAL_COMPUTATION_ID
            externalAggregatorDuchyId = DUCHY_ID
            externalAggregatorCertificateId = EXTERNAL_DUCHY_CERTIFICATE_ID
            resultPublicKey = RESULT_PUBLIC_KEY
            encryptedResult = ENCRYPTED_RESULT
          }
          .build()
      )
  }
}
