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
import com.google.protobuf.timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlinx.coroutines.runBlocking
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.CertificateKt as InternalCertificateKt
import org.wfanet.measurement.internal.kingdom.ComputationParticipant as InternalComputationParticipant
import org.wfanet.measurement.internal.kingdom.ComputationParticipantKt as InternalComputationParticipantKt
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase as InternalComputationParticipantsCoroutineService
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as InternalComputationParticipantsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ConfirmComputationParticipantRequest as InternalConfirmComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntryKt
import org.wfanet.measurement.internal.kingdom.FailComputationParticipantRequest as InternalFailComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntryKt
import org.wfanet.measurement.internal.kingdom.SetParticipantRequisitionParamsRequest as InternalSetParticipantRequisitionParamsRequest
import org.wfanet.measurement.internal.kingdom.certificate as internalCertificate
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.duchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.measurementLogEntry
import org.wfanet.measurement.system.v1alpha.ComputationParticipant
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt.RequisitionParamsKt.liquidLegionsV2
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKt.requisitionParams
import org.wfanet.measurement.system.v1alpha.ConfirmComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.FailComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.SetParticipantRequisitionParamsRequest
import org.wfanet.measurement.system.v1alpha.computationParticipant

private const val DUCHY_ID: String = "some-duchy-id"
private const val MILL_ID: String = "some-mill-id"
private const val STAGE_ATTEMPT_STAGE = 9
private const val STAGE_ATTEMPT_STAGE_NAME = "a stage"
private const val STAGE_ATTEMPT_ATTEMPT_NUMBER = 1L
private const val DUCHY_ERROR_MESSAGE = "something is wrong."
private const val PUBLIC_API_VERSION = "v2alpha"
private const val EXTERNAL_COMPUTATION_ID = 1L
private const val EXTERNAL_DUCHY_CERTIFICATE_ID = 4L

private val EXTERNAL_COMPUTATION_ID_STRING = externalIdToApiId(EXTERNAL_COMPUTATION_ID)
private val EXTERNAL_DUCHY_CERTIFICATE_ID_STRING = externalIdToApiId(EXTERNAL_DUCHY_CERTIFICATE_ID)
private val DUCHY_CERTIFICATE_PUBLIC_API_NAME =
  "duchies/$DUCHY_ID/certificates/$EXTERNAL_DUCHY_CERTIFICATE_ID_STRING"
private val SYSTEM_COMPUTATION_PARTICIPANT_NAME =
  "computations/$EXTERNAL_COMPUTATION_ID_STRING/participants/$DUCHY_ID"

private val DUCHY_CERTIFICATE_DER = ByteString.copyFromUtf8("an X.509 certificate")
private val DUCHY_ELGAMAL_KEY = ByteString.copyFromUtf8("an elgamal key.")
private val DUCHY_ELGAMAL_KEY_SIGNATURE = ByteString.copyFromUtf8("an elgamal key signature.")

private val INTERNAL_COMPUTATION_PARTICIPANT =
  InternalComputationParticipant.newBuilder()
    .apply {
      externalDuchyId = DUCHY_ID
      externalComputationId = EXTERNAL_COMPUTATION_ID
      state = InternalComputationParticipant.State.CREATED
      updateTimeBuilder.apply {
        seconds = 123
        nanos = 456
      }
      apiVersion = PUBLIC_API_VERSION
    }
    .build()

private val INTERNAL_COMPUTATION_PARTICIPANT_WITH_PARAMS =
  INTERNAL_COMPUTATION_PARTICIPANT.copy {
    state = InternalComputationParticipant.State.REQUISITION_PARAMS_SET
    details =
      InternalComputationParticipantKt.details {
        liquidLegionsV2 =
          InternalComputationParticipantKt.liquidLegionsV2Details {
            elGamalPublicKey = DUCHY_ELGAMAL_KEY
            elGamalPublicKeySignature = DUCHY_ELGAMAL_KEY_SIGNATURE
          }
      }
    duchyCertificate = internalCertificate {
      externalDuchyId = DUCHY_ID
      externalCertificateId = EXTERNAL_DUCHY_CERTIFICATE_ID
      details = InternalCertificateKt.details { x509Der = DUCHY_CERTIFICATE_DER }
    }
  }

private val INTERNAL_COMPUTATION_PARTICIPANT_WITH_FAILURE =
  INTERNAL_COMPUTATION_PARTICIPANT.copy {
    state = InternalComputationParticipant.State.FAILED
    failureLogEntry = duchyMeasurementLogEntry {
      externalDuchyId = DUCHY_ID
      logEntry = measurementLogEntry {
        details =
          MeasurementLogEntryKt.details {
            logMessage = DUCHY_ERROR_MESSAGE
            error =
              MeasurementLogEntryKt.errorDetails {
                type = MeasurementLogEntry.ErrorDetails.Type.PERMANENT
                errorTime = timestamp {
                  seconds = 1001
                  nanos = 2002
                }
              }
          }
      }
      details =
        DuchyMeasurementLogEntryKt.details {
          duchyChildReferenceId = MILL_ID
          stageAttempt =
            DuchyMeasurementLogEntryKt.stageAttempt {
              stage = STAGE_ATTEMPT_STAGE
              stageName = STAGE_ATTEMPT_STAGE_NAME
              attemptNumber = STAGE_ATTEMPT_ATTEMPT_NUMBER
              stageStartTime = timestamp {
                seconds = 100
                nanos = 200
              }
            }
        }
    }
  }

@RunWith(JUnit4::class)
class ComputationParticipantsServiceTest {
  @get:Rule val duchyIdSetter = DuchyIdSetter(DUCHY_ID)

  private val duchyIdProvider = { DuchyIdentity(DUCHY_ID) }

  private val internalComputationParticipantsServiceMock:
    InternalComputationParticipantsCoroutineService =
    mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(internalComputationParticipantsServiceMock)
  }

  private val service =
    ComputationParticipantsService(
      InternalComputationParticipantsCoroutineStub(grpcTestServerRule.channel),
      duchyIdProvider
    )

  @Test
  fun `SetParticipantRequisitionParams successfully`() = runBlocking {
    whenever(internalComputationParticipantsServiceMock.setParticipantRequisitionParams(any()))
      .thenReturn(INTERNAL_COMPUTATION_PARTICIPANT_WITH_PARAMS)

    val request =
      SetParticipantRequisitionParamsRequest.newBuilder()
        .apply {
          name = SYSTEM_COMPUTATION_PARTICIPANT_NAME
          requisitionParamsBuilder.apply {
            duchyCertificate = DUCHY_CERTIFICATE_PUBLIC_API_NAME
            liquidLegionsV2Builder.apply {
              elGamalPublicKey = DUCHY_ELGAMAL_KEY
              elGamalPublicKeySignature = DUCHY_ELGAMAL_KEY_SIGNATURE
            }
          }
        }
        .build()
    val response: ComputationParticipant = service.setParticipantRequisitionParams(request)

    assertThat(response)
      .isEqualTo(
        computationParticipant {
          name = SYSTEM_COMPUTATION_PARTICIPANT_NAME
          state = ComputationParticipant.State.REQUISITION_PARAMS_SET
          updateTime = INTERNAL_COMPUTATION_PARTICIPANT.updateTime
          requisitionParams = requisitionParams {
            duchyCertificate = DUCHY_CERTIFICATE_PUBLIC_API_NAME
            duchyCertificateDer = DUCHY_CERTIFICATE_DER
            liquidLegionsV2 = liquidLegionsV2 {
              elGamalPublicKey = DUCHY_ELGAMAL_KEY
              elGamalPublicKeySignature = DUCHY_ELGAMAL_KEY_SIGNATURE
            }
          }
        }
      )
    verifyProtoArgument(
        internalComputationParticipantsServiceMock,
        InternalComputationParticipantsCoroutineService::setParticipantRequisitionParams
      )
      .isEqualTo(
        InternalSetParticipantRequisitionParamsRequest.newBuilder()
          .apply {
            externalComputationId = EXTERNAL_COMPUTATION_ID
            externalDuchyId = DUCHY_ID
            externalDuchyCertificateId = EXTERNAL_DUCHY_CERTIFICATE_ID
            liquidLegionsV2 = INTERNAL_COMPUTATION_PARTICIPANT_WITH_PARAMS.details.liquidLegionsV2
          }
          .build()
      )
  }

  @Test
  fun `FailComputationParticipant successfully`() = runBlocking {
    whenever(internalComputationParticipantsServiceMock.failComputationParticipant(any()))
      .thenReturn(INTERNAL_COMPUTATION_PARTICIPANT_WITH_FAILURE)
    val failureLogEntry = INTERNAL_COMPUTATION_PARTICIPANT_WITH_FAILURE.failureLogEntry

    val request =
      FailComputationParticipantRequest.newBuilder()
        .apply {
          name = SYSTEM_COMPUTATION_PARTICIPANT_NAME
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
        .build()
    val response: ComputationParticipant = service.failComputationParticipant(request)

    assertThat(response.state).isEqualTo(ComputationParticipant.State.FAILED)
    assertThat(response.failure).isEqualTo(request.failure)
    verifyProtoArgument(
        internalComputationParticipantsServiceMock,
        InternalComputationParticipantsCoroutineService::failComputationParticipant
      )
      .isEqualTo(
        InternalFailComputationParticipantRequest.newBuilder()
          .apply {
            externalComputationId = EXTERNAL_COMPUTATION_ID
            externalDuchyId = DUCHY_ID
            errorMessage = DUCHY_ERROR_MESSAGE
            duchyChildReferenceId = MILL_ID
            errorDetails = failureLogEntry.logEntry.details.error
            stageAttempt = failureLogEntry.details.stageAttempt
          }
          .build()
      )
  }

  @Test
  fun `ConfirmComputationParticipant successfully`() = runBlocking {
    whenever(internalComputationParticipantsServiceMock.confirmComputationParticipant(any()))
      .thenReturn(
        INTERNAL_COMPUTATION_PARTICIPANT_WITH_PARAMS.copy {
          state = InternalComputationParticipant.State.READY
        }
      )

    val request =
      ConfirmComputationParticipantRequest.newBuilder()
        .apply { name = SYSTEM_COMPUTATION_PARTICIPANT_NAME }
        .build()
    val response: ComputationParticipant = service.confirmComputationParticipant(request)

    assertThat(response.state).isEqualTo(ComputationParticipant.State.READY)
    verifyProtoArgument(
        internalComputationParticipantsServiceMock,
        InternalComputationParticipantsCoroutineService::confirmComputationParticipant
      )
      .isEqualTo(
        InternalConfirmComputationParticipantRequest.newBuilder()
          .apply {
            externalComputationId = EXTERNAL_COMPUTATION_ID
            externalDuchyId = DUCHY_ID
          }
          .build()
      )
  }

  @Test
  fun `missing resource name should throw`() {
    val e =
      Assert.assertThrows(StatusRuntimeException::class.java) {
        runBlocking {
          service.failComputationParticipant(FailComputationParticipantRequest.getDefaultInstance())
        }
      }
    assertThat(e.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(e.localizedMessage).contains("Resource name unspecified or invalid.")
  }

  @Test
  fun `missing protocol should throw`() {
    val e =
      Assert.assertThrows(StatusRuntimeException::class.java) {
        runBlocking {
          service.setParticipantRequisitionParams(
            SetParticipantRequisitionParamsRequest.newBuilder()
              .apply {
                name = SYSTEM_COMPUTATION_PARTICIPANT_NAME
                requisitionParamsBuilder.apply {
                  duchyCertificate = DUCHY_CERTIFICATE_PUBLIC_API_NAME
                }
              }
              .build()
          )
        }
      }
    assertThat(e.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(e.localizedMessage).contains("protocol not set in the requisition_params")
  }
}
