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

import com.google.common.truth.Truth
import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlinx.coroutines.runBlocking
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.UseConstructor
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.ComputationParticipant as InternalComputationParticipant
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineImplBase as InternalComputationParticipantsCoroutineService
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as InternalComputationParticipantsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ConfirmComputationParticipantRequest as InternalConfirmComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.FailComputationParticipantRequest as InternalFailComputationParticipantRequest
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.SetParticipantRequisitionParamsRequest as InternalSetParticipantRequisitionParamsRequest
import org.wfanet.measurement.system.v1alpha.ConfirmComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.FailComputationParticipantRequest
import org.wfanet.measurement.system.v1alpha.SetParticipantRequisitionParamsRequest

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
private val SYSTEM_COMPUTATION_PARTICIPATE_NAME =
  "computations/$EXTERNAL_COMPUTATION_ID_STRING/participants/$DUCHY_ID"

private val DUCHY_ELGAMAL_KEY = ByteString.copyFromUtf8("an elgamal key.")
private val DUCHY_ELGAMAL_KEY_SIGNATURE = ByteString.copyFromUtf8("an elgamal key signature.")

private val INTERNAL_COMPUTATION_PARTICIPANT =
  InternalComputationParticipant.newBuilder()
    .apply {
      externalDuchyId = DUCHY_ID
      externalDuchyCertificateId = EXTERNAL_DUCHY_CERTIFICATE_ID
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

@RunWith(JUnit4::class)
class ComputationParticipantsServiceTest {
  @get:Rule val duchyIdSetter = DuchyIdSetter(DUCHY_ID)

  private val duchyIdProvider = { DuchyIdentity(DUCHY_ID) }

  private val internalComputationParticipantsServiceMock:
    InternalComputationParticipantsCoroutineService =
    mock(useConstructor = UseConstructor.parameterless())

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
      .thenReturn(INTERNAL_COMPUTATION_PARTICIPANT)

    val request =
      SetParticipantRequisitionParamsRequest.newBuilder()
        .apply {
          name = SYSTEM_COMPUTATION_PARTICIPATE_NAME
          requisitionParamsBuilder.apply {
            duchyCertificate = DUCHY_CERTIFICATE_PUBLIC_API_NAME
            liquidLegionsV2Builder.apply {
              elGamalPublicKey = DUCHY_ELGAMAL_KEY
              elGamalPublicKeySignature = DUCHY_ELGAMAL_KEY_SIGNATURE
            }
          }
        }
        .build()

    service.setParticipantRequisitionParams(request)

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
            liquidLegionsV2Builder.apply {
              elGamalPublicKey = DUCHY_ELGAMAL_KEY
              elGamalPublicKeySignature = DUCHY_ELGAMAL_KEY_SIGNATURE
            }
          }
          .build()
      )
  }

  @Test
  fun `FailComputationParticipant successfully`() = runBlocking {
    whenever(internalComputationParticipantsServiceMock.failComputationParticipant(any()))
      .thenReturn(INTERNAL_COMPUTATION_PARTICIPANT)

    val request =
      FailComputationParticipantRequest.newBuilder()
        .apply {
          name = SYSTEM_COMPUTATION_PARTICIPATE_NAME
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

    service.failComputationParticipant(request)

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
            errorDetailsBuilder.apply {
              type = MeasurementLogEntry.ErrorDetails.Type.PERMANENT
              errorTimeBuilder.apply {
                seconds = 1001
                nanos = 2002
              }
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
          .build()
      )
  }

  @Test
  fun `ConfirmComputationParticipantRequest successfully`() = runBlocking {
    whenever(internalComputationParticipantsServiceMock.confirmComputationParticipant(any()))
      .thenReturn(INTERNAL_COMPUTATION_PARTICIPANT)

    val request =
      ConfirmComputationParticipantRequest.newBuilder()
        .apply { name = SYSTEM_COMPUTATION_PARTICIPATE_NAME }
        .build()

    service.confirmComputationParticipant(request)

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
    Truth.assertThat(e.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    Truth.assertThat(e.localizedMessage).contains("Resource name unspecified or invalid.")
  }

  @Test
  fun `missing protocol should throw`() {
    val e =
      Assert.assertThrows(StatusRuntimeException::class.java) {
        runBlocking {
          service.setParticipantRequisitionParams(
            SetParticipantRequisitionParamsRequest.newBuilder()
              .apply {
                name = SYSTEM_COMPUTATION_PARTICIPATE_NAME
                requisitionParamsBuilder.apply {
                  duchyCertificate = DUCHY_CERTIFICATE_PUBLIC_API_NAME
                }
              }
              .build()
          )
        }
      }
    Truth.assertThat(e.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    Truth.assertThat(e.localizedMessage).contains("protocol not set in the requisition_params")
  }
}
