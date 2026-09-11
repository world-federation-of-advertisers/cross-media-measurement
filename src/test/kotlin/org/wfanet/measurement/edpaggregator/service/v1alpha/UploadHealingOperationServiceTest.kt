// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.advanceUploadHealingStepRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingStep
import org.wfanet.measurement.internal.edpaggregator.AdvanceUploadHealingStepRequest as InternalAdvanceRequest
import org.wfanet.measurement.internal.edpaggregator.CreateUploadHealingOperationRequest as InternalCreateRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction as InternalRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation as InternalOperation
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperationServiceGrpcKt as InternalServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.UploadHealingStep as InternalStep
import org.wfanet.measurement.internal.edpaggregator.uploadHealingOperation as internalOperation
import org.wfanet.measurement.internal.edpaggregator.uploadHealingStep as internalStep

@RunWith(JUnit4::class)
class UploadHealingOperationServiceTest {
  private val internalService:
    InternalServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase =
    mockService()

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalService) }

  @Test
  fun `create forwards the complete healing graph`() = runBlocking {
    var captured: InternalCreateRequest? = null
    org.mockito.kotlin
      .whenever(internalService.createUploadHealingOperation(org.mockito.kotlin.any()))
      .thenAnswer { invocation ->
        captured = invocation.getArgument(0)
        INTERNAL_OPERATION
      }
    val service =
      UploadHealingOperationService(
        InternalServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(grpcTestServerRule.channel)
      )

    val result =
      service.createUploadHealingOperation(
        createUploadHealingOperationRequest {
          parent = DATA_PROVIDER
          uploadHealingOperationId = OPERATION_ID
          requestId = REQUEST_ID
          uploadHealingOperation = uploadHealingOperation {
            reason = "bad data"
            labeledImpressionsBlobPrefix = "gs://output/vid"
            badRawImpressionUploads += UPLOAD
            cutoffTime = timestamp { seconds = 100L }
            steps += uploadHealingStep {
              sequenceNumber = 0L
              sourceRawImpressionUpload = UPLOAD
              rawImpressionUploadModelLine = MODEL_LINE_ROW
              cmmsModelLine = CMMS_MODEL_LINE
              memoized = true
              recoveryAction =
                RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
              recoveryPredecessorRawImpressionUpload = PREDECESSOR
              recoveryTarget = true
            }
          }
        }
      )

    assertThat(captured!!.uploadHealingOperation.stepsList.single().recoveryAction)
      .isEqualTo(
        InternalRecoveryAction.RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY
      )
    assertThat(
        captured!!.uploadHealingOperation.stepsList.single().sourceRawImpressionUploadResourceId
      )
      .isEqualTo("upload")
    assertThat(result.name).isEqualTo("$DATA_PROVIDER/uploadHealingOperations/$OPERATION_ID")
    assertThat(result.stepsList.single().rawImpressionUploadModelLine).isEqualTo(MODEL_LINE_ROW)
    Unit
  }

  @Test
  fun `advance forwards a server-verified action instead of writable state`() = runBlocking {
    var captured: InternalAdvanceRequest? = null
    org.mockito.kotlin
      .whenever(internalService.advanceUploadHealingStep(org.mockito.kotlin.any()))
      .thenAnswer { invocation ->
        captured = invocation.getArgument(0)
        INTERNAL_OPERATION.stepsList.single()
      }
    val service = newService()

    val result =
      service.advanceUploadHealingStep(
        advanceUploadHealingStepRequest {
          name = "$DATA_PROVIDER/uploadHealingOperations/$OPERATION_ID/steps/1"
          etag = "etag"
          action = AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY
          recoveryDoneBlobGeneration = 123L
          requestId = REQUEST_ID
        }
      )

    assertThat(captured!!.action).isEqualTo(InternalAdvanceRequest.Action.RECORD_RECOVERY)
    assertThat(captured!!.uploadHealingStepId).isEqualTo(1L)
    assertThat(captured!!.recoveryDoneBlobGeneration).isEqualTo(123L)
    assertThat(result.state)
      .isEqualTo(
        org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep.State.PENDING_EVICTION
      )
    Unit
  }

  @Test
  fun `create requires request ID`() = runBlocking {
    val error =
      assertFailsWith<StatusRuntimeException> {
        newService().createUploadHealingOperation(validCreateRequest(requestId = ""))
      }

    assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `advance requires request ID`() = runBlocking {
    val error =
      assertFailsWith<StatusRuntimeException> {
        newService()
          .advanceUploadHealingStep(
            advanceUploadHealingStepRequest {
              name = "$DATA_PROVIDER/uploadHealingOperations/$OPERATION_ID/steps/1"
              etag = "etag"
              action = AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION
            }
          )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `create translates internal errors`() = runBlocking {
    org.mockito.kotlin
      .whenever(internalService.createUploadHealingOperation(org.mockito.kotlin.any()))
      .thenThrow(Status.ALREADY_EXISTS.withDescription("internal details").asRuntimeException())
    val error =
      assertFailsWith<StatusRuntimeException> {
        newService().createUploadHealingOperation(validCreateRequest())
      }

    assertThat(error.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(error.status.description).doesNotContain("internal details")
  }

  @Test
  fun `get translates internal errors`() = runBlocking {
    org.mockito.kotlin
      .whenever(internalService.getUploadHealingOperation(org.mockito.kotlin.any()))
      .thenThrow(Status.NOT_FOUND.withDescription("internal details").asRuntimeException())
    val error =
      assertFailsWith<StatusRuntimeException> {
        newService()
          .getUploadHealingOperation(
            getUploadHealingOperationRequest {
              name = "$DATA_PROVIDER/uploadHealingOperations/$OPERATION_ID"
            }
          )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(error.status.description).doesNotContain("internal details")
  }

  @Test
  fun `advance translates internal errors`() = runBlocking {
    org.mockito.kotlin
      .whenever(internalService.advanceUploadHealingStep(org.mockito.kotlin.any()))
      .thenThrow(
        Status.FAILED_PRECONDITION.withDescription("internal details").asRuntimeException()
      )
    val error =
      assertFailsWith<StatusRuntimeException> {
        newService()
          .advanceUploadHealingStep(
            advanceUploadHealingStepRequest {
              name = "$DATA_PROVIDER/uploadHealingOperations/$OPERATION_ID/steps/1"
              etag = "etag"
              action = AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION
              requestId = REQUEST_ID
            }
          )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(error.status.description).doesNotContain("internal details")
  }

  private fun newService() =
    UploadHealingOperationService(
      InternalServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(grpcTestServerRule.channel)
    )

  private fun validCreateRequest(requestId: String = REQUEST_ID) =
    createUploadHealingOperationRequest {
      parent = DATA_PROVIDER
      uploadHealingOperationId = OPERATION_ID
      this.requestId = requestId
      uploadHealingOperation = uploadHealingOperation {
        reason = "bad data"
        labeledImpressionsBlobPrefix = "gs://output/vid"
        badRawImpressionUploads += UPLOAD
        cutoffTime = timestamp { seconds = 100L }
        steps += uploadHealingStep {
          sequenceNumber = 0L
          sourceRawImpressionUpload = UPLOAD
          rawImpressionUploadModelLine = MODEL_LINE_ROW
          cmmsModelLine = CMMS_MODEL_LINE
          recoveryAction =
            RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION
        }
      }
    }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp"
    private const val UPLOAD = "$DATA_PROVIDER/rawImpressionUploads/upload"
    private const val PREDECESSOR = "$DATA_PROVIDER/rawImpressionUploads/previous"
    private const val MODEL_LINE_ROW = "$UPLOAD/rawImpressionUploadModelLines/row"
    private const val CMMS_MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml"
    private const val OPERATION_ID = "11111111-1111-4111-8111-111111111111"
    private const val REQUEST_ID = "22222222-2222-4222-8222-222222222222"
    private val INTERNAL_OPERATION: InternalOperation = internalOperation {
      dataProviderResourceId = "dp"
      uploadHealingOperationId = OPERATION_ID
      state = InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_IN_PROGRESS
      reason = "bad data"
      labeledImpressionsBlobPrefix = "gs://output/vid"
      badRawImpressionUploadResourceIds += "upload"
      cutoffTime = timestamp { seconds = 100L }
      steps += internalStep {
        uploadHealingStepId = 1L
        sequenceNumber = 0L
        sourceRawImpressionUploadResourceId = "upload"
        rawImpressionUploadModelLineResourceId = "row"
        cmmsModelLine = CMMS_MODEL_LINE
        memoized = true
        recoveryAction =
          InternalRecoveryAction.RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY
        recoveryPredecessorRawImpressionUploadResourceId = "previous"
        recoveryTarget = true
        state = InternalStep.State.UPLOAD_HEALING_STEP_STATE_PENDING_EVICTION
        etag = "etag"
      }
      etag = "operation-etag"
    }
  }
}
