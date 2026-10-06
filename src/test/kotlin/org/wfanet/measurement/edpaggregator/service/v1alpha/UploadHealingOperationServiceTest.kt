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
import org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.approveUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listUploadHealingOperationsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.retryUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.ApproveUploadHealingOperationRequest as InternalApproveRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction as InternalRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.RetryUploadHealingOperationRequest as InternalRetryRequest
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation as InternalOperation
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperationServiceGrpcKt as InternalServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.UploadHealingStep as InternalStep
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.uploadHealingOperation as internalOperation
import org.wfanet.measurement.internal.edpaggregator.uploadHealingStep as internalStep

@RunWith(JUnit4::class)
class UploadHealingOperationServiceTest {
  private val internalService:
    InternalServiceGrpcKt.UploadHealingOperationServiceCoroutineImplBase =
    mockService()

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalService) }

  @Test
  fun `get exposes every operation state and associated candidates`() = runBlocking {
    val expectedStates =
      mapOf(
        InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED to
          UploadHealingOperation.State.APPROVAL_REQUIRED,
        InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVED to
          UploadHealingOperation.State.APPROVED,
        InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_DRAINING to
          UploadHealingOperation.State.DRAINING,
        InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING to
          UploadHealingOperation.State.EVICTING,
        InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_REPLAYING to
          UploadHealingOperation.State.REPLAYING,
        InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_RECOVERING to
          UploadHealingOperation.State.RECOVERING,
        InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION to
          UploadHealingOperation.State.NEEDS_ATTENTION,
        InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_COMPLETE to
          UploadHealingOperation.State.COMPLETE,
      )
    org.mockito.kotlin
      .whenever(internalService.getUploadHealingOperation(org.mockito.kotlin.any()))
      .thenAnswer { invocation ->
        val state =
          invocation
            .getArgument<
              org.wfanet.measurement.internal.edpaggregator.GetUploadHealingOperationRequest
            >(
              0
            )
            .uploadHealingOperationId
        INTERNAL_OPERATION.copy {
          this.state = InternalOperation.State.valueOf(state)
          if (
            this.state == InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
          ) {
            resumeState = InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_RECOVERING
          }
          rawImpressionUploadCorrectionCandidateIds += CANDIDATE_ID
        }
      }
    val service = newService()

    for ((internalState, publicState) in expectedStates) {
      val operation =
        service.getUploadHealingOperation(
          getUploadHealingOperationRequest {
            name = "$DATA_PROVIDER/uploadHealingOperations/${internalState.name}"
          }
        )

      assertThat(operation.state).isEqualTo(publicState)
      assertThat(operation.rawImpressionUploadCorrectionCandidatesList)
        .containsExactly("$DATA_PROVIDER/rawImpressionUploadCorrectionCandidates/$CANDIDATE_ID")
      if (publicState == UploadHealingOperation.State.NEEDS_ATTENTION) {
        assertThat(operation.resumeState).isEqualTo(UploadHealingOperation.State.RECOVERING)
      }
    }
  }

  @Test
  fun `list forwards filters and page tokens`() = runBlocking {
    val captured =
      mutableListOf<
        org.wfanet.measurement.internal.edpaggregator.ListUploadHealingOperationsRequest
      >()
    val nextPageToken =
      org.wfanet.measurement.internal.edpaggregator.listUploadHealingOperationsPageToken {
        after =
          org.wfanet.measurement.internal.edpaggregator.ListUploadHealingOperationsPageTokenKt
            .after {
              createTime = timestamp { seconds = 1L }
              uploadHealingOperationId = OPERATION_ID
            }
      }
    org.mockito.kotlin
      .whenever(internalService.listUploadHealingOperations(org.mockito.kotlin.any()))
      .thenAnswer { invocation ->
        captured +=
          invocation.getArgument<
            org.wfanet.measurement.internal.edpaggregator.ListUploadHealingOperationsRequest
          >(
            0
          )
        org.wfanet.measurement.internal.edpaggregator.listUploadHealingOperationsResponse {
          uploadHealingOperations += INTERNAL_OPERATION
          this.nextPageToken = nextPageToken
        }
      }
    val service = newService()

    val first =
      service.listUploadHealingOperations(
        listUploadHealingOperationsRequest {
          parent = DATA_PROVIDER
          pageSize = 10
          filter =
            ListUploadHealingOperationsRequestKt.filter {
              stateIn += UploadHealingOperation.State.APPROVAL_REQUIRED
            }
        }
      )
    service.listUploadHealingOperations(
      listUploadHealingOperationsRequest {
        parent = DATA_PROVIDER
        pageSize = 10
        pageToken = first.nextPageToken
        filter =
          ListUploadHealingOperationsRequestKt.filter {
            stateIn += UploadHealingOperation.State.APPROVAL_REQUIRED
          }
      }
    )

    assertThat(first.uploadHealingOperationsList.map { it.name })
      .containsExactly("$DATA_PROVIDER/uploadHealingOperations/$OPERATION_ID")
    assertThat(captured[0].filter.stateInList)
      .containsExactly(InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED)
    assertThat(captured[1].pageToken).isEqualTo(nextPageToken)
  }

  @Test
  fun `approve forwards decision etag and request ID`() = runBlocking {
    var captured: InternalApproveRequest? = null
    org.mockito.kotlin
      .whenever(internalService.approveUploadHealingOperation(org.mockito.kotlin.any()))
      .thenAnswer { invocation ->
        captured = invocation.getArgument(0)
        INTERNAL_OPERATION.copy {
          state = InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVED
        }
      }

    val operation =
      newService()
        .approveUploadHealingOperation(
          approveUploadHealingOperationRequest {
            name = "$DATA_PROVIDER/uploadHealingOperations/$OPERATION_ID"
            candidateDecisions +=
              approvalDecision(
                RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE
              )
            etag = "etag"
            requestId = REQUEST_ID
          }
        )

    assertThat(captured!!.dataProviderResourceId).isEqualTo("dp")
    assertThat(captured!!.uploadHealingOperationId).isEqualTo(OPERATION_ID)
    assertThat(captured!!.candidateDecisionsList.single().decision)
      .isEqualTo(
        org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate
          .Decision
          .DECISION_APPLY_CANDIDATE
      )
    assertThat(captured!!.etag).isEqualTo("etag")
    assertThat(captured!!.requestId).isEqualTo(REQUEST_ID)
    assertThat(operation.state).isEqualTo(UploadHealingOperation.State.APPROVED)
  }

  @Test
  fun `approve rejects non-version-4 request ID`() = runBlocking {
    val error =
      assertFailsWith<StatusRuntimeException> {
        newService()
          .approveUploadHealingOperation(
            approveUploadHealingOperationRequest {
              name = "$DATA_PROVIDER/uploadHealingOperations/$OPERATION_ID"
              candidateDecisions +=
                approvalDecision(
                  RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE
                )
              etag = "etag"
              requestId = "11111111-1111-1111-8111-111111111111"
            }
          )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `approve rejects non-RFC-4122 request ID`() = runBlocking {
    val error =
      assertFailsWith<StatusRuntimeException> {
        newService()
          .approveUploadHealingOperation(
            approveUploadHealingOperationRequest {
              name = "$DATA_PROVIDER/uploadHealingOperations/$OPERATION_ID"
              candidateDecisions +=
                approvalDecision(
                  RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE
                )
              etag = "etag"
              requestId = "11111111-1111-4111-0111-111111111111"
            }
          )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `approve rejects noncanonical request ID`() = runBlocking {
    val error =
      assertFailsWith<StatusRuntimeException> {
        newService()
          .approveUploadHealingOperation(
            approveUploadHealingOperationRequest {
              name = "$DATA_PROVIDER/uploadHealingOperations/$OPERATION_ID"
              candidateDecisions +=
                approvalDecision(
                  RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE
                )
              etag = "etag"
              requestId = "1-1-4111-8111-1"
            }
          )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `retry forwards etag and request ID`() = runBlocking {
    var captured: InternalRetryRequest? = null
    org.mockito.kotlin
      .whenever(internalService.retryUploadHealingOperation(org.mockito.kotlin.any()))
      .thenAnswer { invocation ->
        captured = invocation.getArgument(0)
        INTERNAL_OPERATION
      }

    newService()
      .retryUploadHealingOperation(
        retryUploadHealingOperationRequest {
          name = "$DATA_PROVIDER/uploadHealingOperations/$OPERATION_ID"
          etag = "etag"
          requestId = REQUEST_ID
        }
      )

    assertThat(captured!!.dataProviderResourceId).isEqualTo("dp")
    assertThat(captured!!.uploadHealingOperationId).isEqualTo(OPERATION_ID)
    assertThat(captured!!.etag).isEqualTo("etag")
    assertThat(captured!!.requestId).isEqualTo(REQUEST_ID)
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

  private fun newService() =
    UploadHealingOperationService(
      InternalServiceGrpcKt.UploadHealingOperationServiceCoroutineStub(grpcTestServerRule.channel)
    )

  private fun approvalDecision(
    decision: RawImpressionUploadCorrectionCandidate.Decision,
    candidateName: String = "$DATA_PROVIDER/rawImpressionUploadCorrectionCandidates/$CANDIDATE_ID",
  ) =
    org.wfanet.measurement.edpaggregator.v1alpha.ApproveUploadHealingOperationRequestKt
      .candidateDecision {
        rawImpressionUploadCorrectionCandidate = candidateName
        this.decision = decision
      }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp"
    private const val UPLOAD = "$DATA_PROVIDER/rawImpressionUploads/upload"
    private const val PREDECESSOR = "$DATA_PROVIDER/rawImpressionUploads/previous"
    private const val MODEL_LINE_ROW = "$UPLOAD/rawImpressionUploadModelLines/row"
    private const val CMMS_MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml"
    private const val OPERATION_ID = "11111111-1111-4111-8111-111111111111"
    private const val REQUEST_ID = "22222222-2222-4222-8222-222222222222"
    private const val CANDIDATE_ID = "33333333-3333-4333-8333-333333333333"
    private val INTERNAL_OPERATION: InternalOperation = internalOperation {
      dataProviderResourceId = "dp"
      uploadHealingOperationId = OPERATION_ID
      state = InternalOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING
      reason = "bad data"
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
