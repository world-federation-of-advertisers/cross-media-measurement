// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.AdvanceUploadHealingStepRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineFailureReason
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation
import org.wfanet.measurement.internal.edpaggregator.UploadHealingStep
import org.wfanet.measurement.internal.edpaggregator.advanceUploadHealingStepRequest
import org.wfanet.measurement.internal.edpaggregator.createUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.getUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.uploadHealingOperation
import org.wfanet.measurement.internal.edpaggregator.uploadHealingStep

@RunWith(JUnit4::class)
class SpannerUploadHealingOperationServiceTest {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  @Test
  fun `operation and step checkpoints are retry safe`() = runBlocking {
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    insertUploadGraph()
    val createRequest = createRequest(memoized = false)

    val created = service.createUploadHealingOperation(createRequest)
    val replayed = service.createUploadHealingOperation(createRequest)

    assertThat(created.stepsList.single().state)
      .isEqualTo(UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_PENDING_EVICTION)
    assertThat(replayed).isEqualTo(created)

    val waitingRequest = advanceUploadHealingStepRequest {
      dataProviderResourceId = DATA_PROVIDER_ID
      uploadHealingOperationId = OPERATION_ID
      uploadHealingStepId = 1L
      etag = created.stepsList.single().etag
      action = AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION
      requestId = WAITING_REQUEST_ID
    }
    val waiting = service.advanceUploadHealingStep(waitingRequest)
    val waitingReplay = service.advanceUploadHealingStep(waitingRequest)

    assertThat(waiting.state)
      .isEqualTo(UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_WAITING_FOR_REPLACEMENT)
    assertThat(waitingReplay).isEqualTo(waiting)
    assertThat(waiting.hasEvictionCompleteTime()).isTrue()

    val started =
      service.advanceUploadHealingStep(
        advanceUploadHealingStepRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          uploadHealingStepId = 1L
          etag = waiting.etag
          action = AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY
          recoveryDoneBlobGeneration = RECOVERY_GENERATION
          requestId = STARTED_REQUEST_ID
        }
      )

    assertThat(started.recoveryDoneBlobGeneration).isEqualTo(RECOVERY_GENERATION)
    assertThat(started.evictionCompleteTime).isEqualTo(waiting.evictionCompleteTime)

    val completed =
      service.advanceUploadHealingStep(
        advanceUploadHealingStepRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          uploadHealingStepId = 1L
          etag = started.etag
          action = AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT
          replacementRawImpressionUploadResourceId = REPLACEMENT_UPLOAD_ID
          requestId = COMPLETE_REQUEST_ID
        }
      )

    assertThat(completed.state)
      .isEqualTo(UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE)
    assertThat(completed.replacementRawImpressionUploadResourceId).isEqualTo(REPLACEMENT_UPLOAD_ID)
    assertThat(completed.evictionCompleteTime).isEqualTo(waiting.evictionCompleteTime)
    val completedOperation =
      service.getUploadHealingOperation(
        getUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
        }
      )
    assertThat(completedOperation.state)
      .isEqualTo(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_COMPLETE)
    Unit
  }

  @Test
  fun `memoized replacement without an active snapshot is rejected`() = runBlocking {
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    insertUploadGraph()
    val created = service.createUploadHealingOperation(createRequest(memoized = true))
    val waiting =
      service.advanceUploadHealingStep(
        advanceUploadHealingStepRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          uploadHealingStepId = 1L
          etag = created.stepsList.single().etag
          action = AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION
        }
      )
    val started =
      service.advanceUploadHealingStep(
        advanceUploadHealingStepRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          uploadHealingStepId = 1L
          etag = waiting.etag
          action = AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY
          recoveryDoneBlobGeneration = RECOVERY_GENERATION
        }
      )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.advanceUploadHealingStep(
          advanceUploadHealingStepRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            uploadHealingStepId = 1L
            etag = started.etag
            action = AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT
            replacementRawImpressionUploadResourceId = REPLACEMENT_UPLOAD_ID
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    val operation =
      service.getUploadHealingOperation(
        getUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
        }
      )
    assertThat(operation.stepsList.single().state)
      .isEqualTo(UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_RECOVERY_STARTED)
    Unit
  }

  @Test
  fun `recovery cannot start before its predecessor completes`() = runBlocking {
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    insertUploadGraph()
    insertEvictedSource(
      rawImpressionUploadId = 3L,
      uploadResourceId = SUCCESSOR_UPLOAD_ID,
      modelLineId = 3L,
      modelLineResourceId = SUCCESSOR_MODEL_LINE_ROW_ID,
    )
    val created =
      service.createUploadHealingOperation(
        createUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          requestId = CREATE_REQUEST_ID
          uploadHealingOperation = uploadHealingOperation {
            reason = "bad source data"
            labeledImpressionsBlobPrefix = "gs://output/vid"
            badRawImpressionUploadResourceIds += SOURCE_UPLOAD_ID
            cutoffTime = timestamp { seconds = 100L }
            steps += createRequest(memoized = false).uploadHealingOperation.stepsList.single()
            steps += uploadHealingStep {
              uploadHealingStepId = 2L
              sequenceNumber = 1L
              sourceRawImpressionUploadResourceId = SUCCESSOR_UPLOAD_ID
              rawImpressionUploadModelLineResourceId = SUCCESSOR_MODEL_LINE_ROW_ID
              cmmsModelLine = CMMS_MODEL_LINE
              memoized = true
              recoveryAction =
                RawImpressionUploadModelLineRecoveryAction
                  .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY
              recoveryPredecessorRawImpressionUploadResourceId = SOURCE_UPLOAD_ID
              recoveryTarget = true
            }
          }
        }
      )
    val successor = created.stepsList.single { it.uploadHealingStepId == 2L }
    val waiting =
      service.advanceUploadHealingStep(
        advanceUploadHealingStepRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          uploadHealingStepId = 2L
          etag = successor.etag
          action = AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION
        }
      )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.advanceUploadHealingStep(
          advanceUploadHealingStepRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            uploadHealingStepId = 2L
            etag = waiting.etag
            action = AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY
            recoveryDoneBlobGeneration = RECOVERY_GENERATION
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `eviction cannot be confirmed before the source model line is evicted`() = runBlocking {
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    insertUploadGraph()
    val created =
      service.createUploadHealingOperation(
        createUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          requestId = CREATE_REQUEST_ID
          uploadHealingOperation = uploadHealingOperation {
            reason = "bad source data"
            labeledImpressionsBlobPrefix = "gs://output/vid"
            badRawImpressionUploadResourceIds += REPLACEMENT_UPLOAD_ID
            cutoffTime = timestamp { seconds = 100L }
            steps += uploadHealingStep {
              uploadHealingStepId = 1L
              sourceRawImpressionUploadResourceId = REPLACEMENT_UPLOAD_ID
              rawImpressionUploadModelLineResourceId = "replacement-model-line-row"
              cmmsModelLine = CMMS_MODEL_LINE
              recoveryAction =
                RawImpressionUploadModelLineRecoveryAction
                  .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_EDP_CORRECTION
            }
          }
        }
      )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.advanceUploadHealingStep(
          advanceUploadHealingStepRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            uploadHealingStepId = 1L
            etag = created.stepsList.single().etag
            action = AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  private fun createRequest(memoized: Boolean) = createUploadHealingOperationRequest {
    dataProviderResourceId = DATA_PROVIDER_ID
    uploadHealingOperationId = OPERATION_ID
    requestId = CREATE_REQUEST_ID
    uploadHealingOperation = uploadHealingOperation {
      reason = "bad source data"
      labeledImpressionsBlobPrefix = "gs://output/vid"
      badRawImpressionUploadResourceIds += SOURCE_UPLOAD_ID
      cutoffTime = timestamp { seconds = 100L }
      steps += uploadHealingStep {
        uploadHealingStepId = 1L
        sequenceNumber = 0L
        sourceRawImpressionUploadResourceId = SOURCE_UPLOAD_ID
        rawImpressionUploadModelLineResourceId = MODEL_LINE_ROW_ID
        cmmsModelLine = CMMS_MODEL_LINE
        this.memoized = memoized
        recoveryAction =
          RawImpressionUploadModelLineRecoveryAction
            .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY
        recoveryTarget = true
      }
    }
  }

  private suspend fun insertUploadGraph() {
    val sourceUpload =
      Mutation.newInsertBuilder("RawImpressionUpload")
        .set("DataProviderResourceId")
        .to(DATA_PROVIDER_ID)
        .set("RawImpressionUploadId")
        .to(1L)
        .set("RawImpressionUploadResourceId")
        .to(SOURCE_UPLOAD_ID)
        .set("DoneBlobUri")
        .to(DONE_BLOB_URI)
        .set("DoneBlobGeneration")
        .to(1L)
        .set("DoneBlobCreateTime")
        .to(com.google.cloud.Timestamp.ofTimeSecondsAndNanos(1L, 0))
        .set("RegistrationComplete")
        .to(true)
        .set("State")
        .to(Value.protoEnum(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_FAILED))
        .set("CreateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .set("UpdateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .build()
    val sourceModelLine =
      Mutation.newInsertBuilder("RawImpressionUploadModelLine")
        .set("DataProviderResourceId")
        .to(DATA_PROVIDER_ID)
        .set("RawImpressionUploadId")
        .to(1L)
        .set("RawImpressionUploadModelLineId")
        .to(1L)
        .set("RawImpressionUploadModelLineResourceId")
        .to(MODEL_LINE_ROW_ID)
        .set("CmmsModelLine")
        .to(CMMS_MODEL_LINE)
        .set("State")
        .to(
          Value.protoEnum(
            RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED
          )
        )
        .set("FailureReason")
        .to(
          Value.protoEnum(
            RawImpressionUploadModelLineFailureReason
              .RAW_IMPRESSION_UPLOAD_MODEL_LINE_FAILURE_REASON_EVICTED_OUTPUT
          )
        )
        .set("EvictionOperationId")
        .to(OPERATION_ID)
        .set("CreateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .set("UpdateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .build()
    val replacementUpload =
      Mutation.newInsertBuilder("RawImpressionUpload")
        .set("DataProviderResourceId")
        .to(DATA_PROVIDER_ID)
        .set("RawImpressionUploadId")
        .to(2L)
        .set("RawImpressionUploadResourceId")
        .to(REPLACEMENT_UPLOAD_ID)
        .set("DoneBlobUri")
        .to(DONE_BLOB_URI)
        .set("DoneBlobGeneration")
        .to(RECOVERY_GENERATION)
        .set("DoneBlobCreateTime")
        .to(com.google.cloud.Timestamp.ofTimeSecondsAndNanos(2L, 0))
        .set("ReplacesRawImpressionUploadResourceId")
        .to(SOURCE_UPLOAD_ID)
        .set("RegistrationComplete")
        .to(true)
        .set("State")
        .to(Value.protoEnum(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED))
        .set("CreateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .set("UpdateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .build()
    val replacementModelLine =
      Mutation.newInsertBuilder("RawImpressionUploadModelLine")
        .set("DataProviderResourceId")
        .to(DATA_PROVIDER_ID)
        .set("RawImpressionUploadId")
        .to(2L)
        .set("RawImpressionUploadModelLineId")
        .to(2L)
        .set("RawImpressionUploadModelLineResourceId")
        .to("replacement-model-line-row")
        .set("CmmsModelLine")
        .to(CMMS_MODEL_LINE)
        .set("State")
        .to(
          Value.protoEnum(
            RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED
          )
        )
        .set("CreateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .set("UpdateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .build()
    spannerDatabase.databaseClient.write(
      listOf(sourceUpload, sourceModelLine, replacementUpload, replacementModelLine)
    )
  }

  private suspend fun insertEvictedSource(
    rawImpressionUploadId: Long,
    uploadResourceId: String,
    modelLineId: Long,
    modelLineResourceId: String,
  ) {
    val upload =
      Mutation.newInsertBuilder("RawImpressionUpload")
        .set("DataProviderResourceId")
        .to(DATA_PROVIDER_ID)
        .set("RawImpressionUploadId")
        .to(rawImpressionUploadId)
        .set("RawImpressionUploadResourceId")
        .to(uploadResourceId)
        .set("DoneBlobUri")
        .to("gs://input/$uploadResourceId/done")
        .set("DoneBlobGeneration")
        .to(3L)
        .set("DoneBlobCreateTime")
        .to(com.google.cloud.Timestamp.ofTimeSecondsAndNanos(3L, 0))
        .set("RegistrationComplete")
        .to(true)
        .set("State")
        .to(Value.protoEnum(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_FAILED))
        .set("CreateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .set("UpdateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .build()
    val modelLine =
      Mutation.newInsertBuilder("RawImpressionUploadModelLine")
        .set("DataProviderResourceId")
        .to(DATA_PROVIDER_ID)
        .set("RawImpressionUploadId")
        .to(rawImpressionUploadId)
        .set("RawImpressionUploadModelLineId")
        .to(modelLineId)
        .set("RawImpressionUploadModelLineResourceId")
        .to(modelLineResourceId)
        .set("CmmsModelLine")
        .to(CMMS_MODEL_LINE)
        .set("State")
        .to(
          Value.protoEnum(
            RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED
          )
        )
        .set("FailureReason")
        .to(
          Value.protoEnum(
            RawImpressionUploadModelLineFailureReason
              .RAW_IMPRESSION_UPLOAD_MODEL_LINE_FAILURE_REASON_EVICTED_OUTPUT
          )
        )
        .set("EvictionOperationId")
        .to(OPERATION_ID)
        .set("CreateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .set("UpdateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .build()
    spannerDatabase.databaseClient.write(listOf(upload, modelLine))
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private const val DATA_PROVIDER_ID = "data-provider"
    private const val SOURCE_UPLOAD_ID = "raw-impression-upload"
    private const val REPLACEMENT_UPLOAD_ID = "replacement-upload"
    private const val MODEL_LINE_ROW_ID = "model-line-row"
    private const val SUCCESSOR_UPLOAD_ID = "successor-upload"
    private const val SUCCESSOR_MODEL_LINE_ROW_ID = "successor-model-line-row"
    private const val CMMS_MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml"
    private const val DONE_BLOB_URI = "gs://input/day/done"
    private const val OPERATION_ID = "11111111-1111-4111-8111-111111111111"
    private const val CREATE_REQUEST_ID = "22222222-2222-4222-8222-222222222222"
    private const val WAITING_REQUEST_ID = "33333333-3333-4333-8333-333333333333"
    private const val COMPLETE_REQUEST_ID = "44444444-4444-4444-8444-444444444444"
    private const val STARTED_REQUEST_ID = "55555555-5555-4555-8555-555555555555"
    private const val RECOVERY_GENERATION = 987L
  }
}
