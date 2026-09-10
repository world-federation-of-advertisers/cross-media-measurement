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

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.FieldMask
import com.google.protobuf.timestamp
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation
import org.wfanet.measurement.internal.edpaggregator.UploadHealingStep
import org.wfanet.measurement.internal.edpaggregator.createUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.getUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.updateUploadHealingStepRequest
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
    val createRequest = createUploadHealingOperationRequest {
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
          cmmsModelLine = "modelProviders/mp/modelSuites/ms/modelLines/ml"
          memoized = true
          recoveryAction =
            RawImpressionUploadModelLineRecoveryAction
              .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY
          recoveryTarget = true
        }
      }
    }

    val created = service.createUploadHealingOperation(createRequest)
    val replayed = service.createUploadHealingOperation(createRequest)

    assertThat(created.stepsList.single().state)
      .isEqualTo(UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_PENDING_EVICTION)
    assertThat(replayed).isEqualTo(created)

    val waitingRequest = updateUploadHealingStepRequest {
      dataProviderResourceId = DATA_PROVIDER_ID
      uploadHealingOperationId = OPERATION_ID
      uploadHealingStep = uploadHealingStep {
        uploadHealingStepId = 1L
        state = UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_WAITING_FOR_REPLACEMENT
        etag = created.stepsList.single().etag
      }
      updateMask = UPDATE_MASK
      requestId = WAITING_REQUEST_ID
    }
    val waiting = service.updateUploadHealingStep(waitingRequest)
    val waitingReplay = service.updateUploadHealingStep(waitingRequest)

    assertThat(waiting.state)
      .isEqualTo(UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_WAITING_FOR_REPLACEMENT)
    assertThat(waitingReplay).isEqualTo(waiting)

    val started =
      service.updateUploadHealingStep(
        updateUploadHealingStepRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          uploadHealingStep = uploadHealingStep {
            uploadHealingStepId = 1L
            state = UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_RECOVERY_STARTED
            recoveryDoneBlobGeneration = RECOVERY_GENERATION
            etag = waiting.etag
          }
          updateMask = UPDATE_MASK
          requestId = STARTED_REQUEST_ID
        }
      )

    assertThat(started.recoveryDoneBlobGeneration).isEqualTo(RECOVERY_GENERATION)

    val completed =
      service.updateUploadHealingStep(
        updateUploadHealingStepRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          uploadHealingStep = uploadHealingStep {
            uploadHealingStepId = 1L
            state = UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE
            replacementRawImpressionUploadResourceId = REPLACEMENT_UPLOAD_ID
            recoveryDoneBlobGeneration = RECOVERY_GENERATION
            etag = started.etag
          }
          updateMask = UPDATE_MASK
          requestId = COMPLETE_REQUEST_ID
        }
      )

    assertThat(completed.state)
      .isEqualTo(UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE)
    assertThat(completed.replacementRawImpressionUploadResourceId).isEqualTo(REPLACEMENT_UPLOAD_ID)
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

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private val UPDATE_MASK =
      FieldMask.newBuilder()
        .addPaths("state")
        .addPaths("replacement_raw_impression_upload_resource_id")
        .addPaths("recovery_done_blob_generation")
        .build()
    private const val DATA_PROVIDER_ID = "data-provider"
    private const val SOURCE_UPLOAD_ID = "raw-impression-upload"
    private const val REPLACEMENT_UPLOAD_ID = "replacement-upload"
    private const val MODEL_LINE_ROW_ID = "model-line-row"
    private const val OPERATION_ID = "11111111-1111-4111-8111-111111111111"
    private const val CREATE_REQUEST_ID = "22222222-2222-4222-8222-222222222222"
    private const val WAITING_REQUEST_ID = "33333333-3333-4333-8333-333333333333"
    private const val COMPLETE_REQUEST_ID = "44444444-4444-4444-8444-444444444444"
    private const val STARTED_REQUEST_ID = "55555555-5555-4555-8555-555555555555"
    private const val RECOVERY_GENERATION = 987L
  }
}
