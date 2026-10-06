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
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.util.UUID
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.findRawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.getVidLabelingEvictionFence
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.AdvanceUploadHealingStepRequest
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTask
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTaskState
import org.wfanet.measurement.internal.edpaggregator.LabeledOutputManifestKt
import org.wfanet.measurement.internal.edpaggregator.ListUploadHealingOperationsRequestKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineFailureReason
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation
import org.wfanet.measurement.internal.edpaggregator.UploadHealingStep
import org.wfanet.measurement.internal.edpaggregator.advanceUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.advanceUploadHealingStepRequest
import org.wfanet.measurement.internal.edpaggregator.approveUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.copy
import org.wfanet.measurement.internal.edpaggregator.createDataAvailabilitySyncTaskRequest
import org.wfanet.measurement.internal.edpaggregator.createUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.dataAvailabilitySyncTask
import org.wfanet.measurement.internal.edpaggregator.getDataAvailabilitySyncTaskRequest
import org.wfanet.measurement.internal.edpaggregator.getUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.labeledOutputManifest
import org.wfanet.measurement.internal.edpaggregator.listUploadHealingOperationsRequest
import org.wfanet.measurement.internal.edpaggregator.reconcileUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.retryUploadHealingOperationRequest
import org.wfanet.measurement.internal.edpaggregator.updateUploadHealingOperationPlanRequest
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
    val taskService = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)
    val replacementTask =
      createAvailabilityTask(
        taskService,
        REPLACEMENT_UPLOAD_ID,
        doneBlobUri = "gs://output/replacement/done",
      )
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

    val completeRequest = advanceUploadHealingStepRequest {
      dataProviderResourceId = DATA_PROVIDER_ID
      uploadHealingOperationId = OPERATION_ID
      uploadHealingStepId = 1L
      etag = started.etag
      action = AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT
      replacementRawImpressionUploadResourceId = REPLACEMENT_UPLOAD_ID
      requestId = COMPLETE_REQUEST_ID
    }
    val completed = service.advanceUploadHealingStep(completeRequest)

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
    val pendingReplacementTask =
      taskService.getDataAvailabilitySyncTask(
        getDataAvailabilitySyncTaskRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          rawImpressionUploadResourceId = REPLACEMENT_UPLOAD_ID
          dataAvailabilitySyncTaskResourceId = replacementTask.dataAvailabilitySyncTaskResourceId
        }
      )
    assertThat(pendingReplacementTask.state)
      .isEqualTo(DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_PENDING)
    assertThat(service.advanceUploadHealingStep(completeRequest)).isEqualTo(completed)
    Unit
  }

  @Test
  fun `confirming replacement eviction supersedes unfinished availability tasks`() = runBlocking {
    assertEvictionTerminatesTask(
      RawImpressionUploadModelLineRecoveryAction
        .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY,
      recoveryTarget = true,
      DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_SUPERSEDED,
    )
  }

  @Test
  fun `confirming no-replacement eviction cancels unfinished availability tasks`() = runBlocking {
    assertEvictionTerminatesTask(
      RawImpressionUploadModelLineRecoveryAction
        .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_NO_REPLACEMENT,
      recoveryTarget = false,
      DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_CANCELLED,
    )
  }

  @Test
  fun `completion transfers the fence to a pending candidate`() = runBlocking {
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    insertUploadGraph()
    val operation =
      service.createUploadHealingOperation(createRequest(memoized = false, recoveryTarget = false))
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newInsertBuilder("VidLabelingEvictionFence")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("EvictionOperationId")
          .to(OPERATION_ID)
          .set("Etag")
          .to("fence-etag")
          .set("State")
          .to(
            Value.protoEnum(
              org.wfanet.measurement.internal.edpaggregator.VidLabelingEvictionFenceState
                .VID_LABELING_EVICTION_FENCE_STATE_EVICTING
            )
          )
          .set("CreateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build()
      )
    )
    insertCorrectionCandidate(CANDIDATE_IDS[1])

    service.advanceUploadHealingStep(
      advanceUploadHealingStepRequest {
        dataProviderResourceId = DATA_PROVIDER_ID
        uploadHealingOperationId = OPERATION_ID
        uploadHealingStepId = 1L
        etag = operation.stepsList.single().etag
        action = AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION
        requestId = WAITING_REQUEST_ID
      }
    )

    val fence =
      spannerDatabase.databaseClient.singleUse().use {
        it.getVidLabelingEvictionFence(DATA_PROVIDER_ID)
      }
    assertThat(fence!!.evictionOperationId).isEqualTo(CANDIDATE_IDS[1])
    assertThat(fence.state)
      .isEqualTo(
        org.wfanet.measurement.internal.edpaggregator.VidLabelingEvictionFenceState
          .VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING
      )
  }

  @Test
  fun `create draft associates multiple correction candidates`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    insertCorrectionCandidate(CANDIDATE_IDS[1])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)

    val operation =
      service.createUploadHealingOperation(
        draftRequest(
          CANDIDATE_IDS.take(2),
          listOf(
            planStep(1L, SOURCE_UPLOAD_ID),
            planStep(2L, SUCCESSOR_UPLOAD_ID, candidateId = CANDIDATE_IDS[1]),
          ),
        )
      )

    assertThat(operation.state)
      .isEqualTo(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED)
    assertThat(operation.rawImpressionUploadCorrectionCandidateIdsList)
      .containsExactlyElementsIn(CANDIDATE_IDS.take(2))
      .inOrder()
    for (candidateId in CANDIDATE_IDS.take(2)) {
      val candidate = readCorrectionCandidate(candidateId)
      assertThat(candidate.state)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_ASSIGNED)
      assertThat(candidate.uploadHealingOperationId).isEqualTo(OPERATION_ID)
    }
  }

  @Test
  fun `reconcile creates and refreshes one draft idempotently`() = runBlocking {
    CANDIDATE_IDS.take(2).forEach { insertCorrectionCandidate(it) }
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val initial = draftRequest(CANDIDATE_IDS.take(1), listOf(planStep(1L, SOURCE_UPLOAD_ID)))
    val created =
      service.reconcileUploadHealingOperation(
        reconcileUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          uploadHealingOperation = initial.uploadHealingOperation
          requestId = CREATE_REQUEST_ID
        }
      )
    val refreshRequest = reconcileUploadHealingOperationRequest {
      dataProviderResourceId = DATA_PROVIDER_ID
      uploadHealingOperationId = OPERATION_ID
      uploadHealingOperation =
        initial.uploadHealingOperation.copy {
          rawImpressionUploadCorrectionCandidateIds.clear()
          rawImpressionUploadCorrectionCandidateIds += CANDIDATE_IDS.take(2)
        }
      etag = created.etag
      requestId = RECONCILE_REQUEST_ID
    }

    val refreshed = service.reconcileUploadHealingOperation(refreshRequest)
    val replayed = service.reconcileUploadHealingOperation(refreshRequest)

    assertThat(refreshed.rawImpressionUploadCorrectionCandidateIdsList)
      .containsExactlyElementsIn(CANDIDATE_IDS.take(2))
      .inOrder()
    assertThat(replayed).isEqualTo(refreshed)
    assertThat(readCorrectionCandidate(CANDIDATE_IDS[1]).uploadHealingOperationId)
      .isEqualTo(OPERATION_ID)
    Unit
  }

  @Test
  fun `reconcile rejects a stale draft etag`() = runBlocking {
    CANDIDATE_IDS.take(2).forEach { insertCorrectionCandidate(it) }
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val initial = draftRequest(CANDIDATE_IDS.take(1), listOf(planStep(1L, SOURCE_UPLOAD_ID)))
    service.reconcileUploadHealingOperation(
      reconcileUploadHealingOperationRequest {
        dataProviderResourceId = DATA_PROVIDER_ID
        uploadHealingOperationId = OPERATION_ID
        uploadHealingOperation = initial.uploadHealingOperation
        requestId = CREATE_REQUEST_ID
      }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.reconcileUploadHealingOperation(
          reconcileUploadHealingOperationRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            uploadHealingOperation =
              initial.uploadHealingOperation.copy {
                rawImpressionUploadCorrectionCandidateIds.clear()
                rawImpressionUploadCorrectionCandidateIds += CANDIDATE_IDS.take(2)
              }
            etag = "stale"
            requestId = RECONCILE_REQUEST_ID
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
    assertThat(readCorrectionCandidate(CANDIDATE_IDS[1]).state)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_PENDING)
    Unit
  }

  @Test
  fun `reconcile request ID cannot be reused for a different plan`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val request = draftRequest(listOf(CANDIDATE_IDS[0]), listOf(planStep(1L, SOURCE_UPLOAD_ID)))
    service.reconcileUploadHealingOperation(
      reconcileUploadHealingOperationRequest {
        dataProviderResourceId = DATA_PROVIDER_ID
        uploadHealingOperationId = OPERATION_ID
        uploadHealingOperation = request.uploadHealingOperation
        requestId = CREATE_REQUEST_ID
      }
    )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.reconcileUploadHealingOperation(
          reconcileUploadHealingOperationRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            uploadHealingOperation = request.uploadHealingOperation.copy { reason = "different" }
            requestId = CREATE_REQUEST_ID
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  @Test
  fun `no-op reconcile reserves its request ID`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val request = draftRequest(listOf(CANDIDATE_IDS[0]), listOf(planStep(1L, SOURCE_UPLOAD_ID)))
    val created =
      service.reconcileUploadHealingOperation(
        reconcileUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          uploadHealingOperation = request.uploadHealingOperation
          requestId = CREATE_REQUEST_ID
        }
      )
    service.reconcileUploadHealingOperation(
      reconcileUploadHealingOperationRequest {
        dataProviderResourceId = DATA_PROVIDER_ID
        uploadHealingOperationId = OPERATION_ID
        uploadHealingOperation = request.uploadHealingOperation
        etag = created.etag
        requestId = RECONCILE_REQUEST_ID
      }
    )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.reconcileUploadHealingOperation(
          reconcileUploadHealingOperationRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            uploadHealingOperation = request.uploadHealingOperation.copy { reason = "different" }
            etag = created.etag
            requestId = RECONCILE_REQUEST_ID
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  @Test
  fun `second controller cannot claim with a stale approved etag`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val created =
      service.createUploadHealingOperation(
        draftRequest(listOf(CANDIDATE_IDS[0]), listOf(planStep(1L, SOURCE_UPLOAD_ID)))
      )
    val approved =
      service.approveUploadHealingOperation(
        approveUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          etag = created.etag
          candidateDecisions +=
            approvalDecision(
              RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE
            )
          requestId = APPROVE_REQUEST_ID
        }
      )
    service.advanceUploadHealingOperation(
      advanceUploadHealingOperationRequest {
        dataProviderResourceId = DATA_PROVIDER_ID
        uploadHealingOperationId = OPERATION_ID
        etag = approved.etag
        state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_DRAINING
        requestId = UUID.randomUUID().toString()
      }
    )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.advanceUploadHealingOperation(
          advanceUploadHealingOperationRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            etag = approved.etag
            state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_DRAINING
            requestId = UUID.randomUUID().toString()
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.ABORTED)
  }

  @Test
  fun `advance operation retry returns success after commit`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val created =
      service.createUploadHealingOperation(
        draftRequest(listOf(CANDIDATE_IDS[0]), listOf(planStep(1L, SOURCE_UPLOAD_ID)))
      )
    val approved =
      service.approveUploadHealingOperation(
        approveUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          etag = created.etag
          candidateDecisions +=
            approvalDecision(
              RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE
            )
          requestId = APPROVE_REQUEST_ID
        }
      )
    val request = advanceUploadHealingOperationRequest {
      dataProviderResourceId = DATA_PROVIDER_ID
      uploadHealingOperationId = OPERATION_ID
      etag = approved.etag
      state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_DRAINING
      requestId = ADVANCE_REQUEST_ID
    }

    val advanced = service.advanceUploadHealingOperation(request)
    val retried = service.advanceUploadHealingOperation(request)

    assertThat(retried).isEqualTo(advanced)
  }

  @Test
  fun `advance operation rejects request ID reused with different transition`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val created =
      service.createUploadHealingOperation(
        draftRequest(listOf(CANDIDATE_IDS[0]), listOf(planStep(1L, SOURCE_UPLOAD_ID)))
      )
    val approved =
      service.approveUploadHealingOperation(
        approveUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          etag = created.etag
          candidateDecisions +=
            approvalDecision(
              RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE
            )
          requestId = APPROVE_REQUEST_ID
        }
      )
    service.advanceUploadHealingOperation(
      advanceUploadHealingOperationRequest {
        dataProviderResourceId = DATA_PROVIDER_ID
        uploadHealingOperationId = OPERATION_ID
        etag = approved.etag
        state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_DRAINING
        requestId = ADVANCE_REQUEST_ID
      }
    )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.advanceUploadHealingOperation(
          advanceUploadHealingOperationRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            etag = approved.etag
            state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING
            requestId = ADVANCE_REQUEST_ID
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  @Test
  fun `reconcile replaces a superseded draft candidate`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val initial =
      service.reconcileUploadHealingOperation(
        reconcileUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          uploadHealingOperation =
            draftRequest(listOf(CANDIDATE_IDS[0]), listOf(planStep(1L, SOURCE_UPLOAD_ID)))
              .uploadHealingOperation
          requestId = CREATE_REQUEST_ID
        }
      )
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newUpdateBuilder("RawImpressionUploadCorrectionCandidate")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("RawImpressionUploadCorrectionCandidateId")
          .to(CANDIDATE_IDS[0])
          .set("State")
          .to(Value.protoEnum(RawImpressionUploadCorrectionCandidate.State.STATE_SUPERSEDED))
          .set("SupersedingRawImpressionUploadCorrectionCandidateId")
          .to(CANDIDATE_IDS[1])
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build()
      )
    )
    insertCorrectionCandidate(CANDIDATE_IDS[1])

    val refreshed =
      service.reconcileUploadHealingOperation(
        reconcileUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          uploadHealingOperation =
            draftRequest(
                listOf(CANDIDATE_IDS[1]),
                listOf(planStep(1L, SOURCE_UPLOAD_ID, candidateId = CANDIDATE_IDS[1])),
              )
              .uploadHealingOperation
          etag = initial.etag
          requestId = RECONCILE_REQUEST_ID
        }
      )

    assertThat(refreshed.rawImpressionUploadCorrectionCandidateIdsList)
      .containsExactly(CANDIDATE_IDS[1])
    assertThat(readCorrectionCandidate(CANDIDATE_IDS[0]).state)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_SUPERSEDED)
    assertThat(readCorrectionCandidate(CANDIDATE_IDS[1]).state)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_ASSIGNED)
  }

  @Test
  fun `list paginates and filters plans`(): Unit = runBlocking {
    CANDIDATE_IDS.forEach { insertCorrectionCandidate(it) }
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    for (index in CANDIDATE_IDS.indices) {
      val request =
        draftRequest(
            listOf(CANDIDATE_IDS[index]),
            if (index == 1) {
              emptyList()
            } else {
              listOf(planStep(1L, SOURCE_UPLOAD_ID, candidateId = CANDIDATE_IDS[index]))
            },
            OPERATION_IDS[index],
            CREATE_REQUEST_IDS[index],
          )
          .copy {
            if (index == 1) {
              uploadHealingOperation =
                uploadHealingOperation.copy {
                  state =
                    UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
                }
            }
          }
      service.createUploadHealingOperation(request)
    }

    val first =
      service.listUploadHealingOperations(
        listUploadHealingOperationsRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          pageSize = 1
          filter =
            ListUploadHealingOperationsRequestKt.filter {
              stateIn +=
                UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED
            }
        }
      )
    val second =
      service.listUploadHealingOperations(
        listUploadHealingOperationsRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          pageSize = 1
          pageToken = first.nextPageToken
          filter =
            ListUploadHealingOperationsRequestKt.filter {
              stateIn +=
                UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED
            }
        }
      )

    assertThat(first.uploadHealingOperationsList.map { it.uploadHealingOperationId })
      .containsExactly(OPERATION_IDS[0])
    assertThat(first.hasNextPageToken()).isTrue()
    assertThat(second.uploadHealingOperationsList.map { it.uploadHealingOperationId })
      .containsExactly(OPERATION_IDS[2])
    assertThat(second.hasNextPageToken()).isFalse()
    assertFailsWith<IllegalArgumentException> {
      service.listUploadHealingOperations(
        listUploadHealingOperationsRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          pageSize = 1
          pageToken = first.nextPageToken
          filter =
            ListUploadHealingOperationsRequestKt.filter {
              stateIn += UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
            }
        }
      )
    }
    assertFailsWith<IllegalArgumentException> {
      service.listUploadHealingOperations(
        listUploadHealingOperationsRequest {
          dataProviderResourceId = "another-data-provider"
          pageSize = 1
          pageToken = first.nextPageToken
          filter =
            ListUploadHealingOperationsRequestKt.filter {
              stateIn +=
                UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED
            }
        }
      )
    }
  }

  @Test
  fun `candidate-backed operation cannot start in evicting`(): Unit = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val request =
      draftRequest(listOf(CANDIDATE_IDS[0]), listOf(planStep(1L, SOURCE_UPLOAD_ID))).copy {
        uploadHealingOperation =
          uploadHealingOperation.copy {
            state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING
          }
      }

    assertFailsWith<IllegalArgumentException> { service.createUploadHealingOperation(request) }
  }

  @Test
  fun `update draft replaces steps and candidate membership`() = runBlocking {
    CANDIDATE_IDS.forEach { insertCorrectionCandidate(it) }
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val created =
      service.createUploadHealingOperation(
        draftRequest(
          CANDIDATE_IDS.take(2),
          listOf(
            planStep(1L, SOURCE_UPLOAD_ID),
            planStep(2L, SUCCESSOR_UPLOAD_ID, candidateId = CANDIDATE_IDS[1]),
          ),
        )
      )
    val candidateUpdatedPlan =
      created.copy {
        rawImpressionUploadCorrectionCandidateIds.clear()
        rawImpressionUploadCorrectionCandidateIds += CANDIDATE_IDS.drop(1)
        steps.clear()
        steps += planStep(1L, SOURCE_UPLOAD_ID, candidateId = CANDIDATE_IDS[1])
      }
    val candidateUpdated =
      service.updateUploadHealingOperationPlan(
        updateUploadHealingOperationPlanRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          uploadHealingOperation = candidateUpdatedPlan
          etag = created.etag
        }
      )
    val stepsUpdatedPlan =
      candidateUpdated.copy {
        steps += planStep(2L, SUCCESSOR_UPLOAD_ID, candidateId = CANDIDATE_IDS[2])
      }
    val stepsUpdated =
      service.updateUploadHealingOperationPlan(
        updateUploadHealingOperationPlanRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          uploadHealingOperation = stepsUpdatedPlan
          etag = candidateUpdated.etag
        }
      )

    assertThat(candidateUpdated.etag).isNotEqualTo(created.etag)
    assertThat(stepsUpdated.etag).isNotEqualTo(candidateUpdated.etag)
    assertThat(stepsUpdated.stepsList.map { it.sourceRawImpressionUploadResourceId })
      .containsExactly(SOURCE_UPLOAD_ID, SUCCESSOR_UPLOAD_ID)
      .inOrder()
    assertThat(readCorrectionCandidate(CANDIDATE_IDS[0]).state)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_PENDING)
    assertThat(readCorrectionCandidate(CANDIDATE_IDS[2]).uploadHealingOperationId)
      .isEqualTo(OPERATION_ID)
  }

  @Test
  fun `approved plan is immutable`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    insertCorrectionCandidate(CANDIDATE_IDS[1])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val created =
      service.createUploadHealingOperation(
        draftRequest(
          CANDIDATE_IDS.take(2),
          listOf(
            planStep(1L, SOURCE_UPLOAD_ID),
            planStep(2L, SUCCESSOR_UPLOAD_ID, candidateId = CANDIDATE_IDS[1]),
          ),
        )
      )
    val approved =
      service.approveUploadHealingOperation(
        approveUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          etag = created.etag
          candidateDecisions +=
            CANDIDATE_IDS.take(2).map {
              approvalDecision(
                RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE,
                it,
              )
            }
          requestId = APPROVE_REQUEST_ID
        }
      )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.updateUploadHealingOperationPlan(
          updateUploadHealingOperationPlanRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            uploadHealingOperation =
              approved.copy {
                state =
                  UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED
                reason = "changed"
              }
            etag = approved.etag
          }
        )
      }

    assertThat(approved.etag).isNotEqualTo(created.etag)
    for (candidateId in CANDIDATE_IDS.take(2)) {
      val candidate = readCorrectionCandidate(candidateId)
      assertThat(candidate.state)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_ASSIGNED)
      assertThat(candidate.decision)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE)
    }
    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `operation advances through automatic workflow states`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    var operation =
      service.createUploadHealingOperation(
        draftRequest(listOf(CANDIDATE_IDS[0]), listOf(planStep(1L, SOURCE_UPLOAD_ID)))
      )
    val approveRequest = approveUploadHealingOperationRequest {
      dataProviderResourceId = DATA_PROVIDER_ID
      uploadHealingOperationId = OPERATION_ID
      etag = operation.etag
      candidateDecisions +=
        approvalDecision(RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE)
      requestId = APPROVE_REQUEST_ID
    }
    operation = service.approveUploadHealingOperation(approveRequest)

    for (state in
      listOf(
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_DRAINING,
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING,
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_REPLAYING,
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_RECOVERING,
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION,
      )) {
      val previousEtag = operation.etag
      operation =
        service.advanceUploadHealingOperation(
          advanceUploadHealingOperationRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            etag = previousEtag
            this.state = state
            requestId = UUID.randomUUID().toString()
          }
        )

      assertThat(operation.state).isEqualTo(state)
      assertThat(operation.etag).isNotEqualTo(previousEtag)
      if (state == UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_DRAINING) {
        assertThat(service.approveUploadHealingOperation(approveRequest)).isEqualTo(operation)
      }
    }
    assertThat(readCorrectionCandidate(CANDIDATE_IDS[0]).state)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_MANUAL_INTERVENTION_REQUIRED)
  }

  @Test
  fun `draft failure marks every candidate as needing attention`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val operation =
      service.createUploadHealingOperation(
        draftRequest(listOf(CANDIDATE_IDS[0]), listOf(planStep(1L, SOURCE_UPLOAD_ID)))
      )

    val needsAttention =
      service.advanceUploadHealingOperation(
        advanceUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          etag = operation.etag
          state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
          requestId = UUID.randomUUID().toString()
        }
      )

    assertThat(needsAttention.state)
      .isEqualTo(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION)
    assertThat(readCorrectionCandidate(CANDIDATE_IDS[0]).state)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_MANUAL_INTERVENTION_REQUIRED)
  }

  @Test
  fun `draining failure resumes from draining`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    var operation =
      service.createUploadHealingOperation(
        draftRequest(listOf(CANDIDATE_IDS[0]), listOf(planStep(1L, SOURCE_UPLOAD_ID)))
      )
    operation =
      service.approveUploadHealingOperation(
        approveUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          etag = operation.etag
          candidateDecisions +=
            approvalDecision(
              RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE
            )
          requestId = APPROVE_REQUEST_ID
        }
      )
    operation =
      service.advanceUploadHealingOperation(
        advanceUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          etag = operation.etag
          state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_DRAINING
          requestId = UUID.randomUUID().toString()
        }
      )
    val failed =
      service.advanceUploadHealingOperation(
        advanceUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          etag = operation.etag
          state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
          requestId = UUID.randomUUID().toString()
        }
      )

    val retryRequest = retryUploadHealingOperationRequest {
      dataProviderResourceId = DATA_PROVIDER_ID
      uploadHealingOperationId = OPERATION_ID
      etag = failed.etag
      requestId = RETRY_REQUEST_ID
    }
    val resumed = service.retryUploadHealingOperation(retryRequest)
    val progressed =
      service.advanceUploadHealingOperation(
        advanceUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          etag = resumed.etag
          state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING
          requestId = UUID.randomUUID().toString()
        }
      )
    val replayedRetry = service.retryUploadHealingOperation(retryRequest)

    assertThat(failed.resumeState)
      .isEqualTo(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_DRAINING)
    assertThat(resumed.state)
      .isEqualTo(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_DRAINING)
    assertThat(replayedRetry).isEqualTo(progressed)
    assertThat(readCorrectionCandidate(CANDIDATE_IDS[0]).state)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_ASSIGNED)
  }

  @Test
  fun `runtime retry preserves partial step checkpoints`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    var operation =
      service.createUploadHealingOperation(
        draftRequest(
          listOf(CANDIDATE_IDS[0]),
          listOf(planStep(1L, SOURCE_UPLOAD_ID), planStep(2L, SUCCESSOR_UPLOAD_ID)),
        )
      )
    operation =
      service.approveUploadHealingOperation(
        approveUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          etag = operation.etag
          candidateDecisions +=
            approvalDecision(
              RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE
            )
          requestId = APPROVE_REQUEST_ID
        }
      )
    for (state in
      listOf(
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_DRAINING,
        UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING,
      )) {
      operation =
        service.advanceUploadHealingOperation(
          advanceUploadHealingOperationRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            etag = operation.etag
            this.state = state
            requestId = UUID.randomUUID().toString()
          }
        )
    }
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newUpdateBuilder("UploadHealingStep")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("UploadHealingOperationId")
          .to(OPERATION_ID)
          .set("UploadHealingStepId")
          .to(1L)
          .set("EvictionCompleteTime")
          .to(Value.COMMIT_TIMESTAMP)
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build()
      )
    )
    operation =
      service.getUploadHealingOperation(
        getUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
        }
      )

    val failed =
      service.advanceUploadHealingOperation(
        advanceUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          etag = operation.etag
          state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
          requestId = UUID.randomUUID().toString()
        }
      )
    val updateError =
      assertFailsWith<StatusRuntimeException> {
        service.updateUploadHealingOperationPlan(
          updateUploadHealingOperationPlanRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            uploadHealingOperation =
              failed.copy {
                state =
                  UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED
                resumeState =
                  UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED
              }
            etag = failed.etag
          }
        )
      }
    val resumed =
      service.retryUploadHealingOperation(
        retryUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          etag = failed.etag
          requestId = RETRY_REQUEST_ID
        }
      )

    assertThat(updateError.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(failed.resumeState)
      .isEqualTo(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_EVICTING)
    assertThat(resumed.resumeState)
      .isEqualTo(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_UNSPECIFIED)
    assertThat(resumed.stepsList.map { it.state })
      .containsExactly(
        UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_WAITING_FOR_REPLACEMENT,
        UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_PENDING_EVICTION,
      )
      .inOrder()
    assertThat(readCorrectionCandidate(CANDIDATE_IDS[0]).state)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_ASSIGNED)
  }

  @Test
  fun `create persists needs-attention plan without steps`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val request =
      draftRequest(listOf(CANDIDATE_IDS[0]), emptyList()).copy {
        uploadHealingOperation =
          uploadHealingOperation.copy {
            state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
          }
      }

    val operation = service.createUploadHealingOperation(request)

    assertThat(operation.state)
      .isEqualTo(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION)
    assertThat(readCorrectionCandidate(CANDIDATE_IDS[0]).state)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_MANUAL_INTERVENTION_REQUIRED)
  }

  @Test
  fun `draft can become needs-attention when its owner leaves retention`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val draft =
      service.createUploadHealingOperation(
        draftRequest(listOf(CANDIDATE_IDS[0]), listOf(planStep(1L, SOURCE_UPLOAD_ID)))
      )
    val needsAttentionPlan =
      draft.copy {
        state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
        reason = "Affected upload is outside the healing retention window"
        steps.clear()
      }

    val needsAttention =
      service.updateUploadHealingOperationPlan(
        updateUploadHealingOperationPlanRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          uploadHealingOperation = needsAttentionPlan
          etag = draft.etag
        }
      )

    assertThat(needsAttention.state)
      .isEqualTo(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION)
    assertThat(readCorrectionCandidate(CANDIDATE_IDS[0]).state)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_MANUAL_INTERVENTION_REQUIRED)
  }

  @Test
  fun `retry replaces needs-attention plan and restores approval state`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val needsAttention =
      service.createUploadHealingOperation(
        draftRequest(listOf(CANDIDATE_IDS[0]), emptyList()).copy {
          uploadHealingOperation =
            uploadHealingOperation.copy {
              state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
            }
        }
      )

    val retried =
      service.updateUploadHealingOperationPlan(
        updateUploadHealingOperationPlanRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          uploadHealingOperation =
            needsAttention.copy {
              state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED
              steps += planStep(1L, SOURCE_UPLOAD_ID)
            }
          etag = needsAttention.etag
        }
      )

    assertThat(retried.state)
      .isEqualTo(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED)
    assertThat(retried.stepsList).hasSize(1)
    assertThat(readCorrectionCandidate(CANDIDATE_IDS[0]).state)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_ASSIGNED)
  }

  @Test
  fun `step cannot advance before plan approval and draining`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val operation =
      service.createUploadHealingOperation(
        draftRequest(listOf(CANDIDATE_IDS[0]), listOf(planStep(1L, SOURCE_UPLOAD_ID)))
      )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.advanceUploadHealingStep(
          advanceUploadHealingStepRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            uploadHealingStepId = 1L
            etag = operation.stepsList.single().etag
            action = AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION
            requestId = WAITING_REQUEST_ID
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `operation cannot be approved before every candidate is decided`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val operation =
      service.createUploadHealingOperation(
        draftRequest(listOf(CANDIDATE_IDS[0]), listOf(planStep(1L, SOURCE_UPLOAD_ID)))
      )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.advanceUploadHealingOperation(
          advanceUploadHealingOperationRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            etag = operation.etag
            state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVED
            requestId = UUID.randomUUID().toString()
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `retry rejects plan that does not need attention`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val operation =
      service.createUploadHealingOperation(
        draftRequest(listOf(CANDIDATE_IDS[0]), listOf(planStep(1L, SOURCE_UPLOAD_ID)))
      )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.retryUploadHealingOperation(
          retryUploadHealingOperationRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            etag = operation.etag
            requestId = RETRY_REQUEST_ID
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `approval rejects non-version-4 request ID`() = runBlocking {
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)

    val error =
      assertFailsWith<IllegalArgumentException> {
        service.approveUploadHealingOperation(
          approveUploadHealingOperationRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            etag = "etag"
            candidateDecisions +=
              approvalDecision(
                RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE
              )
            requestId = "11111111-1111-1111-8111-111111111111"
          }
        )
      }

    assertThat(error).hasMessageThat().contains("request_id must be a UUID4")
  }

  @Test
  fun `approval rejects non-RFC-4122 request ID`() = runBlocking {
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)

    val error =
      assertFailsWith<IllegalArgumentException> {
        service.approveUploadHealingOperation(
          approveUploadHealingOperationRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            etag = "etag"
            candidateDecisions +=
              approvalDecision(
                RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE
              )
            requestId = "11111111-1111-4111-0111-111111111111"
          }
        )
      }

    assertThat(error).hasMessageThat().contains("request_id must be a UUID4")
  }

  @Test
  fun `approval rejects noncanonical request ID`() = runBlocking {
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)

    val error =
      assertFailsWith<IllegalArgumentException> {
        service.approveUploadHealingOperation(
          approveUploadHealingOperationRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            etag = "etag"
            candidateDecisions +=
              approvalDecision(
                RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE
              )
            requestId = "1-1-4111-8111-1"
          }
        )
      }

    assertThat(error).hasMessageThat().contains("request_id must be a UUID4")
  }

  @Test
  fun `no-replacement approval finalizes the stored plan`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    insertCorrectionCandidate(CANDIDATE_IDS[1])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val operation =
      service.createUploadHealingOperation(
        draftRequest(
          CANDIDATE_IDS.take(2),
          listOf(
            planStep(1L, SOURCE_UPLOAD_ID),
            planStep(2L, SUCCESSOR_UPLOAD_ID, candidateId = CANDIDATE_IDS[1]),
          ),
        )
      )
    val approved =
      service.approveUploadHealingOperation(
        approveUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          etag = operation.etag
          candidateDecisions +=
            CANDIDATE_IDS.take(2).map {
              approvalDecision(
                RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT,
                it,
              )
            }
          requestId = APPROVE_REQUEST_ID
        }
      )

    assertThat(approved.state)
      .isEqualTo(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVED)
    for (candidateId in CANDIDATE_IDS.take(2)) {
      val candidate = readCorrectionCandidate(candidateId)
      assertThat(candidate.state)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_ASSIGNED)
      assertThat(candidate.decision)
        .isEqualTo(
          RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT
        )
    }
    assertThat(approved.stepsList.map { it.recoveryAction }.distinct())
      .containsExactly(
        RawImpressionUploadModelLineRecoveryAction
          .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_NO_REPLACEMENT
      )
    assertThat(approved.stepsList.all { !it.recoveryTarget }).isTrue()
  }

  @Test
  fun `approval applies independent decisions to candidates in one plan`() = runBlocking {
    CANDIDATE_IDS.take(2).forEach { insertCorrectionCandidate(it) }
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val operation =
      service.createUploadHealingOperation(
        draftRequest(
          CANDIDATE_IDS.take(2),
          listOf(
            planStep(1L, SOURCE_UPLOAD_ID),
            planStep(2L, SUCCESSOR_UPLOAD_ID, candidateId = CANDIDATE_IDS[1]),
          ),
        )
      )

    val approved =
      service.approveUploadHealingOperation(
        approveUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          etag = operation.etag
          candidateDecisions +=
            approvalDecision(
              RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE,
              CANDIDATE_IDS[0],
            )
          candidateDecisions +=
            approvalDecision(
              RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT,
              CANDIDATE_IDS[1],
            )
          requestId = APPROVE_REQUEST_ID
        }
      )

    assertThat(approved.stepsList.map { it.recoveryAction })
      .containsExactly(
        RawImpressionUploadModelLineRecoveryAction
          .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_EDP_CORRECTION,
        RawImpressionUploadModelLineRecoveryAction
          .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_NO_REPLACEMENT,
      )
      .inOrder()
    assertThat(readCorrectionCandidate(CANDIDATE_IDS[0]).decision)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE)
    assertThat(readCorrectionCandidate(CANDIDATE_IDS[1]).decision)
      .isEqualTo(
        RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT
      )
  }

  @Test
  fun `superseded active plan requeues unaffected candidates for replanning`() = runBlocking {
    CANDIDATE_IDS.take(2).forEach { insertCorrectionCandidate(it) }
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    var operation =
      service.createUploadHealingOperation(
        draftRequest(
          CANDIDATE_IDS.take(2),
          listOf(
            planStep(1L, SOURCE_UPLOAD_ID),
            planStep(2L, SUCCESSOR_UPLOAD_ID, candidateId = CANDIDATE_IDS[1]),
          ),
        )
      )
    operation =
      service.approveUploadHealingOperation(
        approveUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          etag = operation.etag
          candidateDecisions +=
            CANDIDATE_IDS.take(2).map {
              approvalDecision(
                RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE,
                it,
              )
            }
          requestId = APPROVE_REQUEST_ID
        }
      )
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newInsertBuilder("VidLabelingEvictionFence")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("EvictionOperationId")
          .to(OPERATION_ID)
          .set("Etag")
          .to("fence-etag")
          .set("State")
          .to(
            Value.protoEnum(
              org.wfanet.measurement.internal.edpaggregator.VidLabelingEvictionFenceState
                .VID_LABELING_EVICTION_FENCE_STATE_EVICTING
            )
          )
          .set("CreateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build()
      )
    )
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newUpdateBuilder("RawImpressionUploadCorrectionCandidate")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("RawImpressionUploadCorrectionCandidateId")
          .to(CANDIDATE_IDS[0])
          .set("State")
          .to(Value.protoEnum(RawImpressionUploadCorrectionCandidate.State.STATE_SUPERSEDED))
          .set("SupersedingRawImpressionUploadCorrectionCandidateId")
          .to(CANDIDATE_IDS[2])
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build()
      )
    )

    val reopened =
      service.advanceUploadHealingOperation(
        advanceUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          etag = operation.etag
          state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED
          requestId = UUID.randomUUID().toString()
        }
      )

    assertThat(reopened.state)
      .isEqualTo(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED)
    assertThat(readCorrectionCandidate(CANDIDATE_IDS[0]).state)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_SUPERSEDED)
    val unaffected = readCorrectionCandidate(CANDIDATE_IDS[1])
    assertThat(unaffected.state)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.State.STATE_ASSIGNED)
    assertThat(unaffected.decision)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.Decision.DECISION_UNSPECIFIED)
    val fence =
      spannerDatabase.databaseClient.singleUse().use {
        it.getVidLabelingEvictionFence(DATA_PROVIDER_ID)
      }
    assertThat(fence!!.state)
      .isEqualTo(
        org.wfanet.measurement.internal.edpaggregator.VidLabelingEvictionFenceState
          .VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING
      )
  }

  @Test
  fun `no-replacement approval rewires memoized predecessors`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val operation =
      service.createUploadHealingOperation(
        draftRequest(
            listOf(CANDIDATE_IDS[0]),
            listOf(
              planStep(1L, "d2", memoized = true, predecessorUploadId = "d1"),
              planStep(
                2L,
                "d3",
                RawImpressionUploadModelLineRecoveryAction
                  .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY,
                memoized = true,
                predecessorUploadId = "d2",
                candidateId = "",
              ),
              planStep(3L, "d4", memoized = true, predecessorUploadId = "d3"),
              planStep(
                4L,
                "d5",
                RawImpressionUploadModelLineRecoveryAction
                  .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY,
                memoized = true,
                predecessorUploadId = "d4",
                candidateId = "",
              ),
            ),
          )
          .copy {
            uploadHealingOperation =
              uploadHealingOperation.copy {
                badRawImpressionUploadResourceIds.clear()
                badRawImpressionUploadResourceIds += listOf("d2", "d4")
              }
          }
      )

    val approved =
      service.approveUploadHealingOperation(
        approveUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          etag = operation.etag
          candidateDecisions +=
            approvalDecision(
              RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT
            )
          requestId = APPROVE_REQUEST_ID
        }
      )

    assertThat(
        approved.stepsList.map {
          Triple(
            it.recoveryAction,
            it.recoveryTarget,
            it.recoveryPredecessorRawImpressionUploadResourceId,
          )
        }
      )
      .containsExactly(
        Triple(
          RawImpressionUploadModelLineRecoveryAction
            .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_NO_REPLACEMENT,
          false,
          "d1",
        ),
        Triple(
          RawImpressionUploadModelLineRecoveryAction
            .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY,
          true,
          "d1",
        ),
        Triple(
          RawImpressionUploadModelLineRecoveryAction
            .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_NO_REPLACEMENT,
          false,
          "d3",
        ),
        Triple(
          RawImpressionUploadModelLineRecoveryAction
            .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY,
          true,
          "d3",
        ),
      )
      .inOrder()
  }

  @Test
  fun `approval request ID cannot be reused for another decision`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val operation =
      service.createUploadHealingOperation(
        draftRequest(listOf(CANDIDATE_IDS[0]), listOf(planStep(1L, SOURCE_UPLOAD_ID)))
      )

    service.approveUploadHealingOperation(
      approveUploadHealingOperationRequest {
        dataProviderResourceId = DATA_PROVIDER_ID
        uploadHealingOperationId = OPERATION_ID
        etag = operation.etag
        candidateDecisions +=
          approvalDecision(RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE)
        requestId = APPROVE_REQUEST_ID
      }
    )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.approveUploadHealingOperation(
          approveUploadHealingOperationRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            etag = operation.etag
            candidateDecisions +=
              approvalDecision(
                RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT
              )
            requestId = APPROVE_REQUEST_ID
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  @Test
  fun `approval request ID cannot be reused for another plan`() = runBlocking {
    insertCorrectionCandidate(CANDIDATE_IDS[0])
    insertCorrectionCandidate(CANDIDATE_IDS[1])
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val first =
      service.createUploadHealingOperation(
        draftRequest(
          listOf(CANDIDATE_IDS[0]),
          listOf(planStep(1L, SOURCE_UPLOAD_ID)),
          OPERATION_IDS[0],
          CREATE_REQUEST_IDS[0],
        )
      )
    val second =
      service.createUploadHealingOperation(
        draftRequest(
          listOf(CANDIDATE_IDS[1]),
          listOf(planStep(1L, SOURCE_UPLOAD_ID, candidateId = CANDIDATE_IDS[1])),
          OPERATION_IDS[1],
          CREATE_REQUEST_IDS[1],
        )
      )
    service.approveUploadHealingOperation(
      approveUploadHealingOperationRequest {
        dataProviderResourceId = DATA_PROVIDER_ID
        uploadHealingOperationId = first.uploadHealingOperationId
        etag = first.etag
        candidateDecisions +=
          approvalDecision(RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE)
        requestId = APPROVE_REQUEST_ID
      }
    )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.approveUploadHealingOperation(
          approveUploadHealingOperationRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = second.uploadHealingOperationId
            etag = second.etag
            candidateDecisions +=
              approvalDecision(
                RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE,
                CANDIDATE_IDS[1],
              )
            requestId = APPROVE_REQUEST_ID
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
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
          requestId = WAITING_REQUEST_ID
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
          requestId = STARTED_REQUEST_ID
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
            requestId = COMPLETE_REQUEST_ID
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
  fun `no-replacement step completes when eviction is confirmed`() = runBlocking {
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    insertUploadGraph()
    val created =
      service.createUploadHealingOperation(
        createRequest(
          memoized = true,
          recoveryAction =
            RawImpressionUploadModelLineRecoveryAction
              .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_NO_REPLACEMENT,
          recoveryTarget = false,
        )
      )

    val completed =
      service.advanceUploadHealingStep(
        advanceUploadHealingStepRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
          uploadHealingStepId = 1L
          etag = created.stepsList.single().etag
          action = AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION
          requestId = WAITING_REQUEST_ID
        }
      )

    assertThat(completed.state)
      .isEqualTo(UploadHealingStep.State.UPLOAD_HEALING_STEP_STATE_COMPLETE)
    assertThat(completed.replacementRawImpressionUploadResourceId).isEmpty()
    val operation =
      service.getUploadHealingOperation(
        getUploadHealingOperationRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          uploadHealingOperationId = OPERATION_ID
        }
      )
    assertThat(operation.state)
      .isEqualTo(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_COMPLETE)
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
              labeledOutputManifest = outputManifest(2L)
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
          requestId = WAITING_REQUEST_ID
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
            requestId = STARTED_REQUEST_ID
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
              labeledOutputManifest = outputManifest(1L)
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
            requestId = WAITING_REQUEST_ID
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `advance requires request ID`() = runBlocking {
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)

    val error =
      assertFailsWith<IllegalArgumentException> {
        service.advanceUploadHealingStep(
          advanceUploadHealingStepRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            uploadHealingStepId = 1L
            etag = "etag"
            action = AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION
          }
        )
      }

    assertThat(error).hasMessageThat().contains("request_id is required")
  }

  @Test
  fun `advance operation requires request ID`() = runBlocking {
    val service = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)

    val error =
      assertFailsWith<IllegalArgumentException> {
        service.advanceUploadHealingOperation(
          advanceUploadHealingOperationRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            uploadHealingOperationId = OPERATION_ID
            etag = "etag"
            state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_DRAINING
          }
        )
      }

    assertThat(error).hasMessageThat().contains("request_id is required")
  }

  private suspend fun assertEvictionTerminatesTask(
    recoveryAction: RawImpressionUploadModelLineRecoveryAction,
    recoveryTarget: Boolean,
    expectedState: DataAvailabilitySyncTaskState,
  ) {
    insertUploadGraph()
    val taskService = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)
    val task = createAvailabilityTask(taskService)
    val healingService = SpannerUploadHealingOperationService(spannerDatabase.databaseClient)
    val operation =
      healingService.createUploadHealingOperation(
        createRequest(
          memoized = false,
          recoveryAction = recoveryAction,
          recoveryTarget = recoveryTarget,
        )
      )

    healingService.advanceUploadHealingStep(
      advanceUploadHealingStepRequest {
        dataProviderResourceId = DATA_PROVIDER_ID
        uploadHealingOperationId = OPERATION_ID
        uploadHealingStepId = 1L
        etag = operation.stepsList.single().etag
        action = AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION
        requestId = WAITING_REQUEST_ID
      }
    )

    val terminated =
      taskService.getDataAvailabilitySyncTask(
        getDataAvailabilitySyncTaskRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          rawImpressionUploadResourceId = SOURCE_UPLOAD_ID
          dataAvailabilitySyncTaskResourceId = task.dataAvailabilitySyncTaskResourceId
        }
      )
    assertThat(terminated.state).isEqualTo(expectedState)
  }

  private suspend fun createAvailabilityTask(
    service: SpannerDataAvailabilitySyncTaskService,
    rawImpressionUploadResourceId: String = SOURCE_UPLOAD_ID,
    doneBlobUri: String = "gs://output/day/done",
  ): DataAvailabilitySyncTask {
    val generation = 123L
    val taskId =
      RequestIds.forDataAvailabilitySyncTask(
        VidLabelingTraceAttributes.gcsObjectPathHash(doneBlobUri),
        generation,
      )
    return service.createDataAvailabilitySyncTask(
      createDataAvailabilitySyncTaskRequest {
        dataProviderResourceId = DATA_PROVIDER_ID
        this.rawImpressionUploadResourceId = rawImpressionUploadResourceId
        dataAvailabilitySyncTaskResourceId = taskId
        requestId = taskId
        dataAvailabilitySyncTask = dataAvailabilitySyncTask {
          this.doneBlobUri = doneBlobUri
          doneBlobGeneration = generation
          cmmsModelLine = CMMS_MODEL_LINE
          eventDate = date {
            year = 2026
            month = 9
            day = 30
          }
        }
      }
    )
  }

  private fun createRequest(
    memoized: Boolean,
    recoveryAction: RawImpressionUploadModelLineRecoveryAction =
      RawImpressionUploadModelLineRecoveryAction
        .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY,
    recoveryTarget: Boolean = true,
  ) = createUploadHealingOperationRequest {
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
        this.recoveryAction = recoveryAction
        this.recoveryTarget = recoveryTarget
        labeledOutputManifest = outputManifest(1L)
      }
    }
  }

  private fun draftRequest(
    candidateIds: List<String>,
    steps: List<UploadHealingStep>,
    operationId: String = OPERATION_ID,
    requestId: String = CREATE_REQUEST_ID,
  ) = createUploadHealingOperationRequest {
    dataProviderResourceId = DATA_PROVIDER_ID
    uploadHealingOperationId = operationId
    this.requestId = requestId
    uploadHealingOperation = uploadHealingOperation {
      state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED
      reason = "raw-impression correction"
      labeledImpressionsBlobPrefix = "gs://output/vid"
      badRawImpressionUploadResourceIds += SOURCE_UPLOAD_ID
      rawImpressionUploadCorrectionCandidateIds += candidateIds
      cutoffTime = timestamp { seconds = 100L }
      this.steps += steps
    }
  }

  private fun approvalDecision(
    decision: RawImpressionUploadCorrectionCandidate.Decision,
    candidateId: String = CANDIDATE_IDS[0],
  ) =
    org.wfanet.measurement.internal.edpaggregator.ApproveUploadHealingOperationRequestKt
      .candidateDecision {
        rawImpressionUploadCorrectionCandidateId = candidateId
        this.decision = decision
      }

  private fun planStep(
    id: Long,
    uploadId: String,
    action: RawImpressionUploadModelLineRecoveryAction =
      RawImpressionUploadModelLineRecoveryAction
        .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_EDP_CORRECTION,
    recoveryTarget: Boolean = true,
    memoized: Boolean = false,
    predecessorUploadId: String = "",
    candidateId: String = CANDIDATE_IDS[0],
  ) = uploadHealingStep {
    uploadHealingStepId = id
    sequenceNumber = id - 1L
    sourceRawImpressionUploadResourceId = uploadId
    rawImpressionUploadModelLineResourceId = "model-line-$id"
    cmmsModelLine = CMMS_MODEL_LINE
    this.memoized = memoized
    recoveryAction = action
    this.recoveryTarget = recoveryTarget
    labeledOutputManifest = outputManifest(id, uploadId)
    recoveryPredecessorRawImpressionUploadResourceId = predecessorUploadId
    rawImpressionUploadCorrectionCandidateId = candidateId
  }

  private fun outputManifest(id: Long, uploadId: String = SOURCE_UPLOAD_ID) =
    labeledOutputManifest {
      blobs +=
        LabeledOutputManifestKt.blobVersion {
          blobUri = "gs://output/$uploadId/$id"
          generation = 100L + id
        }
    }

  private suspend fun insertCorrectionCandidate(candidateId: String) {
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newInsertBuilder("RawImpressionUploadCorrectionCandidate")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("RawImpressionUploadCorrectionCandidateId")
          .to(candidateId)
          .set("RawImpressionUploadResourceId")
          .to("upload-$candidateId")
          .set("CreateRequestId")
          .to(UUID.randomUUID().toString())
          .set("Classification")
          .to(
            Value.protoEnum(
              RawImpressionUploadCorrectionCandidate.Classification.CLASSIFICATION_EDITED
            )
          )
          .set("PriorManifestDigest")
          .to(com.google.cloud.ByteArray.copyFrom(ByteArray(32) { 1 }))
          .set("CurrentManifestDigest")
          .to(com.google.cloud.ByteArray.copyFrom(ByteArray(32) { 2 }))
          .set("ManifestComparison")
          .to(
            com.google.cloud.ByteArray.copyFrom(
              RawImpressionUploadCorrectionCandidate.ManifestComparison.getDefaultInstance()
                .toByteArray()
            )
          )
          .set("State")
          .to(Value.protoEnum(RawImpressionUploadCorrectionCandidate.State.STATE_PENDING))
          .set("Decision")
          .to(Value.protoEnum(RawImpressionUploadCorrectionCandidate.Decision.DECISION_UNSPECIFIED))
          .set("ExpireTime")
          .to(com.google.cloud.Timestamp.ofTimeSecondsAndNanos(1_000L, 0))
          .set("AdvanceRequestIds")
          .toStringArray(emptyList())
          .set("AdvanceRequestFingerprints")
          .toBytesArray(emptyList())
          .set("CreateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build()
      )
    )
  }

  private suspend fun readCorrectionCandidate(
    candidateId: String
  ): RawImpressionUploadCorrectionCandidate =
    spannerDatabase.databaseClient.singleUse().use {
      checkNotNull(it.findRawImpressionUploadCorrectionCandidate(DATA_PROVIDER_ID, candidateId))
        .rawImpressionUploadCorrectionCandidate
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
    private const val APPROVE_REQUEST_ID = "66666666-6666-4666-8666-666666666666"
    private const val RETRY_REQUEST_ID = "77777777-7777-4777-8777-777777777777"
    private const val RECONCILE_REQUEST_ID = "14141414-1414-4414-8414-141414141414"
    private const val ADVANCE_REQUEST_ID = "15151515-1515-4515-8515-151515151515"
    private const val RECOVERY_GENERATION = 987L
    private val OPERATION_IDS =
      listOf(
        OPERATION_ID,
        "88888888-8888-4888-8888-888888888888",
        "99999999-9999-4999-8999-999999999999",
      )
    private val CREATE_REQUEST_IDS =
      listOf(
        CREATE_REQUEST_ID,
        "12121212-1212-4212-8212-121212121212",
        "13131313-1313-4313-8313-131313131313",
      )
    private val CANDIDATE_IDS =
      listOf(
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
      )
  }
}
