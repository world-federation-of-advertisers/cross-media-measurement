/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.vidlabeling.healing

import com.google.protobuf.ByteString
import com.google.protobuf.util.Timestamps
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import java.io.IOException
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadCorrectionCandidateKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.UploadHealingOperationKey
import org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledOutputManifest
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledOutputManifestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadCorrectionCandidatesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListUploadHealingOperationsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidateServiceGrpcKt.RawImpressionUploadCorrectionCandidateServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingEvictionFenceState
import org.wfanet.measurement.edpaggregator.v1alpha.acquireRawImpressionUploadEvictionFenceRequest
import org.wfanet.measurement.edpaggregator.v1alpha.advanceRawImpressionUploadEvictionFenceRequest
import org.wfanet.measurement.edpaggregator.v1alpha.advanceUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.advanceUploadHealingStepRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadCorrectionCandidateRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.labeledOutputManifest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadCorrectionCandidatesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listUploadHealingOperationsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.reconcileUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingStep
import org.wfanet.measurement.edpaggregator.vidlabeling.RawImpressionUploadManifestClassifier
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds
import org.wfanet.measurement.internal.edpaggregator.LabeledOutputManifest as InternalLabeledOutputManifest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction as InternalRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation as InternalOperation

/** Reads the live raw-object manifest for one exact done-object generation. */
fun interface CorrectionManifestReader {
  suspend fun read(
    doneBlobUri: String,
    doneBlobGeneration: Long,
  ): Collection<RawImpressionUploadManifestClassifier.File>
}

/** Replays one exact done-object generation through the raw-upload watcher. */
fun interface DoneBlobReplayer {
  suspend fun replay(request: Request)

  data class Request(
    val doneBlobUri: String,
    val doneBlobGeneration: Long,
    val sourceRawImpressionUpload: String,
    val cmmsModelLines: List<String>,
    val uploadHealingOperation: String,
  )
}

/** Receives healing-controller lifecycle events. */
fun interface VidLabelingHealingControllerEventSink {
  fun record(event: Event)

  data class Event(
    val type: Type,
    val dataProvider: String,
    val uploadHealingOperation: String = "",
  ) {
    enum class Type {
      APPROVAL_PENDING,
      STALLED,
      MANIFEST_MISMATCH,
      OUT_OF_RETENTION,
      NEEDS_ATTENTION,
    }
  }
}

/** Plans and advances approved VID-labeling upload corrections. */
class VidLabelingHealingController(
  dataProviderConfigs: Collection<DataProviderConfig>,
  private val candidatesStub: RawImpressionUploadCorrectionCandidateServiceCoroutineStub,
  private val operationsStub: UploadHealingOperationServiceCoroutineStub,
  private val uploadsStub: RawImpressionUploadServiceCoroutineStub,
  private val filesStub: RawImpressionUploadFileServiceCoroutineStub,
  private val modelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val rankIndexBlobsStub: RankIndexBlobServiceCoroutineStub,
  private val plannerFactory: (DataProviderConfig) -> RawImpressionUploadCorrectionPlanner,
  private val evictionExecutorFactory: (DataProviderConfig) -> EvictionExecutor,
  private val manifestReader: CorrectionManifestReader,
  private val doneBlobReplayerFactory: (DataProviderConfig) -> DoneBlobReplayer,
  private val recoveryExecutorFactory: (DataProviderConfig) -> RecoveryExecutor,
  private val eventSink: VidLabelingHealingControllerEventSink =
    VidLabelingHealingControllerEventSink {},
  private val clock: Clock = Clock.systemUTC(),
) {
  /** Configuration for one DataProvider correction pipeline. */
  data class DataProviderConfig(
    val name: String,
    val labeledImpressionsBlobPrefix: String,
    val retention: Duration,
    val stallTimeout: Duration = Duration.ofHours(1),
  )

  private val dataProviderConfigs = dataProviderConfigs.associateBy { it.name }
  private val manifestClassifier = RawImpressionUploadManifestClassifier()

  init {
    require(this.dataProviderConfigs.size == dataProviderConfigs.size) {
      "DataProvider configurations must have unique names"
    }
    for (config in dataProviderConfigs) {
      require(config.name.isNotBlank()) { "DataProvider name is required" }
      require(config.labeledImpressionsBlobPrefix.isNotBlank()) {
        "labeledImpressionsBlobPrefix is required"
      }
      require(!config.retention.isNegative && !config.retention.isZero) {
        "retention must be positive"
      }
      require(!config.stallTimeout.isNegative && !config.stallTimeout.isZero) {
        "stallTimeout must be positive"
      }
    }
  }

  /** Result of one controller invocation. */
  data class RunResult(val processedDataProviders: Int, val failedDataProviders: Int)

  /** Reconciles draft plans and advances active operations. */
  suspend fun run(): RunResult {
    var processed = 0
    var failed = 0
    for (config in dataProviderConfigs.values.sortedBy { it.name }) {
      try {
        reconcileDraft(config)
        advanceUntilBlocked(config)
        processed++
      } catch (e: Exception) {
        if (e is CancellationException) throw e
        if (e is InterruptedException) {
          Thread.currentThread().interrupt()
          throw e
        }
        failed++
        eventSink.record(
          VidLabelingHealingControllerEventSink.Event(
            if (e is ManifestMismatchException) {
              VidLabelingHealingControllerEventSink.Event.Type.MANIFEST_MISMATCH
            } else {
              VidLabelingHealingControllerEventSink.Event.Type.NEEDS_ATTENTION
            },
            config.name,
          )
        )
        logger.log(Level.WARNING, "Healing controller could not process ${config.name}", e)
      }
    }
    return RunResult(processed, failed)
  }

  private suspend fun advanceUntilBlocked(config: DataProviderConfig) {
    repeat(MAX_TRANSITIONS_PER_RUN) {
      val before = listOperations(config.name, ACTIVE_STATES).singleOrNull() ?: return
      advanceActiveOperation(config, before)
      val after = listOperations(config.name, ACTIVE_STATES).singleOrNull() ?: return
      if (after.etag == before.etag) {
        if (
          after.hasUpdateTime() &&
            after.updateTime.toInstant().plus(config.stallTimeout) <= clock.instant()
        ) {
          eventSink.record(
            VidLabelingHealingControllerEventSink.Event(
              VidLabelingHealingControllerEventSink.Event.Type.STALLED,
              config.name,
              after.name,
            )
          )
        }
        return
      }
    }
  }

  private suspend fun reconcileDraft(config: DataProviderConfig) {
    val operations = listOperations(config.name, NONTERMINAL_STATES)
    val blocking = mutableListOf<UploadHealingOperation>()
    blocking += operations.filter { it.state in ACTIVE_STATES }
    for (operation in
      operations.filter {
        it.state == UploadHealingOperation.State.NEEDS_ATTENTION &&
          it.resumeState != UploadHealingOperation.State.STATE_UNSPECIFIED
      }) {
      val candidates =
        operation.rawImpressionUploadCorrectionCandidatesList.map { getCandidate(it) }
      if (candidates.any { it.state == RawImpressionUploadCorrectionCandidate.State.SUPERSEDED }) {
        advanceOperation(operation, UploadHealingOperation.State.APPROVAL_REQUIRED)
        return
      }
      blocking += operation
    }
    check(blocking.size <= 1) { "${config.name} has multiple active healing operations" }
    if (blocking.isNotEmpty()) return

    val drafts =
      operations.filter {
        it.state == UploadHealingOperation.State.APPROVAL_REQUIRED ||
          it.state == UploadHealingOperation.State.NEEDS_ATTENTION &&
            it.resumeState == UploadHealingOperation.State.STATE_UNSPECIFIED
      }
    check(drafts.size <= 1) { "${config.name} has multiple mutable correction plans" }
    val draft = drafts.singleOrNull()
    val summaries =
      listCandidates(
          config.name,
          listOf(
            RawImpressionUploadCorrectionCandidate.State.PENDING,
            RawImpressionUploadCorrectionCandidate.State.PLANNED,
            RawImpressionUploadCorrectionCandidate.State.MANUAL_INTERVENTION_REQUIRED,
          ),
        )
        .filter {
          it.state == RawImpressionUploadCorrectionCandidate.State.PENDING ||
            it.uploadHealingOperation == draft?.name
        }
    if (summaries.isEmpty()) return

    val operationId =
      draft?.name?.let {
        requireNotNull(UploadHealingOperationKey.fromName(it)).uploadHealingOperationId
      }
        ?: summaries
          .minWith(
            compareBy<RawImpressionUploadCorrectionCandidate> { it.createTime.seconds }
              .thenBy { it.createTime.nanos }
              .thenBy { it.name }
          )
          .let {
            requireNotNull(RawImpressionUploadCorrectionCandidateKey.fromName(it.name))
              .rawImpressionUploadCorrectionCandidateId
          }
    val cutoffTime = draft?.cutoffTime?.toInstant() ?: clock.instant().minus(config.retention)
    var planningFailed = false
    val plan =
      try {
        val candidates = summaries.map { getCandidate(it.name) }
        val ownerNames =
          candidates
            .flatMap { candidate ->
              candidate.manifestDifferencesList.mapNotNull { difference ->
                difference.historicalOwnerRawImpressionUpload.takeIf { it.isNotEmpty() }
              }
            }
            .distinct()
        val owners = ownerNames.associateWith { getUpload(it) }
        val plannerCandidates =
          candidates.map { candidate ->
            val historicalOwnerNames =
              candidate.manifestDifferencesList
                .mapNotNull { it.historicalOwnerRawImpressionUpload.takeIf(String::isNotEmpty) }
                .distinct()
            val historicalOwners =
              historicalOwnerNames.map { ownerName ->
                RawImpressionUploadCorrectionPlanner.HistoricalOwner(
                  ownerName,
                  owners.getValue(ownerName).createTime.toInstant(),
                )
              }
            val candidateUpload = getUpload(candidate.rawImpressionUpload)
            RawImpressionUploadCorrectionPlanner.Candidate(
              candidate.name,
              candidate.createTime.toInstant(),
              historicalOwners,
              supersedingRevisions(candidateUpload, historicalOwnerNames),
            )
          }
        plannerFactory(config)
          .plan(
            plannerCandidates,
            cutoffTime,
            mapOf(
              config.name to
                RawImpressionUploadCorrectionPlanner.DataProviderConfig(
                  config.labeledImpressionsBlobPrefix
                )
            ),
            operationIdsByDataProvider = mapOf(config.name to operationId),
          )
          .single()
          .toPublicPlan()
      } catch (e: Exception) {
        if (e is CancellationException || e is InterruptedException || e.isTransient()) throw e
        planningFailed = true
        uploadHealingOperation {
          reason = "Correction plan could not be computed: ${e.message.orEmpty()}"
          labeledImpressionsBlobPrefix = config.labeledImpressionsBlobPrefix
          badRawImpressionUploads += summaries.map { it.rawImpressionUpload }
          this.cutoffTime = cutoffTime.toProtoTime()
          rawImpressionUploadCorrectionCandidates += summaries.map { it.name }
        }
      }
    val reconciled =
      operationsStub.reconcileUploadHealingOperation(
        reconcileUploadHealingOperationRequest {
          parent = config.name
          uploadHealingOperation = plan
          uploadHealingOperationId = operationId
          if (draft != null) etag = draft.etag
          requestId = RequestIds.forReconcileUploadHealingOperation(operationId, plan.toByteArray())
        }
      )
    eventSink.record(
      VidLabelingHealingControllerEventSink.Event(
        if (planningFailed) {
          VidLabelingHealingControllerEventSink.Event.Type.NEEDS_ATTENTION
        } else if (reconciled.state == UploadHealingOperation.State.NEEDS_ATTENTION) {
          VidLabelingHealingControllerEventSink.Event.Type.OUT_OF_RETENTION
        } else {
          VidLabelingHealingControllerEventSink.Event.Type.APPROVAL_PENDING
        },
        config.name,
        reconciled.name,
      )
    )
  }

  private suspend fun advanceActiveOperation(
    config: DataProviderConfig,
    active: UploadHealingOperation,
  ) {
    try {
      if (
        active.rawImpressionUploadCorrectionCandidatesList
          .map { getCandidate(it) }
          .any { it.state == RawImpressionUploadCorrectionCandidate.State.SUPERSEDED }
      ) {
        advanceOperation(active, UploadHealingOperation.State.APPROVAL_REQUIRED)
        return
      }
      when (active.state) {
        UploadHealingOperation.State.APPROVED -> claim(active)
        UploadHealingOperation.State.DRAINING -> drain(active)
        UploadHealingOperation.State.EVICTING -> evict(config, active)
        UploadHealingOperation.State.REPLAYING,
        UploadHealingOperation.State.RECOVERING -> advanceRecovery(config, active)
        UploadHealingOperation.State.STATE_UNSPECIFIED,
        UploadHealingOperation.State.APPROVAL_REQUIRED,
        UploadHealingOperation.State.NEEDS_ATTENTION,
        UploadHealingOperation.State.COMPLETE,
        UploadHealingOperation.State.UNRECOGNIZED -> error("invalid active state ${active.state}")
      }
    } catch (e: Exception) {
      if (e is CancellationException) throw e
      if (e is InterruptedException) throw e
      if (e.isDrainInProgress()) return
      if (e.isTransient()) return
      moveToNeedsAttention(active.name, e)
    }
  }

  private suspend fun claim(operation: UploadHealingOperation) {
    advanceOperation(operation, UploadHealingOperation.State.DRAINING)
  }

  private suspend fun drain(operation: UploadHealingOperation) {
    val key = requireNotNull(UploadHealingOperationKey.fromName(operation.name))
    val acquireState =
      VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING
    val acquiredFence =
      uploadsStub.acquireRawImpressionUploadEvictionFence(
        acquireRawImpressionUploadEvictionFenceRequest {
          parent = key.parentKey.toName()
          evictionOperationId = key.uploadHealingOperationId
          state = acquireState
          requestId =
            RequestIds.forAcquireUploadEvictionFence(
              key.uploadHealingOperationId,
              acquireState.name,
            )
        }
      )
    var fenceState = acquiredFence.state
    var fenceEtag = acquiredFence.etag
    if (
      fenceState == VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_APPROVAL_PENDING
    ) {
      val nextState = VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_DRAINING
      fenceEtag =
        uploadsStub
          .advanceRawImpressionUploadEvictionFence(
            advanceRawImpressionUploadEvictionFenceRequest {
              parent = key.parentKey.toName()
              evictionOperationId = key.uploadHealingOperationId
              state = nextState
              etag = fenceEtag
              requestId =
                RequestIds.forAdvanceUploadEvictionFence(
                  key.uploadHealingOperationId,
                  nextState.name,
                  fenceEtag,
                )
            }
          )
          .etag
      fenceState = nextState
    }
    if (fenceState != VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING) {
      val nextState = VidLabelingEvictionFenceState.VID_LABELING_EVICTION_FENCE_STATE_EVICTING
      uploadsStub.advanceRawImpressionUploadEvictionFence(
        advanceRawImpressionUploadEvictionFenceRequest {
          parent = key.parentKey.toName()
          evictionOperationId = key.uploadHealingOperationId
          state = nextState
          etag = fenceEtag
          requestId =
            RequestIds.forAdvanceUploadEvictionFence(
              key.uploadHealingOperationId,
              nextState.name,
              fenceEtag,
            )
        }
      )
    }
    advanceOperation(operation, UploadHealingOperation.State.EVICTING)
  }

  private suspend fun evict(config: DataProviderConfig, initial: UploadHealingOperation) {
    verifyExactCandidateManifests(initial)
    var operation = initial
    evictionExecutorFactory(config).evict(operation.toEvictionPlan(), operation.reason) { entry ->
      val step =
        operation.stepsList.single { it.rawImpressionUploadModelLine == entry.modelLineName }
      if (step.state == UploadHealingStep.State.PENDING_EVICTION) {
        checkpoint(step, AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION)
        operation = getOperation(operation.name)
      }
    }
    operation = getOperation(operation.name)
    if (operation.state == UploadHealingOperation.State.COMPLETE) return
    advanceToNextRecoveryPhase(operation)
  }

  private suspend fun advanceRecovery(config: DataProviderConfig, initial: UploadHealingOperation) {
    var operation = initial
    var group = nextRecoveryGroup(operation) ?: return
    val replacement = findReadyReplacement(group)
    if (replacement != null) {
      for (step in group.filter { it.state == UploadHealingStep.State.WAITING_FOR_REPLACEMENT }) {
        checkpoint(
          step,
          AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY,
          recoveryDoneBlobGeneration = replacement.doneBlobGeneration,
        )
        operation = getOperation(operation.name)
      }
      val source = group.first().sourceRawImpressionUpload
      val action = group.first().recoveryAction
      group =
        operation.stepsList.filter {
          it.sourceRawImpressionUpload == source && it.recoveryAction == action && it.recoveryTarget
        }
      operation = confirmReplacement(operation, group, replacement.name)
      if (operation.state != UploadHealingOperation.State.COMPLETE) {
        advanceToNextRecoveryPhase(operation)
      }
      return
    }
    if (group.any { it.state == UploadHealingStep.State.RECOVERY_STARTED }) {
      val generation =
        group.firstOrNull { it.recoveryDoneBlobGeneration > 0L }?.recoveryDoneBlobGeneration
          ?: error("Recovery group has no recorded done-object generation")
      for (step in group.filter { it.state == UploadHealingStep.State.WAITING_FOR_REPLACEMENT }) {
        checkpoint(
          step,
          AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY,
          recoveryDoneBlobGeneration = generation,
        )
      }
      return
    }
    val recoveryDoneBlobGeneration =
      when (group.first().recoveryAction) {
        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION -> {
          verifyExactCandidateManifests(operation)
          val replay = correctionReplay(operation, group)
          doneBlobReplayerFactory(config).replay(replay)
          replay.doneBlobGeneration
        }
        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY -> {
          val source = getUpload(group.first().sourceRawImpressionUpload)
          verifyExactUploadManifest(source, completeSnapshot = false)
          recoveryExecutorFactory(config)
            .recover(source.name, group.map { it.cmmsModelLine }.distinct())
            .doneBlobGeneration
        }
        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_NO_REPLACEMENT ->
          error("A no-replacement step cannot be a recovery target")
        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_UNSPECIFIED,
        RawImpressionUploadModelLine.RecoveryAction.UNRECOGNIZED ->
          error("Healing step has no recovery action")
      }
    val stepNames = group.mapTo(mutableSetOf()) { it.name }
    for (step in group) {
      checkpoint(
        step,
        AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY,
        recoveryDoneBlobGeneration = recoveryDoneBlobGeneration,
      )
      operation = getOperation(operation.name)
      group = operation.stepsList.filter { it.name in stepNames }
    }
  }

  private suspend fun correctionReplay(
    operation: UploadHealingOperation,
    group: List<UploadHealingStep>,
  ): DoneBlobReplayer.Request {
    val source = group.first().sourceRawImpressionUpload
    val candidateNames =
      group.map { it.rawImpressionUploadCorrectionCandidate }.filter { it.isNotEmpty() }.distinct()
    check(candidateNames.size == 1) { "Correction replay for $source has no unique candidate" }
    val candidate =
      getCandidate(candidateNames.single()).also {
        check(it.decision == RawImpressionUploadCorrectionCandidate.Decision.DECISION_CORRECT) {
          "Correction candidate ${it.name} is not approved for replacement"
        }
      }
    val candidateUpload = getUpload(candidate.rawImpressionUpload)
    return DoneBlobReplayer.Request(
      candidateUpload.doneBlobUri,
      candidateUpload.doneBlobGeneration,
      source,
      group.map { it.cmmsModelLine }.distinct(),
      operation.name,
    )
  }

  private suspend fun advanceToNextRecoveryPhase(operation: UploadHealingOperation) {
    val group = nextRecoveryGroup(operation) ?: return
    val nextState =
      when (group.first().recoveryAction) {
        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION ->
          UploadHealingOperation.State.REPLAYING
        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY ->
          UploadHealingOperation.State.RECOVERING
        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_NO_REPLACEMENT,
        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_UNSPECIFIED,
        RawImpressionUploadModelLine.RecoveryAction.UNRECOGNIZED ->
          error("Healing recovery target has no replay action")
      }
    if (operation.state != nextState) advanceOperation(operation, nextState)
  }

  private fun nextRecoveryGroup(operation: UploadHealingOperation): List<UploadHealingStep>? =
    operation.stepsList
      .filter { it.recoveryTarget }
      .groupBy { it.sourceRawImpressionUpload to it.recoveryAction }
      .values
      .filterNot { steps -> steps.all { it.state == UploadHealingStep.State.COMPLETE } }
      .minByOrNull { steps -> steps.minOf { it.sequenceNumber } }

  private suspend fun verifyExactCandidateManifests(operation: UploadHealingOperation) {
    for (candidateName in operation.rawImpressionUploadCorrectionCandidatesList) {
      val candidate = getCandidate(candidateName)
      val upload = getUpload(candidate.rawImpressionUpload)
      val liveDigest = verifyExactUploadManifest(upload, completeSnapshot = true)
      if (liveDigest != candidate.currentManifestDigest) {
        throw ManifestMismatchException("Live manifest no longer matches $candidateName")
      }
    }
  }

  private suspend fun verifyExactUploadManifest(
    upload: RawImpressionUpload,
    completeSnapshot: Boolean,
  ): ByteString {
    val persistedDigest =
      if (completeSnapshot) {
        digestPersistedManifest(upload.name)
      } else {
        digestEffectivePersistedManifest(upload)
      }
    val liveDigest =
      manifestClassifier.digest(manifestReader.read(upload.doneBlobUri, upload.doneBlobGeneration))
    if (liveDigest != persistedDigest) {
      throw ManifestMismatchException("Live manifest no longer matches ${upload.name}")
    }
    return liveDigest
  }

  private suspend fun digestEffectivePersistedManifest(upload: RawImpressionUpload): ByteString {
    val parent = requireNotNull(RawImpressionUploadKey.fromName(upload.name)).parentKey.toName()
    val revisions =
      listUploads(parent, upload.doneBlobUri).map { revision ->
        RawImpressionUploadManifestClassifier.Revision(
          rawImpressionUpload = revision.name,
          doneBlobUri = revision.doneBlobUri,
          doneBlobGeneration = revision.doneBlobGeneration,
          doneBlobCreateTime =
            if (revision.hasDoneBlobCreateTime()) {
              revision.doneBlobCreateTime.toInstant()
            } else {
              null
            },
          createTime = revision.createTime.toInstant(),
          replacesRawImpressionUpload = revision.replacesRawImpressionUpload,
          uploadHealingOperation = revision.uploadHealingOperation,
          registrationComplete = revision.registrationComplete,
          failed = revision.state == RawImpressionUpload.State.FAILED,
          files = listPersistedFiles(revision.name),
        )
      }
    return manifestClassifier.digest(
      manifestClassifier
        .reconstructEffectiveManifest(
          requireNotNull(revisions.singleOrNull { it.rawImpressionUpload == upload.name }),
          revisions,
        )
        .values
        .map { it.file }
    )
  }

  private suspend fun digestPersistedManifest(uploadName: String): ByteString {
    return manifestClassifier.digest(listPersistedFiles(uploadName))
  }

  private suspend fun listPersistedFiles(
    uploadName: String
  ): List<RawImpressionUploadManifestClassifier.File> {
    val files = mutableListOf<RawImpressionUploadManifestClassifier.File>()
    var pageToken = ""
    do {
      val response =
        filesStub.listRawImpressionUploadFiles(
          listRawImpressionUploadFilesRequest {
            parent = uploadName
            this.pageToken = pageToken
          }
        )
      files +=
        response.rawImpressionUploadFilesList.map {
          RawImpressionUploadManifestClassifier.File(it.blobUri, it.blobGeneration, it.eventDate)
        }
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return files
  }

  private suspend fun confirmReplacement(
    initial: UploadHealingOperation,
    steps: List<UploadHealingStep>,
    replacementName: String,
  ): UploadHealingOperation {
    var operation = initial
    for (step in steps.filter { it.state != UploadHealingStep.State.COMPLETE }) {
      checkpoint(step, AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT, replacementName)
      operation = getOperation(operation.name)
    }
    return operation
  }

  private suspend fun checkpoint(
    step: UploadHealingStep,
    action: AdvanceUploadHealingStepRequest.Action,
    replacementName: String = "",
    recoveryDoneBlobGeneration: Long = 0L,
  ) {
    operationsStub.advanceUploadHealingStep(
      advanceUploadHealingStepRequest {
        name = step.name
        etag = step.etag
        this.action = action
        replacementRawImpressionUpload = replacementName
        this.recoveryDoneBlobGeneration = recoveryDoneBlobGeneration
        requestId =
          RequestIds.forUploadHealingStep(
            step.name,
            action.name,
            "$replacementName:$recoveryDoneBlobGeneration",
          )
      }
    )
  }

  private suspend fun advanceOperation(
    operation: UploadHealingOperation,
    state: UploadHealingOperation.State,
  ): UploadHealingOperation =
    operationsStub.advanceUploadHealingOperation(
      advanceUploadHealingOperationRequest {
        name = operation.name
        etag = operation.etag
        this.state = state
        requestId =
          RequestIds.forAdvanceUploadHealingOperation(operation.name, state.name, operation.etag)
      }
    )

  private suspend fun moveToNeedsAttention(operationName: String, cause: Throwable) {
    val operation = getOperation(operationName)
    if (
      operation.state == UploadHealingOperation.State.NEEDS_ATTENTION ||
        operation.state == UploadHealingOperation.State.COMPLETE
    ) {
      return
    }
    try {
      advanceOperation(operation, UploadHealingOperation.State.NEEDS_ATTENTION)
    } catch (advanceFailure: Throwable) {
      cause.addSuppressed(advanceFailure)
      throw cause
    }
    eventSink.record(
      VidLabelingHealingControllerEventSink.Event(
        if (cause is ManifestMismatchException) {
          VidLabelingHealingControllerEventSink.Event.Type.MANIFEST_MISMATCH
        } else {
          VidLabelingHealingControllerEventSink.Event.Type.NEEDS_ATTENTION
        },
        requireNotNull(UploadHealingOperationKey.fromName(operationName)).parentKey.toName(),
        operationName,
      )
    )
    logger.log(Level.SEVERE, "Healing operation $operationName needs attention", cause)
  }

  private suspend fun getOperation(name: String): UploadHealingOperation =
    operationsStub.getUploadHealingOperation(getUploadHealingOperationRequest { this.name = name })

  private suspend fun getCandidate(name: String): RawImpressionUploadCorrectionCandidate =
    candidatesStub.getRawImpressionUploadCorrectionCandidate(
      getRawImpressionUploadCorrectionCandidateRequest { this.name = name }
    )

  private suspend fun getUpload(name: String): RawImpressionUpload =
    uploadsStub.getRawImpressionUpload(getRawImpressionUploadRequest { this.name = name })

  private suspend fun listCandidates(
    parent: String,
    states: Collection<RawImpressionUploadCorrectionCandidate.State>,
  ): List<RawImpressionUploadCorrectionCandidate> {
    val candidates = mutableListOf<RawImpressionUploadCorrectionCandidate>()
    var pageToken = ""
    do {
      val response =
        candidatesStub.listRawImpressionUploadCorrectionCandidates(
          listRawImpressionUploadCorrectionCandidatesRequest {
            this.parent = parent
            filter =
              ListRawImpressionUploadCorrectionCandidatesRequestKt.filter { stateIn += states }
            this.pageToken = pageToken
          }
        )
      candidates += response.rawImpressionUploadCorrectionCandidatesList
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return candidates
  }

  private suspend fun listOperations(
    parent: String,
    states: Collection<UploadHealingOperation.State>,
  ): List<UploadHealingOperation> {
    val operations = mutableListOf<UploadHealingOperation>()
    var pageToken = ""
    do {
      val response =
        operationsStub.listUploadHealingOperations(
          listUploadHealingOperationsRequest {
            this.parent = parent
            filter = ListUploadHealingOperationsRequestKt.filter { stateIn += states }
            this.pageToken = pageToken
          }
        )
      operations += response.uploadHealingOperationsList
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return operations
  }

  private suspend fun findReadyReplacement(steps: List<UploadHealingStep>): RawImpressionUpload? {
    val sourceName = steps.first().sourceRawImpressionUpload
    val source = getUpload(sourceName)
    val sourceKey = requireNotNull(RawImpressionUploadKey.fromName(sourceName))
    val revisions = listUploads(sourceKey.parentKey.toName(), source.doneBlobUri)
    val latest = findLatestUpload(revisions) ?: return null
    val inPlaceRecovery =
      latest.name == sourceName &&
        steps.all {
          it.recoveryAction ==
            RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY &&
            it.recoveryDoneBlobGeneration == source.doneBlobGeneration
        }
    if (!inPlaceRecovery && !replacesUpload(latest.name, sourceName, revisions)) return null
    val rows = listModelLines(latest.name).associateBy { it.cmmsModelLine }
    if (latest.state == RawImpressionUpload.State.FAILED) {
      val stillEvicted =
        inPlaceRecovery &&
          steps.all { step ->
            rows[step.cmmsModelLine]?.let {
              it.state == RawImpressionUploadModelLine.State.FAILED &&
                it.failureReason == RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT
            } == true
          }
      if (stillEvicted) return null
      throw ReplacementFailedException("Replacement upload ${latest.name} failed")
    }
    if (!latest.registrationComplete || latest.state != RawImpressionUpload.State.COMPLETED) {
      return null
    }
    if (steps.any { rows[it.cmmsModelLine]?.state == RawImpressionUploadModelLine.State.FAILED }) {
      throw ReplacementFailedException("Replacement model line failed for ${latest.name}")
    }
    if (
      steps.any { rows[it.cmmsModelLine]?.state != RawImpressionUploadModelLine.State.COMPLETED }
    ) {
      return null
    }
    if (steps.filter { it.memoized }.any { !hasActiveSnapshot(latest.name, it.cmmsModelLine) }) {
      return null
    }
    return latest
  }

  private suspend fun listUploads(parent: String, doneBlobUri: String): List<RawImpressionUpload> {
    val uploads = mutableListOf<RawImpressionUpload>()
    var pageToken = ""
    do {
      val response =
        uploadsStub.listRawImpressionUploads(
          listRawImpressionUploadsRequest {
            this.parent = parent
            filter = ListRawImpressionUploadsRequestKt.filter { this.doneBlobUri = doneBlobUri }
            this.pageToken = pageToken
          }
        )
      uploads += response.rawImpressionUploadsList
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return uploads
  }

  private suspend fun listModelLines(uploadName: String): List<RawImpressionUploadModelLine> {
    val rows = mutableListOf<RawImpressionUploadModelLine>()
    var pageToken = ""
    do {
      val response =
        modelLinesStub.listRawImpressionUploadModelLines(
          listRawImpressionUploadModelLinesRequest {
            parent = uploadName
            this.pageToken = pageToken
          }
        )
      rows += response.rawImpressionUploadModelLinesList
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return rows
  }

  private suspend fun hasActiveSnapshot(uploadName: String, cmmsModelLine: String): Boolean {
    val response =
      rankIndexBlobsStub.listRankIndexBlobs(
        listRankIndexBlobsRequest {
          parent = uploadName
          pageSize = 1
          filter =
            org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequestKt.filter {
              blobType = RankIndexBlob.BlobType.SNAPSHOT
              this.cmmsModelLine = cmmsModelLine
            }
        }
      )
    return response.rankIndexBlobsCount > 0
  }

  private fun findLatestUpload(uploads: List<RawImpressionUpload>): RawImpressionUpload? {
    val timestamped = uploads.filter { it.hasDoneBlobCreateTime() }
    return if (timestamped.isNotEmpty()) {
      timestamped.maxWithOrNull { left, right ->
        Timestamps.compare(left.doneBlobCreateTime, right.doneBlobCreateTime)
      }
    } else {
      uploads.maxWithOrNull { left, right -> Timestamps.compare(left.createTime, right.createTime) }
    }
  }

  private fun replacesUpload(
    candidateName: String,
    sourceName: String,
    revisions: List<RawImpressionUpload>,
  ): Boolean {
    val byName = revisions.associateBy { it.name }
    val visited = mutableSetOf<String>()
    var current = byName[candidateName]?.replacesRawImpressionUpload.orEmpty()
    while (current.isNotEmpty() && visited.add(current)) {
      if (current == sourceName) return true
      current = byName[current]?.replacesRawImpressionUpload.orEmpty()
    }
    return false
  }

  private suspend fun supersedingRevisions(
    candidateUpload: RawImpressionUpload,
    historicalOwners: Collection<String>,
  ): Set<String> {
    val parent =
      requireNotNull(RawImpressionUploadKey.fromName(candidateUpload.name)).parentKey.toName()
    val revisions = listUploads(parent, candidateUpload.doneBlobUri).associateBy { it.name }
    val chain = mutableListOf<String>()
    var current = candidateUpload.replacesRawImpressionUpload
    while (current.isNotEmpty() && current !in chain) {
      chain += current
      current = revisions[current]?.replacesRawImpressionUpload.orEmpty()
    }
    val oldestOwnerIndex = historicalOwners.map { chain.indexOf(it) }.maxOrNull() ?: -1
    check(oldestOwnerIndex >= 0) {
      "Candidate ${candidateUpload.name} does not descend from its historical owners"
    }
    return chain.take(oldestOwnerIndex + 1).toSet()
  }

  private fun InternalOperation.toPublicPlan(): UploadHealingOperation {
    return uploadHealingOperation {
      reason = this@toPublicPlan.reason
      labeledImpressionsBlobPrefix = this@toPublicPlan.labeledImpressionsBlobPrefix
      badRawImpressionUploads +=
        badRawImpressionUploadResourceIdsList.map {
          RawImpressionUploadKey(dataProviderResourceId, it).toName()
        }
      cutoffTime = this@toPublicPlan.cutoffTime
      rawImpressionUploadCorrectionCandidates +=
        rawImpressionUploadCorrectionCandidateIdsList.map {
          RawImpressionUploadCorrectionCandidateKey(dataProviderResourceId, it).toName()
        }
      steps +=
        this@toPublicPlan.stepsList.map { step ->
          val source =
            RawImpressionUploadKey(dataProviderResourceId, step.sourceRawImpressionUploadResourceId)
          uploadHealingStep {
            sequenceNumber = step.sequenceNumber
            sourceRawImpressionUpload = source.toName()
            rawImpressionUploadModelLine =
              org.wfanet.measurement.edpaggregator.service
                .RawImpressionUploadModelLineKey(
                  source,
                  step.rawImpressionUploadModelLineResourceId,
                )
                .toName()
            cmmsModelLine = step.cmmsModelLine
            memoized = step.memoized
            recoveryAction = step.recoveryAction.toPublic()
            if (step.recoveryPredecessorRawImpressionUploadResourceId.isNotEmpty()) {
              recoveryPredecessorRawImpressionUpload =
                RawImpressionUploadKey(
                    dataProviderResourceId,
                    step.recoveryPredecessorRawImpressionUploadResourceId,
                  )
                  .toName()
            }
            recoveryTarget = step.recoveryTarget
            if (step.rawImpressionUploadCorrectionCandidateId.isNotEmpty()) {
              rawImpressionUploadCorrectionCandidate =
                RawImpressionUploadCorrectionCandidateKey(
                    dataProviderResourceId,
                    step.rawImpressionUploadCorrectionCandidateId,
                  )
                  .toName()
            }
            labeledOutputManifest = step.labeledOutputManifest.toPublic()
          }
        }
    }
  }

  private fun InternalRecoveryAction.toPublic(): RawImpressionUploadModelLine.RecoveryAction =
    when (this) {
      InternalRecoveryAction.RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_EDP_CORRECTION ->
        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION
      InternalRecoveryAction.RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY ->
        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
      InternalRecoveryAction.RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_NO_REPLACEMENT ->
        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_NO_REPLACEMENT
      InternalRecoveryAction.RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_UNSPECIFIED,
      InternalRecoveryAction.UNRECOGNIZED ->
        error("Planner produced an unspecified recovery action")
    }

  private fun UploadHealingOperation.toEvictionPlan(): EvictUploader.EvictionPlan {
    val operationId =
      requireNotNull(UploadHealingOperationKey.fromName(name)).uploadHealingOperationId
    val cascade =
      stepsList
        .sortedBy { it.sequenceNumber }
        .map { step ->
          EvictUploader.CascadeEntry(
            step.sourceRawImpressionUpload,
            step.rawImpressionUploadModelLine,
            step.cmmsModelLine,
            step.memoized,
            step.recoveryAction,
            step.recoveryPredecessorRawImpressionUpload,
            step.labeledOutputManifest,
          )
        }
    val badUploads = badRawImpressionUploadsList.toSet()
    fun recoveryTargets(action: RawImpressionUploadModelLine.RecoveryAction) =
      stepsList
        .filter { it.recoveryTarget && it.recoveryAction == action }
        .groupBy { it.sourceRawImpressionUpload }
        .map { (uploadName, steps) ->
          EvictUploader.RecoveryTarget(uploadName, steps.map { it.cmmsModelLine }.distinct())
        }
    return EvictUploader.EvictionPlan(
      cascade,
      cascade.map { it.uploadName }.filter { it !in badUploads }.distinct(),
      cascade.filter { it.memoized }.mapTo(mutableSetOf()) { it.cmmsModelLine },
      cascade.filterNot { it.memoized }.mapTo(mutableSetOf()) { it.cmmsModelLine },
      badRawImpressionUploadsList,
      cascade
        .filter {
          it.recoveryAction ==
            RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_NO_REPLACEMENT
        }
        .mapTo(mutableSetOf()) { it.uploadName },
      cutoffTime.toInstant(),
      operationId,
      recoveryTargets(
        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
      ),
      recoveryTargets(RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION),
    )
  }

  private fun InternalLabeledOutputManifest.toPublic(): LabeledOutputManifest =
    labeledOutputManifest {
      blobs +=
        this@toPublic.blobsList.map { blob ->
          LabeledOutputManifestKt.blobVersion {
            blobUri = blob.blobUri
            if (blob.hasGeneration()) generation = blob.generation
          }
        }
    }

  private fun Throwable.isDrainInProgress(): Boolean =
    statusCode() == Status.Code.FAILED_PRECONDITION &&
      message.orEmpty().contains("lease", ignoreCase = true)

  private fun Throwable.isTransient(): Boolean =
    this is IOException ||
      statusCode() in
        setOf(
          Status.Code.ABORTED,
          Status.Code.CANCELLED,
          Status.Code.DEADLINE_EXCEEDED,
          Status.Code.RESOURCE_EXHAUSTED,
          Status.Code.UNAVAILABLE,
        )

  private fun Throwable.statusCode(): Status.Code? =
    when (this) {
      is StatusException -> status.code
      is StatusRuntimeException -> status.code
      else -> null
    }

  companion object {
    private val ACTIVE_STATES =
      listOf(
        UploadHealingOperation.State.APPROVED,
        UploadHealingOperation.State.DRAINING,
        UploadHealingOperation.State.EVICTING,
        UploadHealingOperation.State.REPLAYING,
        UploadHealingOperation.State.RECOVERING,
      )
    private val NONTERMINAL_STATES =
      ACTIVE_STATES +
        listOf(
          UploadHealingOperation.State.APPROVAL_REQUIRED,
          UploadHealingOperation.State.NEEDS_ATTENTION,
        )
    private const val MAX_TRANSITIONS_PER_RUN = 16
    private val logger = Logger.getLogger(VidLabelingHealingController::class.java.name)
  }

  private class ManifestMismatchException(message: String) : IllegalStateException(message)

  private class ReplacementFailedException(message: String) : IllegalStateException(message)
}
