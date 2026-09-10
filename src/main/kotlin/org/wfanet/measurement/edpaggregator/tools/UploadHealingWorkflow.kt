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

package org.wfanet.measurement.edpaggregator.tools

import com.google.protobuf.util.Timestamps
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.UploadHealingOperationKey
import org.wfanet.measurement.edpaggregator.service.UploadHealingStepKey
import org.wfanet.measurement.edpaggregator.v1alpha.AdvanceUploadHealingStepRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperationServiceGrpcKt.UploadHealingOperationServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.advanceUploadHealingStepRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getUploadHealingOperationRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.uploadHealingStep
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds

/** Executes an eviction while reporting each durable per-entry checkpoint. */
fun interface EvictionExecutor {
  suspend fun evict(
    plan: EvictUploader.EvictionPlan,
    reason: String,
    onEntryEvicted: suspend (EvictUploader.CascadeEntry) -> Unit,
  ): EvictUploader.EvictionResult
}

/** Starts or resumes one memoized recovery upload. */
fun interface RecoveryExecutor {
  suspend fun recover(
    sourceUploadName: String,
    cmmsModelLines: List<String>,
  ): RecoverUploader.Result
}

/** Executes and resumes a persisted upload-eviction and replacement workflow. */
class UploadHealingWorkflow(
  private val operationsStub: UploadHealingOperationServiceCoroutineStub,
  private val uploadsStub: RawImpressionUploadServiceCoroutineStub,
  private val modelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val rankIndexBlobsStub: RankIndexBlobServiceCoroutineStub,
  private val evictionExecutor: EvictionExecutor,
  private val recoveryExecutor: RecoveryExecutor,
) {
  data class Progress(
    val operation: UploadHealingOperation,
    val nextAction: String,
    val evictionResult: EvictUploader.EvictionResult? = null,
  )

  /** Persists [plan] before its first mutation, then advances it as far as currently possible. */
  suspend fun start(
    plan: EvictUploader.EvictionPlan,
    reason: String,
    labeledImpressionsBlobPrefix: String,
  ): Progress {
    val dataProviderName = dataProviderOf(plan.badUploads.first())
    val operationId = plan.evictionOperationId
    val operatorRecoveryTargets =
      plan.recoveryTargets
        .flatMap { target -> target.cmmsModelLines.map { target.uploadName to it } }
        .toSet()
    val operation = uploadHealingOperation {
      this.reason = reason
      this.labeledImpressionsBlobPrefix = labeledImpressionsBlobPrefix
      badRawImpressionUploads += plan.badUploads
      cutoffTime = plan.cutoffTime.toProtoTime()
      steps +=
        plan.cascade.mapIndexed { index, entry ->
          uploadHealingStep {
            sequenceNumber = index.toLong()
            sourceRawImpressionUpload = entry.uploadName
            rawImpressionUploadModelLine = entry.modelLineName
            cmmsModelLine = entry.cmmsModelLine
            memoized = entry.memoized
            recoveryAction = entry.recoveryAction
            recoveryPredecessorRawImpressionUpload = entry.recoveryPredecessorUploadName
            recoveryTarget =
              entry.recoveryAction ==
                RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION ||
                (entry.uploadName to entry.cmmsModelLine) in operatorRecoveryTargets
          }
        }
    }
    val created =
      operationsStub.createUploadHealingOperation(
        createUploadHealingOperationRequest {
          parent = dataProviderName
          uploadHealingOperation = operation
          uploadHealingOperationId = operationId
          requestId = RequestIds.forUploadHealingOperation(operationId)
        }
      )
    return resume(created.name)
  }

  /** Advances [operationName] until it must wait for an external replacement or worker result. */
  suspend fun resume(operationName: String): Progress {
    var operation = getOperation(operationName)
    var evictionResult: EvictUploader.EvictionResult? = null
    if (operation.state == UploadHealingOperation.State.COMPLETE) {
      return Progress(operation, "Healing operation ${operation.name} is complete.")
    }

    if (operation.stepsList.any { it.state == UploadHealingStep.State.PENDING_EVICTION }) {
      val plan = operation.toEvictionPlan()
      evictionResult =
        evictionExecutor.evict(plan, operation.reason) { entry ->
          val step =
            operation.stepsList.single { it.rawImpressionUploadModelLine == entry.modelLineName }
          if (step.state == UploadHealingStep.State.PENDING_EVICTION) {
            operation =
              checkpoint(
                step,
                if (step.recoveryTarget) {
                  UploadHealingStep.State.WAITING_FOR_REPLACEMENT
                } else {
                  UploadHealingStep.State.COMPLETE
                },
              )
          }
        }
      operation = getOperation(operationName)
    }

    val groups =
      operation.stepsList
        .filter { it.recoveryTarget }
        .groupBy { it.sourceRawImpressionUpload to it.recoveryAction }
        .values
        .sortedBy { steps -> steps.minOf { it.sequenceNumber } }
    for (initialGroup in groups) {
      var group =
        operation.stepsList.filter {
          it.sourceRawImpressionUpload == initialGroup.first().sourceRawImpressionUpload &&
            it.recoveryAction == initialGroup.first().recoveryAction &&
            it.recoveryTarget
        }
      if (group.all { it.state == UploadHealingStep.State.COMPLETE }) continue

      val readyReplacement = findReadyReplacement(group)
      if (readyReplacement != null) {
        for (step in group.filter { it.state != UploadHealingStep.State.COMPLETE }) {
          operation = checkpoint(step, UploadHealingStep.State.COMPLETE, readyReplacement.name)
        }
        continue
      }

      val source = group.first().sourceRawImpressionUpload
      val modelLines = group.map { it.cmmsModelLine }.distinct()
      when (group.first().recoveryAction) {
        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION -> {
          return Progress(
            operation,
            "Wait for the data provider to correct $source and for its replacement to complete " +
              "for $modelLines, then rerun resume for ${operation.name}.",
            evictionResult,
          )
        }
        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY -> {
          if (group.all { it.state == UploadHealingStep.State.WAITING_FOR_REPLACEMENT }) {
            val recovery = recoveryExecutor.recover(source, modelLines)
            for (step in group) {
              operation =
                checkpoint(
                  step,
                  UploadHealingStep.State.RECOVERY_STARTED,
                  recoveryDoneBlobGeneration = recovery.doneBlobGeneration,
                )
            }
          } else {
            val startedGeneration =
              group.firstOrNull { it.recoveryDoneBlobGeneration > 0L }?.recoveryDoneBlobGeneration
                ?: error("Recovery for $source started without a done-object generation")
            for (step in
              group.filter { it.state == UploadHealingStep.State.WAITING_FOR_REPLACEMENT }) {
              operation =
                checkpoint(
                  step,
                  UploadHealingStep.State.RECOVERY_STARTED,
                  recoveryDoneBlobGeneration = startedGeneration,
                )
            }
          }
          group =
            operation.stepsList.filter {
              it.sourceRawImpressionUpload == source &&
                it.recoveryAction == initialGroup.first().recoveryAction &&
                it.recoveryTarget
            }
          return Progress(
            operation,
            "Recovery for $source has started for ${group.map { it.cmmsModelLine }}. Wait for " +
              "the replacement upload to complete, then rerun resume for ${operation.name}.",
            evictionResult,
          )
        }
        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_UNSPECIFIED,
        RawImpressionUploadModelLine.RecoveryAction.UNRECOGNIZED ->
          error("Healing step for $source has no recovery action")
      }
    }

    operation = getOperation(operationName)
    check(operation.state == UploadHealingOperation.State.COMPLETE) {
      "All healing steps are complete but ${operation.name} is still ${operation.state}"
    }
    return Progress(operation, "Healing operation ${operation.name} is complete.", evictionResult)
  }

  private suspend fun checkpoint(
    step: UploadHealingStep,
    state: UploadHealingStep.State,
    replacementUploadName: String = "",
    recoveryDoneBlobGeneration: Long = step.recoveryDoneBlobGeneration,
  ): UploadHealingOperation {
    val action =
      when (state) {
        UploadHealingStep.State.WAITING_FOR_REPLACEMENT ->
          AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION
        UploadHealingStep.State.RECOVERY_STARTED ->
          AdvanceUploadHealingStepRequest.Action.RECORD_RECOVERY
        UploadHealingStep.State.COMPLETE ->
          if (replacementUploadName.isEmpty()) {
            AdvanceUploadHealingStepRequest.Action.CONFIRM_EVICTION
          } else {
            AdvanceUploadHealingStepRequest.Action.CONFIRM_REPLACEMENT
          }
        UploadHealingStep.State.PENDING_EVICTION,
        UploadHealingStep.State.STATE_UNSPECIFIED,
        UploadHealingStep.State.UNRECOGNIZED -> error("Unsupported checkpoint state: $state")
      }
    operationsStub.advanceUploadHealingStep(
      advanceUploadHealingStepRequest {
        name = step.name
        etag = step.etag
        this.action = action
        if (replacementUploadName.isNotEmpty()) {
          replacementRawImpressionUpload = replacementUploadName
        }
        if (recoveryDoneBlobGeneration > 0L) {
          this.recoveryDoneBlobGeneration = recoveryDoneBlobGeneration
        }
        requestId =
          RequestIds.forUploadHealingStep(
            step.name,
            state.name,
            "$replacementUploadName:$recoveryDoneBlobGeneration",
          )
      }
    )
    val operationName =
      requireNotNull(UploadHealingStepKey.fromName(step.name)) {
          "Malformed UploadHealingStep resource name: ${step.name}"
        }
        .parentKey
        .toName()
    return getOperation(operationName)
  }

  private suspend fun getOperation(name: String): UploadHealingOperation =
    operationsStub.getUploadHealingOperation(getUploadHealingOperationRequest { this.name = name })

  private fun UploadHealingOperation.toEvictionPlan(): EvictUploader.EvictionPlan {
    val operationId =
      requireNotNull(UploadHealingOperationKey.fromName(name)) {
          "Malformed UploadHealingOperation resource name: $name"
        }
        .uploadHealingOperationId
    val cascade =
      stepsList
        .sortedBy { it.sequenceNumber }
        .map { step ->
          EvictUploader.CascadeEntry(
            uploadName = step.sourceRawImpressionUpload,
            modelLineName = step.rawImpressionUploadModelLine,
            cmmsModelLine = step.cmmsModelLine,
            memoized = step.memoized,
            recoveryAction = step.recoveryAction,
            recoveryPredecessorUploadName = step.recoveryPredecessorRawImpressionUpload,
          )
        }
    val badUploadSet = badRawImpressionUploadsList.toSet()
    val recoveryTargets =
      stepsList
        .filter {
          it.recoveryTarget &&
            it.recoveryAction ==
              RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
        }
        .groupBy { it.sourceRawImpressionUpload }
        .map { (uploadName, steps) ->
          EvictUploader.RecoveryTarget(uploadName, steps.map { it.cmmsModelLine }.distinct())
        }
    return EvictUploader.EvictionPlan(
      cascade = cascade,
      extraUploads = cascade.map { it.uploadName }.filter { it !in badUploadSet }.distinct(),
      memoizedModelLines =
        cascade.filter { it.memoized }.mapTo(mutableSetOf()) { it.cmmsModelLine },
      nonMemoizedModelLines =
        cascade.filterNot { it.memoized }.mapTo(mutableSetOf()) { it.cmmsModelLine },
      badUploads = badRawImpressionUploadsList,
      cutoffTime = cutoffTime.toInstant(),
      evictionOperationId = operationId,
      recoveryTargets = recoveryTargets,
    )
  }

  private suspend fun findReadyReplacement(steps: List<UploadHealingStep>): RawImpressionUpload? {
    val sourceName = steps.first().sourceRawImpressionUpload
    val source =
      uploadsStub.getRawImpressionUpload(getRawImpressionUploadRequest { name = sourceName })
    val sourceKey = requireNotNull(RawImpressionUploadKey.fromName(sourceName))
    val revisions = listUploads(sourceKey.parentKey.toName(), source.doneBlobUri)
    val latest = findLatestUpload(revisions) ?: return null
    if (latest.name == sourceName || !replacesUpload(latest.name, sourceName, revisions))
      return null
    if (!isRegistrationComplete(latest) || latest.state != RawImpressionUpload.State.COMPLETED) {
      return null
    }
    val rows = listModelLines(latest.name).associateBy { it.cmmsModelLine }
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
            ListRankIndexBlobsRequestKt.filter {
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

  private fun isRegistrationComplete(upload: RawImpressionUpload): Boolean =
    upload.registrationComplete || upload.state != RawImpressionUpload.State.CREATED

  private fun dataProviderOf(uploadName: String): String =
    requireNotNull(RawImpressionUploadKey.fromName(uploadName)) {
        "Malformed RawImpressionUpload resource name: $uploadName"
      }
      .parentKey
      .toName()
}
