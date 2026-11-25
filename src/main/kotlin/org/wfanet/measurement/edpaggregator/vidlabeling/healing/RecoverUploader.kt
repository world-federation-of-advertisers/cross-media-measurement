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

import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.vidlabeling.RawImpressionUploadRevisions
import org.wfanet.measurement.edpaggregator.vidlabeling.RecoveryPredecessorResolver
import org.wfanet.measurement.edpaggregator.vidlabeling.RecoveryPredecessorResolver.FailureReason as RecoveryPredecessorFailure
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingMetadataReader
import org.wfanet.measurement.securecomputation.datawatcher.WatchedBlobs

/** Starts or resumes one memoized recovery upload. */
fun interface RecoveryExecutor {
  suspend fun recover(
    sourceUploadName: String,
    cmmsModelLines: List<String>,
  ): RecoverUploader.Result
}

/**
 * Registers a fresh done-object generation for an evicted upload whose model lines are marked
 * `RECOVERY_ACTION_OPERATOR_RECOVERY`, on the memoized path only.
 */
class RecoverUploader(
  private val uploadsStub: RawImpressionUploadServiceCoroutineStub,
  private val modelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val rankIndexBlobsStub: RankIndexBlobServiceCoroutineStub,
  private val rpcThrottlers: VidLabelingRpcThrottlers,
  private val getHealingOperation: suspend (name: String) -> UploadHealingOperation? = { null },
  private val rewriteDoneBlob:
    suspend (doneBlobUri: String, expectedGeneration: Long, metadata: Map<String, String>) -> Long,
) : RecoveryExecutor {

  private val recoveryPredecessorResolver =
    RecoveryPredecessorResolver(
      getUpload = { name ->
        rpcThrottlers.metadataRead.onReady {
          uploadsStub.getRawImpressionUpload(getRawImpressionUploadRequest { this.name = name })
        }
      },
      listModelLines = ::listModelLines,
      listSnapshots = ::historicalSnapshots,
      hasActiveSnapshot = ::hasActiveSnapshot,
      getHealingOperation = getHealingOperation,
    )

  data class Result(
    val sourceUpload: String,
    val doneBlobUri: String,
    val doneBlobGeneration: Long,
    val modelLines: List<String>,
  )

  /**
   * Rewrites the source upload's done object so DataWatcher dispatches a new upload restricted to
   * [cmmsModelLines].
   */
  override suspend fun recover(sourceUploadName: String, cmmsModelLines: List<String>): Result {
    val sourceKey =
      requireNotNull(RawImpressionUploadKey.fromName(sourceUploadName)) {
        "Malformed RawImpressionUpload resource name: $sourceUploadName"
      }
    require(cmmsModelLines.isNotEmpty()) { "at least one model line is required" }
    require(cmmsModelLines.all { ModelLineKey.fromName(it) != null }) {
      "all model lines must be valid CMMS ModelLine resource names: $cmmsModelLines"
    }
    require(cmmsModelLines.distinct().size == cmmsModelLines.size) {
      "model lines must not contain duplicates: $cmmsModelLines"
    }

    val source =
      rpcThrottlers.metadataRead.onReady {
        uploadsStub.getRawImpressionUpload(
          getRawImpressionUploadRequest { name = sourceUploadName }
        )
      }
    require(source.doneBlobGeneration > 0L) {
      "$sourceUploadName does not have a valid done-object generation"
    }

    val latest = findLatestUploadByDoneBlob(sourceKey.parentKey.toName(), source.doneBlobUri)
    val isResumableIncompleteRecovery =
      latest != null &&
        latest.replacesRawImpressionUpload == source.name &&
        !isRegistrationComplete(latest)
    require(
      latest?.name == source.name ||
        isResumableIncompleteRecovery ||
        (latest != null &&
          latest.processingDeferred &&
          latest.state in
            setOf(RawImpressionUpload.State.CREATED, RawImpressionUpload.State.CORRECTION_REQUIRED))
    ) {
      "$sourceUploadName has been superseded by ${latest?.name}; recover the latest revision"
    }
    val rowsByCmmsModelLine = listModelLines(sourceUploadName).associateBy { it.cmmsModelLine }
    val missing = cmmsModelLines.filter { it !in rowsByCmmsModelLine }
    require(missing.isEmpty()) { "$sourceUploadName has no model-line rows for: $missing" }

    val notFailed =
      cmmsModelLines
        .map { rowsByCmmsModelLine.getValue(it) }
        .filter { it.state != RawImpressionUploadModelLine.State.FAILED }
    require(notFailed.isEmpty()) {
      "Recovery only accepts FAILED model-line rows: ${notFailed.map { it.name to it.state }}"
    }

    val requestedRows = cmmsModelLines.map { rowsByCmmsModelLine.getValue(it) }
    val notOperatorRecovery =
      requestedRows.filter {
        it.recoveryAction !=
          RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
      }
    require(notOperatorRecovery.isEmpty()) {
      "Recovery only accepts model lines marked for operator recovery: " +
        notOperatorRecovery.map { it.name to it.recoveryAction }
    }
    val evictionOperationIds = requestedRows.map { it.evictionOperationId }.toSet()
    require(evictionOperationIds.size == 1 && evictionOperationIds.single().isNotEmpty()) {
      "all requested model lines must belong to one eviction operation"
    }
    val operationName =
      sourceKey.parentKey.toName() + "/uploadHealingOperations/" + evictionOperationIds.single()
    val operation = getHealingOperation(operationName)
    val laterDeferredSource =
      latest != null &&
        latest.name != source.name &&
        latest.processingDeferred &&
        latest.state in
          setOf(RawImpressionUpload.State.CREATED, RawImpressionUpload.State.CORRECTION_REQUIRED) &&
        operation?.name == operationName &&
        operation.state == UploadHealingOperation.State.RECOVERING
    if (laterDeferredSource) {
      val planned =
        checkNotNull(operation).stepsList.filter {
          it.recoveryTarget && it.sourceRawImpressionUpload == source.name
        }
      require(
        planned.map { it.cmmsModelLine }.toSet() == cmmsModelLines.toSet() &&
          planned.all {
            it.state == UploadHealingStep.State.WAITING_FOR_REPLACEMENT &&
              it.recoveryAction ==
                RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
          }
      ) {
        "Deferred-source recovery does not match the approved operation"
      }
    }
    require(latest?.name == source.name || isResumableIncompleteRecovery || laterDeferredSource) {
      "$sourceUploadName has been superseded by ${latest?.name}; recover the latest revision"
    }
    for (row in requestedRows) {
      requireRecoveryPredecessorReady(sourceKey.parentKey.toName(), row)
    }

    val recoverableModelLines =
      rowsByCmmsModelLine.values
        .filter { it.state == RawImpressionUploadModelLine.State.FAILED }
        .filter {
          it.recoveryAction ==
            RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
        }
        .filter { hasDeletedSnapshotHistory(sourceUploadName, it.cmmsModelLine) }
        .mapTo(mutableSetOf()) { it.cmmsModelLine }
    require(cmmsModelLines.toSet() == recoverableModelLines) {
      "Recovery requires the complete set of FAILED memoized model lines whose snapshots " +
        "were deleted; requested=$cmmsModelLines, recoverable=$recoverableModelLines"
    }

    val metadata =
      mapOf(
        WatchedBlobs.OVERRIDE_MODEL_LINES_KEY to cmmsModelLines.joinToString(separator = ","),
        WatchedBlobs.RECOVERY_SOURCE_UPLOAD_KEY to source.name,
        WatchedBlobs.EVICTION_OPERATION_ID_KEY to evictionOperationIds.single(),
      )
    val generation =
      if (laterDeferredSource) source.doneBlobGeneration
      else rewriteDoneBlob(source.doneBlobUri, source.doneBlobGeneration, metadata)
    require(generation > 0L && (generation != source.doneBlobGeneration || laterDeferredSource)) {
      "rewriting ${source.doneBlobUri} did not create a distinct valid generation: $generation"
    }
    return Result(source.name, source.doneBlobUri, generation, cmmsModelLines)
  }

  private suspend fun findLatestUploadByDoneBlob(
    parent: String,
    doneBlobUri: String,
  ): RawImpressionUpload? {
    var pageToken = ""
    var latest: RawImpressionUpload? = null
    do {
      val response =
        VidLabelingMetadataReader.uploadRevisionsPage(
          uploadsStub,
          rpcThrottlers.metadataRead,
          parent,
          doneBlobUri,
          pageToken,
        )
      latest = RawImpressionUploadRevisions.latest(response.resources + listOfNotNull(latest))
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return latest
  }

  private fun isRegistrationComplete(upload: RawImpressionUpload): Boolean =
    upload.registrationComplete || upload.state != RawImpressionUpload.State.CREATED

  /** Requires the latest replacement of this row's predecessor to own a live completed snapshot. */
  private suspend fun requireRecoveryPredecessorReady(
    dataProviderName: String,
    row: RawImpressionUploadModelLine,
  ) {
    val predecessorName = row.recoveryPredecessorRawImpressionUpload
    if (predecessorName.isEmpty()) return
    val predecessor =
      rpcThrottlers.metadataRead.onReady {
        uploadsStub.getRawImpressionUpload(getRawImpressionUploadRequest { name = predecessorName })
      }
    val revisions = listUploads(dataProviderName, predecessor.doneBlobUri)
    val operationName = "$dataProviderName/uploadHealingOperations/" + row.evictionOperationId
    val operation = getHealingOperation(operationName)
    if (operation != null) {
      require(operation.name == operationName) { "Recovery operation identity does not match" }
      require(
        operation.state in
          setOf(UploadHealingOperation.State.REPLAYING, UploadHealingOperation.State.RECOVERING)
      ) {
        "Recovery operation is not executing its approved plan"
      }
      require(
        operation.stepsList.any {
          it.rawImpressionUploadModelLine == row.name &&
            it.cmmsModelLine == row.cmmsModelLine &&
            it.recoveryTarget &&
            it.recoveryAction ==
              RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
        }
      ) {
        "Recovery model line is not part of the active operation"
      }
    }
    val result =
      recoveryPredecessorResolver.resolve(predecessor, row, revisions, operationName, operation)
    if (result is RecoveryPredecessorResolver.Result.NotReady) {
      when (result.reason) {
        RecoveryPredecessorFailure.PLANNED_PREDECESSOR_INCOMPLETE ->
          throw IllegalArgumentException("The planned recovery predecessor has not completed")
        RecoveryPredecessorFailure.PLANNED_REPLACEMENT_MISSING ->
          error("Planned recovery predecessor replacement is missing")
        RecoveryPredecessorFailure.REPLACEMENT_OPERATION_MISMATCH ->
          throw IllegalArgumentException(
            "Planned predecessor replacement belongs to another operation"
          )
        RecoveryPredecessorFailure.NO_REVISION ->
          throw IllegalArgumentException(
            "No upload revision found for recovery predecessor $predecessorName"
          )
        RecoveryPredecessorFailure.NOT_A_REPLACEMENT ->
          throw IllegalArgumentException(
            "Latest upload ${result.latestUploadName} does not replace recovery predecessor $predecessorName"
          )
        RecoveryPredecessorFailure.OUTPUT_NOT_DURABLE ->
          throw IllegalArgumentException(
            "Recovery predecessor $predecessorName has not produced durable labeled output for ${row.cmmsModelLine}"
          )
        RecoveryPredecessorFailure.NO_LIVE_SNAPSHOT ->
          throw IllegalArgumentException(
            "Recovery predecessor $predecessorName has no live replacement snapshot for ${row.cmmsModelLine}"
          )
      }
    }
  }

  private suspend fun listUploads(parent: String, doneBlobUri: String): List<RawImpressionUpload> =
    VidLabelingMetadataReader.uploadRevisions(
      uploadsStub,
      rpcThrottlers.metadataRead,
      parent,
      doneBlobUri,
    )

  private suspend fun listModelLines(uploadName: String): List<RawImpressionUploadModelLine> =
    VidLabelingMetadataReader.uploadModelLines(
      modelLinesStub,
      rpcThrottlers.metadataRead,
      uploadName,
    )

  private suspend fun historicalSnapshots(
    uploadName: String,
    cmmsModelLine: String,
    showDeleted: Boolean,
  ): List<RankIndexBlob> =
    VidLabelingMetadataReader.snapshots(
      rankIndexBlobsStub,
      rpcThrottlers.metadataRead,
      uploadName,
      cmmsModelLine,
      showDeleted,
    )

  private suspend fun hasDeletedSnapshotHistory(
    uploadName: String,
    cmmsModelLine: String,
  ): Boolean =
    VidLabelingMetadataReader.hasDeletedSnapshotHistory(
      rankIndexBlobsStub,
      rpcThrottlers.metadataRead,
      uploadName,
      cmmsModelLine,
    )

  private suspend fun hasActiveSnapshot(uploadName: String, cmmsModelLine: String): Boolean =
    VidLabelingMetadataReader.hasActiveSnapshot(
      rankIndexBlobsStub,
      rpcThrottlers.metadataRead,
      uploadName,
      cmmsModelLine,
    )
}
