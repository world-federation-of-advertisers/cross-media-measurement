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

package org.wfanet.measurement.edpaggregator.vidlabeling

import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingStep

/** Resolves recovery predecessor evidence without choosing the caller's authorization or errors. */
class RecoveryPredecessorResolver(
  private val getUpload: suspend (String) -> RawImpressionUpload,
  private val listModelLines: suspend (String) -> List<RawImpressionUploadModelLine>,
  private val listSnapshots: suspend (String, String, Boolean) -> List<RankIndexBlob>,
  private val hasActiveSnapshot: suspend (String, String) -> Boolean,
  private val getHealingOperation: suspend (String) -> UploadHealingOperation?,
) {
  enum class FailureReason {
    PLANNED_PREDECESSOR_INCOMPLETE,
    PLANNED_REPLACEMENT_MISSING,
    REPLACEMENT_OPERATION_MISMATCH,
    NO_REVISION,
    NOT_A_REPLACEMENT,
    OUTPUT_NOT_DURABLE,
    NO_LIVE_SNAPSHOT,
  }

  sealed interface Result {
    data class Ready(val upload: RawImpressionUpload) : Result

    data class NotReady(val reason: FailureReason, val latestUploadName: String = "") : Result
  }

  /** Resolves a predecessor after the caller validates [operation]'s identity and authorization. */
  suspend fun resolve(
    predecessor: RawImpressionUpload,
    row: RawImpressionUploadModelLine,
    revisions: List<RawImpressionUpload>,
    operationName: String,
    operation: UploadHealingOperation?,
  ): Result {
    val predecessorName = row.recoveryPredecessorRawImpressionUpload
    val plannedPredecessor =
      operation?.stepsList?.singleOrNull {
        it.recoveryTarget &&
          it.sourceRawImpressionUpload == predecessorName &&
          it.cmmsModelLine == row.cmmsModelLine &&
          it.state == UploadHealingStep.State.COMPLETE
      }
    if (
      operation?.stepsList?.any {
        it.sourceRawImpressionUpload == predecessorName && it.cmmsModelLine == row.cmmsModelLine
      } == true && plannedPredecessor == null
    ) {
      return Result.NotReady(FailureReason.PLANNED_PREDECESSOR_INCOMPLETE)
    }
    val historicalPredecessor = resolveHistoricalPredecessor(predecessor, row, operation)
    val plannedReplacementName = plannedPredecessor?.replacementRawImpressionUpload.orEmpty()
    val latest =
      if (plannedReplacementName.isNotEmpty()) {
        val replacement =
          revisions.singleOrNull { it.name == plannedReplacementName }
            ?: return Result.NotReady(FailureReason.PLANNED_REPLACEMENT_MISSING)
        if (replacement.uploadHealingOperation != operationName) {
          return Result.NotReady(FailureReason.REPLACEMENT_OPERATION_MISMATCH)
        }
        replacement
      } else {
        historicalPredecessor
          ?: RawImpressionUploadRevisions.latest(
            if (operation != null) {
              revisions.filter {
                it.registrationComplete && it.state == RawImpressionUpload.State.COMPLETED
              }
            } else {
              revisions
            }
          )
          ?: return Result.NotReady(FailureReason.NO_REVISION)
      }
    if (
      latest.name != predecessorName &&
        !RawImpressionUploadRevisions.replaces(latest.name, predecessorName, revisions)
    ) {
      return Result.NotReady(FailureReason.NOT_A_REPLACEMENT, latest.name)
    }
    val replacementRow =
      listModelLines(latest.name).firstOrNull { it.cmmsModelLine == row.cmmsModelLine }
    if (
      replacementRow?.state !in
        setOf(
          RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING,
          RawImpressionUploadModelLine.State.COMPLETED,
        )
    ) {
      return Result.NotReady(FailureReason.OUTPUT_NOT_DURABLE)
    }
    if (!hasActiveSnapshot(latest.name, row.cmmsModelLine)) {
      return Result.NotReady(FailureReason.NO_LIVE_SNAPSHOT)
    }
    return Result.Ready(latest)
  }

  private suspend fun resolveHistoricalPredecessor(
    predecessor: RawImpressionUpload,
    row: RawImpressionUploadModelLine,
    operation: UploadHealingOperation?,
  ): RawImpressionUpload? {
    if (
      operation == null ||
        operation.state !in
          setOf(UploadHealingOperation.State.REPLAYING, UploadHealingOperation.State.RECOVERING) ||
        !predecessor.registrationComplete
    )
      return null
    if (
      operation.stepsList.any {
        it.sourceRawImpressionUpload == predecessor.name && it.cmmsModelLine == row.cmmsModelLine
      }
    )
      return null
    val target =
      operation.stepsList.singleOrNull {
        it.rawImpressionUploadModelLine == row.name &&
          it.cmmsModelLine == row.cmmsModelLine &&
          it.recoveryTarget &&
          it.memoized &&
          it.recoveryAction == row.recoveryAction
      } ?: return null
    val predecessorRow =
      listModelLines(predecessor.name).singleOrNull { it.cmmsModelLine == row.cmmsModelLine }
        ?: return null
    if (predecessorRow.state != RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING) return null
    val priorName = predecessor.uploadHealingOperation
    if (priorName.isNotEmpty()) {
      val prior = getHealingOperation(priorName) ?: return null
      if (prior.name != priorName || prior.state != UploadHealingOperation.State.COMPLETE)
        return null
    }
    val source = getUpload(target.sourceRawImpressionUpload)
    return if (
      HistoricalAvailabilitySnapshots.hasHistoricalBasis(
        source,
        listSnapshots(source.name, row.cmmsModelLine, true),
        listSnapshots(predecessor.name, row.cmmsModelLine, false),
      )
    )
      predecessor
    else null
  }
}
