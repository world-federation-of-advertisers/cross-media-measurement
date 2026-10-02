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

import java.time.Instant
import java.util.UUID
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadCorrectionCandidateKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineRecoveryAction
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation
import org.wfanet.measurement.internal.edpaggregator.uploadHealingOperation
import org.wfanet.measurement.internal.edpaggregator.uploadHealingStep

/** Builds one durable healing plan for each DataProvider with pending candidates. */
class RawImpressionUploadCorrectionPlanner(
  private val planCorrection: suspend (List<String>, Instant, String) -> EvictUploader.EvictionPlan,
  private val operationIdGenerator: () -> String = { UUID.randomUUID().toString() },
) {
  /** A historical upload whose derived output is affected by a candidate. */
  data class HistoricalOwner(val rawImpressionUpload: String, val createTime: Instant)

  /** A correction candidate with its affected historical upload owners. */
  data class Candidate(
    val name: String,
    val createTime: Instant,
    val historicalOwners: List<HistoricalOwner>,
    val supersedingRevisions: Set<String> = emptySet(),
  )

  /** Builds deterministic, DataProvider-scoped plans from [candidates]. */
  suspend fun plan(
    candidates: Collection<Candidate>,
    cutoffTime: Instant,
    operationIdsByDataProvider: Map<String, String> = emptyMap(),
  ): List<UploadHealingOperation> =
    candidates
      .groupBy { candidate -> candidateKey(candidate).parentKey.toName() }
      .toSortedMap()
      .map { (dataProviderName, groupedCandidates) ->
        buildPlan(
          dataProviderName,
          groupedCandidates,
          cutoffTime,
          operationIdsByDataProvider[dataProviderName],
        )
      }

  private suspend fun buildPlan(
    dataProviderName: String,
    candidates: List<Candidate>,
    cutoffTime: Instant,
    existingOperationId: String?,
  ): UploadHealingOperation {
    require(candidates.isNotEmpty())
    val dataProviderKey = requireNotNull(DataProviderKey.fromName(dataProviderName))
    val orderedCandidates =
      candidates
        .distinctBy { it.name }
        .sortedWith(compareBy<Candidate> { it.createTime }.thenBy { it.name })
    val owners =
      orderedCandidates
        .flatMap { it.historicalOwners }
        .groupBy { it.rawImpressionUpload }
        .map { (name, values) -> HistoricalOwner(name, values.minOf { it.createTime }) }
        .sortedWith(compareBy<HistoricalOwner> { it.createTime }.thenBy { it.rawImpressionUpload })
    require(owners.isNotEmpty()) { "correction candidates must identify historical owners" }
    require(
      owners.all {
        requireNotNull(RawImpressionUploadKey.fromName(it.rawImpressionUpload)).parentKey ==
          dataProviderKey
      }
    ) {
      "historical owners must belong to their candidate DataProvider"
    }
    val operationId = existingOperationId ?: operationIdGenerator()
    require(runCatching { UUID.fromString(operationId) }.isSuccess) {
      "operationIdGenerator must return a UUID"
    }
    val candidateIds =
      orderedCandidates.map { candidateKey(it).rawImpressionUploadCorrectionCandidateId }
    val candidateIdByAssociatedUpload = buildMap {
      for (candidate in orderedCandidates) {
        val candidateId = candidateKey(candidate).rawImpressionUploadCorrectionCandidateId
        val associatedUploads =
          candidate.historicalOwners.map { it.rawImpressionUpload } + candidate.supersedingRevisions
        for (uploadName in associatedUploads.toSet()) {
          require(put(uploadName, candidateId) == null) {
            "upload $uploadName belongs to multiple candidates"
          }
        }
      }
    }
    if (owners.any { it.createTime < cutoffTime }) {
      return uploadHealingOperation {
        dataProviderResourceId = dataProviderKey.dataProviderId
        uploadHealingOperationId = operationId
        state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION
        reason = OUT_OF_RETENTION_REASON
        rawImpressionUploadCorrectionCandidateIds += candidateIds
      }
    }

    val ownerNames = owners.map { it.rawImpressionUpload }
    val evictionPlan = planCorrection(ownerNames, cutoffTime, operationId)
    val recoveryTargets =
      (evictionPlan.recoveryTargets + evictionPlan.replacementTargets)
        .flatMap { target -> target.cmmsModelLines.map { target.uploadName to it } }
        .toSet()
    val cascade = evictionPlan.cascade.distinctBy { it.uploadName to it.modelLineName }
    return uploadHealingOperation {
      dataProviderResourceId = dataProviderKey.dataProviderId
      uploadHealingOperationId = operationId
      state = UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED
      reason = DEFAULT_REASON
      rawImpressionUploadCorrectionCandidateIds += candidateIds
      steps +=
        cascade.mapIndexed { index, entry ->
          uploadHealingStep {
            uploadHealingStepId = index.toLong() + 1L
            sequenceNumber = index.toLong()
            sourceRawImpressionUploadResourceId = uploadId(entry.uploadName)
            rawImpressionUploadModelLineResourceId =
              requireNotNull(
                  org.wfanet.measurement.edpaggregator.service.RawImpressionUploadModelLineKey
                    .fromName(entry.modelLineName)
                )
                .rawImpressionUploadModelLineId
            cmmsModelLine = entry.cmmsModelLine
            memoized = entry.memoized
            recoveryAction = entry.recoveryAction.toInternal()
            if (entry.recoveryPredecessorUploadName.isNotEmpty()) {
              recoveryPredecessorRawImpressionUploadResourceId =
                uploadId(entry.recoveryPredecessorUploadName)
            }
            recoveryTarget = entry.uploadName to entry.cmmsModelLine in recoveryTargets
            candidateIdByAssociatedUpload[entry.uploadName]?.let {
              rawImpressionUploadCorrectionCandidateId = it
            }
          }
        }
    }
  }

  private fun candidateKey(candidate: Candidate): RawImpressionUploadCorrectionCandidateKey =
    requireNotNull(RawImpressionUploadCorrectionCandidateKey.fromName(candidate.name)) {
      "Malformed RawImpressionUploadCorrectionCandidate name: ${candidate.name}"
    }

  private fun uploadId(name: String): String =
    requireNotNull(RawImpressionUploadKey.fromName(name)) {
        "Malformed RawImpressionUpload name: $name"
      }
      .rawImpressionUploadId

  private fun RawImpressionUploadModelLine.RecoveryAction.toInternal() =
    when (this) {
      RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION ->
        RawImpressionUploadModelLineRecoveryAction
          .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_EDP_CORRECTION
      RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY ->
        RawImpressionUploadModelLineRecoveryAction
          .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_OPERATOR_RECOVERY
      RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_NO_REPLACEMENT ->
        RawImpressionUploadModelLineRecoveryAction
          .RAW_IMPRESSION_UPLOAD_MODEL_LINE_RECOVERY_ACTION_NO_REPLACEMENT
      RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_UNSPECIFIED,
      RawImpressionUploadModelLine.RecoveryAction.UNRECOGNIZED ->
        error("recovery action is required")
    }

  companion object {
    private const val DEFAULT_REASON = "Raw-impression upload correction"
    private const val OUT_OF_RETENTION_REASON =
      "Affected upload is outside the healing retention window"
  }
}
