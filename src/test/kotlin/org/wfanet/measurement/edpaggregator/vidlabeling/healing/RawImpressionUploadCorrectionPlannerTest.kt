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

import com.google.common.truth.Truth.assertThat
import java.time.Instant
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.internal.edpaggregator.UploadHealingOperation

@RunWith(JUnit4::class)
class RawImpressionUploadCorrectionPlannerTest {
  @Test
  fun `plan combines D2 and D4 candidates and deduplicates mixed model-line cascade`() =
    runBlocking<Unit> {
      var plannedBadUploads: List<String> = emptyList()
      val planner =
        RawImpressionUploadCorrectionPlanner(
          planCorrection = { badUploads, cutoffTime, operationId ->
            plannedBadUploads = badUploads
            EvictUploader.EvictionPlan(
              cascade =
                listOf(
                  entry(D2, DIRECT_MODEL_LINE, memoized = false),
                  entry(D2, MEMOIZED_MODEL_LINE, memoized = true),
                  entry(D3, MEMOIZED_MODEL_LINE, memoized = true),
                  entry(D4, DIRECT_MODEL_LINE, memoized = false),
                  entry(D4, MEMOIZED_MODEL_LINE, memoized = true),
                  entry(D4, MEMOIZED_MODEL_LINE, memoized = true),
                  entry(D5, MEMOIZED_MODEL_LINE, memoized = true),
                ),
              extraUploads = listOf(D3, D5),
              memoizedModelLines = setOf(MEMOIZED_MODEL_LINE),
              nonMemoizedModelLines = setOf(DIRECT_MODEL_LINE),
              badUploads = badUploads,
              noReplacementUploads = emptySet(),
              cutoffTime = cutoffTime,
              evictionOperationId = operationId,
              recoveryTargets =
                listOf(
                  EvictUploader.RecoveryTarget(D3, listOf(MEMOIZED_MODEL_LINE)),
                  EvictUploader.RecoveryTarget(D5, listOf(MEMOIZED_MODEL_LINE)),
                ),
              replacementTargets =
                listOf(
                  EvictUploader.RecoveryTarget(D2, listOf(DIRECT_MODEL_LINE, MEMOIZED_MODEL_LINE)),
                  EvictUploader.RecoveryTarget(D4, listOf(DIRECT_MODEL_LINE, MEMOIZED_MODEL_LINE)),
                ),
            )
          },
          operationIdGenerator = { OPERATION_ID },
        )

      val operations =
        planner.plan(
          listOf(candidate(C4, 4, D4), candidate(C2, 2, D2), candidate(C2, 2, D2)),
          CUTOFF,
        )

      val operation = operations.single()
      assertThat(plannedBadUploads).containsExactly(D2, D4).inOrder()
      assertThat(operation.state)
        .isEqualTo(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_APPROVAL_REQUIRED)
      assertThat(operation.rawImpressionUploadCorrectionCandidateIdsList)
        .containsExactly("candidate-d2", "candidate-d4")
        .inOrder()
      assertThat(
          operation.stepsList.map {
            Triple(it.sourceRawImpressionUploadResourceId, it.cmmsModelLine, it.recoveryTarget)
          }
        )
        .containsExactly(
          Triple("d2", DIRECT_MODEL_LINE, true),
          Triple("d2", MEMOIZED_MODEL_LINE, true),
          Triple("d3", MEMOIZED_MODEL_LINE, true),
          Triple("d4", DIRECT_MODEL_LINE, true),
          Triple("d4", MEMOIZED_MODEL_LINE, true),
          Triple("d5", MEMOIZED_MODEL_LINE, true),
        )
        .inOrder()
      assertThat(operation.stepsList.map { it.sequenceNumber })
        .containsExactly(0L, 1L, 2L, 3L, 4L, 5L)
        .inOrder()
      assertThat(operation.stepsList.map { it.rawImpressionUploadCorrectionCandidateId })
        .containsExactly("candidate-d2", "candidate-d2", "", "candidate-d4", "candidate-d4", "")
        .inOrder()
    }

  @Test
  fun `plan groups candidates by DataProvider`() =
    runBlocking<Unit> {
      val plannedGroups = mutableListOf<List<String>>()
      val planner =
        RawImpressionUploadCorrectionPlanner(
          planCorrection = { badUploads, cutoffTime, operationId ->
            plannedGroups += badUploads
            emptyPlan(badUploads, cutoffTime, operationId)
          },
          operationIdGenerator = { OPERATION_ID },
        )

      val operations =
        planner.plan(
          listOf(
            candidate(C2, 2, D2),
            candidate(
              "$OTHER_DATA_PROVIDER/rawImpressionUploadCorrectionCandidates/candidate",
              2,
              "$OTHER_DATA_PROVIDER/rawImpressionUploads/d2",
            ),
          ),
          CUTOFF,
        )

      assertThat(operations).hasSize(2)
      assertThat(plannedGroups).hasSize(2)
    }

  @Test
  fun `plan associates a candidate with its latest replacement target`() =
    runBlocking<Unit> {
      val planner =
        RawImpressionUploadCorrectionPlanner(
          planCorrection = { badUploads, cutoffTime, operationId ->
            EvictUploader.EvictionPlan(
              cascade =
                listOf(
                  entry(D2, DIRECT_MODEL_LINE, memoized = false),
                  entry(D3, DIRECT_MODEL_LINE, memoized = false)
                    .copy(
                      recoveryAction =
                        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION
                    ),
                  entry(D4, DIRECT_MODEL_LINE, memoized = false)
                    .copy(
                      recoveryAction =
                        RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION
                    ),
                ),
              extraUploads = listOf(D3, D4),
              memoizedModelLines = emptySet(),
              nonMemoizedModelLines = setOf(DIRECT_MODEL_LINE),
              badUploads = badUploads,
              noReplacementUploads = emptySet(),
              cutoffTime = cutoffTime,
              evictionOperationId = operationId,
              recoveryTargets = emptyList(),
              replacementTargets =
                listOf(EvictUploader.RecoveryTarget(D4, listOf(DIRECT_MODEL_LINE))),
            )
          },
          operationIdGenerator = { OPERATION_ID },
        )

      val operation =
        planner
          .plan(listOf(candidate(C2, 2, D2).copy(supersedingRevisions = setOf(D3, D4))), CUTOFF)
          .single()

      assertThat(operation.stepsList.map { it.rawImpressionUploadCorrectionCandidateId })
        .containsExactly("candidate-d2", "candidate-d2", "candidate-d2")
    }

  @Test
  fun `plan marks out-of-retention candidates as needing attention`() =
    runBlocking<Unit> {
      var plannerCalled = false
      val planner =
        RawImpressionUploadCorrectionPlanner(
          planCorrection = { badUploads, cutoffTime, operationId ->
            plannerCalled = true
            emptyPlan(badUploads, cutoffTime, operationId)
          },
          operationIdGenerator = { OPERATION_ID },
        )
      val oldOwner =
        RawImpressionUploadCorrectionPlanner.HistoricalOwner(D2, CUTOFF.minusSeconds(1L))

      val operation = planner.plan(listOf(candidate(C2, 2, oldOwner)), CUTOFF).single()

      assertThat(plannerCalled).isFalse()
      assertThat(operation.state)
        .isEqualTo(UploadHealingOperation.State.UPLOAD_HEALING_OPERATION_STATE_NEEDS_ATTENTION)
      assertThat(operation.reason).contains("outside the healing retention window")
      assertThat(operation.stepsList).isEmpty()
    }

  private fun candidate(
    name: String,
    order: Long,
    owner: String,
  ): RawImpressionUploadCorrectionPlanner.Candidate =
    candidate(
      name,
      order,
      RawImpressionUploadCorrectionPlanner.HistoricalOwner(owner, Instant.ofEpochSecond(order)),
    )

  private fun candidate(
    name: String,
    order: Long,
    owner: RawImpressionUploadCorrectionPlanner.HistoricalOwner,
  ) =
    RawImpressionUploadCorrectionPlanner.Candidate(
      name,
      Instant.ofEpochSecond(order),
      listOf(owner),
    )

  private fun entry(upload: String, modelLine: String, memoized: Boolean) =
    EvictUploader.CascadeEntry(
      uploadName = upload,
      modelLineName = "$upload/rawImpressionUploadModelLines/${modelLine.substringAfterLast('/')}",
      cmmsModelLine = modelLine,
      memoized = memoized,
      recoveryAction =
        if (upload == D2 || upload == D4) {
          RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_EDP_CORRECTION
        } else {
          RawImpressionUploadModelLine.RecoveryAction.RECOVERY_ACTION_OPERATOR_RECOVERY
        },
      recoveryPredecessorUploadName = "",
    )

  private fun emptyPlan(badUploads: List<String>, cutoffTime: Instant, operationId: String) =
    EvictUploader.EvictionPlan(
      cascade = badUploads.map { upload -> entry(upload, DIRECT_MODEL_LINE, memoized = false) },
      extraUploads = emptyList(),
      memoizedModelLines = emptySet(),
      nonMemoizedModelLines = setOf(DIRECT_MODEL_LINE),
      badUploads = badUploads,
      noReplacementUploads = emptySet(),
      cutoffTime = cutoffTime,
      evictionOperationId = operationId,
      recoveryTargets = emptyList(),
    )

  companion object {
    private const val DATA_PROVIDER = "dataProviders/dp"
    private const val OTHER_DATA_PROVIDER = "dataProviders/other"
    private const val D2 = "$DATA_PROVIDER/rawImpressionUploads/d2"
    private const val D3 = "$DATA_PROVIDER/rawImpressionUploads/d3"
    private const val D4 = "$DATA_PROVIDER/rawImpressionUploads/d4"
    private const val D5 = "$DATA_PROVIDER/rawImpressionUploads/d5"
    private const val C2 = "$DATA_PROVIDER/rawImpressionUploadCorrectionCandidates/candidate-d2"
    private const val C4 = "$DATA_PROVIDER/rawImpressionUploadCorrectionCandidates/candidate-d4"
    private const val MEMOIZED_MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/memoized"
    private const val DIRECT_MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/direct"
    private const val OPERATION_ID = "11111111-1111-4111-8111-111111111111"
    private val CUTOFF = Instant.ofEpochSecond(1L)
  }
}
