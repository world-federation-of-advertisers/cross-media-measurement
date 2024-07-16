// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.common

import org.wfanet.panelmatch.client.internal.ExchangeWorkflow

/**
 * Helper that keeps track of the dependencies between steps in an [ExchangeWorkflow]. Steps can be
 * marked as completed or in-progress, and the graph can be queried to find an available step for a
 * given [ExchangeWorkflow.Party].
 */
class ExchangeWorkflowDependencyGraph
private constructor(
  private val orderedSteps: List<ExchangeWorkflow.Step>,
  private val prerequisiteStepIds: Map<String, Set<String>>,
) {

  /** An exchange step along with its index in the workflow. */
  data class IndexedStep(val step: ExchangeWorkflow.Step, val index: Int)

  private val completedStepIds = mutableSetOf<String>()
  private val inProgressStepIds = mutableSetOf<String>()

  /** Marks the step with the given [stepId] as completed, unblocking steps which depend on it. */
  fun markStepAsCompleted(stepId: String) {
    check(completedStepIds.containsAll(prerequisiteStepIds.getValue(stepId))) {
      "Step $stepId has incomplete prerequisite steps"
    }
    completedStepIds += stepId
  }

  /**
   * Marks the step with the given [stepId] as in-progress. In-progress steps continue to block
   * dependent steps, but are not not eligible to be returned from [getUnblockedStep].
   */
  fun markStepAsInProgress(stepId: String) {
    check(completedStepIds.containsAll(prerequisiteStepIds.getValue(stepId))) {
      "Step $stepId has incomplete prerequisite steps"
    }
    inProgressStepIds += stepId
  }

  /** Returns true if the given [party] has any remaining (incomplete) steps. */
  fun hasRemainingSteps(party: ExchangeWorkflow.Party): Boolean {
    for (step in orderedSteps) {
      if (step.party != party) {
        continue
      }
      if (step.stepId !in completedStepIds) {
        return true
      }
    }
    return false
  }

  /**
   * Returns an [IndexedStep] belonging to [party] such that all the step's prerequisite steps are
   * completed and the step has not been marked as in-progress. Returns null if there is no such
   * step.
   */
  fun getUnblockedStep(party: ExchangeWorkflow.Party): IndexedStep? {
    for ((index, step) in orderedSteps.withIndex()) {
      val stepId = step.stepId
      if (stepId in completedStepIds || stepId in inProgressStepIds || step.party != party) {
        continue
      }
      val prerequisiteIds = prerequisiteStepIds.getValue(stepId)
      if (completedStepIds.containsAll(prerequisiteIds)) {
        return IndexedStep(step, index)
      }
    }
    return null
  }

  companion object {

    /** Returns a new [ExchangeWorkflowDependencyGraph] for the given [workflow]. */
    fun fromWorkflow(workflow: ExchangeWorkflow): ExchangeWorkflowDependencyGraph {
      val outputLabelToIndexedStep = mutableMapOf<String, IndexedStep>()
      for ((index, step) in workflow.stepsList.withIndex()) {
        for (outputLabel in step.outputLabelsMap.values) {
          outputLabelToIndexedStep[outputLabel] = IndexedStep(step, index)
        }
      }

      val prerequisiteStepIds = mutableMapOf<String, Set<String>>()
      for ((index, step) in workflow.stepsList.withIndex()) {
        prerequisiteStepIds[step.stepId] =
          step.inputLabelsMap.values
            .map { label -> outputLabelToIndexedStep.getValue(label) }
            .filter { it.index < index }
            .map { it.step.stepId }
            .toSet()
      }

      return ExchangeWorkflowDependencyGraph(workflow.stepsList, prerequisiteStepIds)
    }
  }
}
