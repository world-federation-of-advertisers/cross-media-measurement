// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common

import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow
import org.wfanet.measurement.internal.kingdom.RecurringExchange

/** Class that creates and updates set of [ExchangeStep]s based on a [ExchangeWorkflow]. */
class ExchangeStepsHierarchy(
  private val recurringExchange: RecurringExchange,
  private val exchangeSteps: List<ExchangeStep>? = null
) {
  private val map = mutableMapOf<Int, ExchangeStep>()
  private var workflow: ExchangeWorkflow = ExchangeWorkflow.getDefaultInstance()

  init {
    require(recurringExchange.details.exchangeWorkflow.isValid()) {
      "Valid RecurringExchange and workflow needed."
    }
    this.workflow = recurringExchange.details.exchangeWorkflow
    if (exchangeSteps == null) {
      createSteps()
    } else {
      exchangeSteps.forEach { map[it.stepIndex] = it }
    }
    updateSteps()
  }

  /**
   * Get list of [ExchangeStep] with updated statuses.
   *
   * @return list of [ExchangeStep]
   */
  fun getExchangeSteps(): List<ExchangeStep> {
    return map.values.toList().sortedBy { it.stepIndex }
  }

  /**
   * Get first [ExchangeStep] with latest Status: READY | READY_TO_RETRY
   *
   * @return an [ExchangeStep]
   */
  fun getReadyExchangeStep(): ExchangeStep {
    return getExchangeSteps().filter { it.state.isReady }.firstOrNull()
      ?: throw error("No ExchangeStep is ready.")
  }

  // If no exchange steps provided, create them based on the workflow.
  private fun createSteps() {
    workflow.stepsList.forEach {
      map[it.stepIndex] =
        ExchangeStep.newBuilder()
          .apply {
            externalRecurringExchangeId = recurringExchange.externalRecurringExchangeId
            if (it.party == ExchangeWorkflow.Party.MODEL_PROVIDER) {
              externalModelProviderId = recurringExchange.externalModelProviderId
            } else if (it.party == ExchangeWorkflow.Party.DATA_PROVIDER) {
              externalDataProviderId = recurringExchange.externalDataProviderId
            }
            date = recurringExchange.nextExchangeDate
            stepIndex = it.stepIndex
            state =
              if (it.prerequisiteStepIndicesCount == 0) {
                ExchangeStep.State.READY
              } else {
                ExchangeStep.State.BLOCKED
              }
          }
          .build()
    }
  }

  // Update the ExchangeSteps based on their prerequisites' statuses.
  private fun updateSteps() {
    workflow.stepsList.forEach { it ->
      var ready = true
      val step = map[it.stepIndex] ?: return
      for (prerequisite in it.prerequisiteStepIndicesList) {
        val preStep = map[prerequisite] ?: continue
        if (preStep.state.isBlocked) {
          ready = false
        }
      }
      if (ready && step.state == ExchangeStep.State.BLOCKED) {
        map[it.stepIndex] = step.toBuilder().setState(ExchangeStep.State.READY).build()
      } else if (!ready && step.state.isReady) {
        map[it.stepIndex] = step.toBuilder().setState(ExchangeStep.State.BLOCKED).build()
      }
    }
  }

  // Checks if [ExchangeWorkflow] is valid and does not have dependency cycle.
  private fun ExchangeWorkflow.isValid(): Boolean {
    if (!isInitialized) {
      return false
    }
    val hashmap = mutableMapOf<Int, MutableList<Int>>()
    stepsList.forEach { step ->
      if (!step.party.isValid) {
        return false
      }
      if (hashmap.containsKey(step.stepIndex)) {
        return false
      }
      hashmap[step.stepIndex] = mutableListOf()
    }
    stepsList.forEach { step ->
      for (prerequisite in step.prerequisiteStepIndicesList) {
        val preSteps = hashmap[prerequisite] ?: return false
        preSteps.add(step.stepIndex)
        hashmap[prerequisite] = preSteps
      }
    }
    val visited = BooleanArray(stepsCount + 1)
    for (key in hashmap.keys) {
      if (hasCycle(hashmap, key, visited)) {
        return false
      }
    }
    return true
  }

  private fun hasCycle(nodes: Map<Int, List<Int>>, key: Int, visited: BooleanArray): Boolean {
    if (visited[key]) {
      return true
    }
    visited[key] = true
    for (child in nodes[key]!!) {
      if (hasCycle(nodes, child, visited)) {
        return true
      }
    }
    visited[key] = false
    return false
  }
}

private val ExchangeWorkflow.Party.isValid: Boolean
  get() =
    when (this) {
      ExchangeWorkflow.Party.MODEL_PROVIDER, ExchangeWorkflow.Party.DATA_PROVIDER -> true
      ExchangeWorkflow.Party.UNRECOGNIZED, ExchangeWorkflow.Party.PARTY_UNSPECIFIED -> false
    }

private val ExchangeStep.State.isBlocked: Boolean
  get() =
    when (this) {
      ExchangeStep.State.SUCCEEDED -> false
      ExchangeStep.State.READY,
      ExchangeStep.State.READY_FOR_RETRY,
      ExchangeStep.State.IN_PROGRESS,
      ExchangeStep.State.BLOCKED,
      ExchangeStep.State.FAILED,
      ExchangeStep.State.UNRECOGNIZED,
      ExchangeStep.State.STATE_UNSPECIFIED -> true
    }

private val ExchangeStep.State.isReady: Boolean
  get() =
    when (this) {
      ExchangeStep.State.READY, ExchangeStep.State.READY_FOR_RETRY -> true
      ExchangeStep.State.IN_PROGRESS,
      ExchangeStep.State.BLOCKED,
      ExchangeStep.State.SUCCEEDED,
      ExchangeStep.State.FAILED,
      ExchangeStep.State.UNRECOGNIZED,
      ExchangeStep.State.STATE_UNSPECIFIED -> false
    }
