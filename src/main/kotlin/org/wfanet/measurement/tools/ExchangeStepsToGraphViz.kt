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

package org.wfanet.measurement.tools

import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Party.DATA_PROVIDER
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Party.MODEL_PROVIDER
import org.wfanet.measurement.common.graphviz.digraph
import org.wfanet.measurement.internal.kingdom.ExchangeStep

private val PARTY_COLOR = mapOf(DATA_PROVIDER to "blue", MODEL_PROVIDER to "red")
private const val BLOB_SHAPE = "egg"
private const val STEP_SHAPE = "box"

fun createGraphViz(exchangeWorkflow: ExchangeWorkflow, exchangeSteps: List<ExchangeStep>): String {
  val stepIndexToStep = exchangeSteps.associateBy { it.stepIndex }
  val steps = exchangeWorkflow.stepsList

  val graph = digraph {
    attributes { set("splines" to "ortho") }

    for ((party, partySteps) in steps.groupBy { it.party }) {
      val color = PARTY_COLOR.getValue(party)
      val outputs = mutableSetOf<String>()

      for (step in partySteps) {
        val nodeName = step.stepId.toNodeName()

        node(nodeName) {
          set("color" to color)
          set("shape" to STEP_SHAPE)
          set(
            "label" to
              step.stepId.plus(": ").plus(stepIndexToStep.get(step.stepId)?.stepStateToString())
          )
        }

        for (label in step.outputLabelsMap.values) {
          edge(nodeName to label.toNodeName())
        }

        for (label in step.inputLabelsMap.values) {
          edge(label.toNodeName() to nodeName)
        }

        outputs.addAll(step.outputLabelsMap.values)
      }

      for (output in outputs) {
        node(output.toNodeName()) {
          set("color" to color)
          set("shape" to BLOB_SHAPE)
          set("label" to output)
        }
      }
    }
  }

  return graph
}

private fun String.toNodeName(): String {
  return replace('-', '_')
}

private fun ExchangeStep.stepStateToString(): String {
  return when (state) {
    ExchangeStep.State.BLOCKED -> "Blocked"
    ExchangeStep.State.READY -> "Ready"
    ExchangeStep.State.READY_FOR_RETRY -> "Ready for retry"
    ExchangeStep.State.IN_PROGRESS -> "In progress"
    ExchangeStep.State.SUCCEEDED -> "Succeeded"
    ExchangeStep.State.FAILED -> "Failed"
    else -> "State unspecified"
  }
}
