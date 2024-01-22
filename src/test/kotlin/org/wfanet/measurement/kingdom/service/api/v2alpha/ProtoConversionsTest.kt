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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Party as V2AlphaParty
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.StepKt.copyFromPreviousExchangeStep as v2AlphaCopyFromPreviousExchangeStorageStep
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.StepKt.copyFromSharedStorageStep as v2AlphaCopyFromSharedStorageStep
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.StepKt.copyToSharedStorageStep as v2AlphaCopyToSharedStorageStep
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.StepKt.inputStep as v2AlphaInputStep
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.step as v2AlphaStep
import org.wfanet.measurement.api.v2alpha.exchangeWorkflow as v2AlphaWorkflow
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow.Party as InternalParty
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow.Step as InternalExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflowKt.step as internalStep
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.exchangeWorkflow as internalWorkflow

private val V2ALPHA_EDP_INPUT_STEP = v2AlphaStep {
  stepId = "edp-input-foo-step"
  party = V2AlphaParty.DATA_PROVIDER
  inputStep = v2AlphaInputStep {}
  outputLabels["input"] = "edp-foo"
}
private val V2ALPHA_EDP_EXPORT_STEP = v2AlphaStep {
  stepId = "edp-export-foo"
  party = V2AlphaParty.DATA_PROVIDER
  copyToSharedStorageStep = v2AlphaCopyToSharedStorageStep {}
  inputLabels["foo"] = "edp-foo"
  outputLabels["foo"] = "foo"
}
private val V2ALPHA_EDP_IMPORT_STEP = v2AlphaStep {
  stepId = "edp-import-bar"
  party = V2AlphaParty.DATA_PROVIDER
  copyFromSharedStorageStep = v2AlphaCopyFromSharedStorageStep {}
  inputLabels["bar"] = "bar"
  outputLabels["bar"] = "edp-bar"
}
private val V2ALPHA_EDP_COPY_FROM_PREVIOUS_EXCHANGE_STEP = v2AlphaStep {
  stepId = "edp-input-or-copy-baz"
  party = V2AlphaParty.DATA_PROVIDER
  copyFromPreviousExchangeStep = v2AlphaCopyFromPreviousExchangeStorageStep {}
  inputLabels["baz"] = "edp-baz"
  outputLabels["baz"] = "edp-baz"
}

private val V2ALPHA_MP_INPUT_STEP = v2AlphaStep {
  stepId = "mp-input-bar-step"
  party = V2AlphaParty.MODEL_PROVIDER
  inputStep = v2AlphaInputStep {}
  outputLabels["input"] = "mp-bar"
}
private val V2ALPHA_MP_EXPORT_STEP = v2AlphaStep {
  stepId = "mp-export-bar"
  party = V2AlphaParty.MODEL_PROVIDER
  copyToSharedStorageStep = v2AlphaCopyToSharedStorageStep {}
  inputLabels["bar"] = "mp-bar"
  outputLabels["bar"] = "bar"
}
private val V2ALPHA_MP_IMPORT_STEP = v2AlphaStep {
  stepId = "mp-import-foo"
  party = V2AlphaParty.MODEL_PROVIDER
  copyFromSharedStorageStep = v2AlphaCopyFromSharedStorageStep {}
  inputLabels["foo"] = "foo"
  outputLabels["foo"] = "mp-foo"
}

private val INTERNAL_EDP_STEP = internalStep { party = InternalParty.DATA_PROVIDER }
private val INTERNAL_MP_STEP = internalStep { party = InternalParty.MODEL_PROVIDER }

@RunWith(JUnit4::class)
class ProtoConversionsTest {

  @Test
  fun `ExchangeWorkflow toInternal succeeds for workflow with no prerequisites`() {
    val v2AlphaWorkflow = v2AlphaWorkflow {
      steps += listOf(V2ALPHA_EDP_INPUT_STEP, V2ALPHA_MP_INPUT_STEP)
    }

    val internalWorkflow = v2AlphaWorkflow.toInternal()

    assertThat(internalWorkflow)
      .isEqualTo(
        internalWorkflow {
          steps += listOf(INTERNAL_EDP_STEP.withStepIndex(0), INTERNAL_MP_STEP.withStepIndex(1))
        }
      )
  }

  @Test
  fun `ExchangeWorkflow toInternal succeeds for workflow with ordered prerequisites`() {
    val v2AlphaWorkflow = v2AlphaWorkflow {
      steps +=
        listOf(
          V2ALPHA_EDP_INPUT_STEP,
          V2ALPHA_EDP_EXPORT_STEP,
          V2ALPHA_MP_INPUT_STEP,
          V2ALPHA_MP_EXPORT_STEP,
          V2ALPHA_EDP_IMPORT_STEP,
          V2ALPHA_MP_IMPORT_STEP,
        )
    }

    val internalWorkflow = v2AlphaWorkflow.toInternal()

    assertThat(internalWorkflow)
      .isEqualTo(
        internalWorkflow {
          steps +=
            listOf(
              INTERNAL_EDP_STEP.withStepIndex(0),
              INTERNAL_EDP_STEP.withStepIndex(1).withPrerequisites(0),
              INTERNAL_MP_STEP.withStepIndex(2),
              INTERNAL_MP_STEP.withStepIndex(3).withPrerequisites(2),
              INTERNAL_EDP_STEP.withStepIndex(4).withPrerequisites(3),
              INTERNAL_MP_STEP.withStepIndex(5).withPrerequisites(1),
            )
        }
      )
  }

  @Test
  fun `ExchangeWorkflow toInternal throws when steps are not topologically sorted`() {
    val v2AlphaWorkflow = v2AlphaWorkflow {
      steps +=
        listOf(
          V2ALPHA_EDP_INPUT_STEP,
          V2ALPHA_EDP_EXPORT_STEP,
          V2ALPHA_EDP_IMPORT_STEP,
          V2ALPHA_MP_INPUT_STEP,
          V2ALPHA_MP_EXPORT_STEP,
          V2ALPHA_MP_IMPORT_STEP,
        )
    }

    val exception = assertFailsWith<IllegalArgumentException> { v2AlphaWorkflow.toInternal() }
    assertThat(exception)
      .hasMessageThat()
      .contains(
        "Step ${V2ALPHA_EDP_IMPORT_STEP.stepId} with index 2 cannot depend on step" +
          " ${V2ALPHA_MP_EXPORT_STEP.stepId} with index 4"
      )
  }

  @Test
  fun `ExchangeWorkflow toInternal does not make copyFromPreviousExchangeStep its own prereq`() {
    val v2AlphaWorkflow = v2AlphaWorkflow {
      steps +=
        listOf(
          V2ALPHA_EDP_INPUT_STEP,
          V2ALPHA_EDP_EXPORT_STEP,
          V2ALPHA_MP_INPUT_STEP,
          V2ALPHA_MP_EXPORT_STEP,
          V2ALPHA_EDP_IMPORT_STEP,
          V2ALPHA_MP_IMPORT_STEP,
          V2ALPHA_EDP_COPY_FROM_PREVIOUS_EXCHANGE_STEP,
        )
    }

    val internalWorkflow = v2AlphaWorkflow.toInternal()

    assertThat(internalWorkflow)
      .isEqualTo(
        internalWorkflow {
          steps +=
            listOf(
              INTERNAL_EDP_STEP.withStepIndex(0),
              INTERNAL_EDP_STEP.withStepIndex(1).withPrerequisites(0),
              INTERNAL_MP_STEP.withStepIndex(2),
              INTERNAL_MP_STEP.withStepIndex(3).withPrerequisites(2),
              INTERNAL_EDP_STEP.withStepIndex(4).withPrerequisites(3),
              INTERNAL_MP_STEP.withStepIndex(5).withPrerequisites(1),
              INTERNAL_EDP_STEP.withStepIndex(6),
            )
        }
      )
  }
}

private fun InternalExchangeStep.withStepIndex(index: Int): InternalExchangeStep =
  this.copy { stepIndex = index }

private fun InternalExchangeStep.withPrerequisites(vararg indices: Int): InternalExchangeStep =
  this.copy { prerequisiteStepIndices += indices.toList() }
