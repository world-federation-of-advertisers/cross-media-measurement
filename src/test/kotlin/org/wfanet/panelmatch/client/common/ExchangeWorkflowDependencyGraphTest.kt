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

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.panelmatch.client.common.ExchangeWorkflowDependencyGraph.IndexedStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Party.DATA_PROVIDER
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Party.MODEL_PROVIDER
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.copyFromSharedStorageStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.copyToSharedStorageStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.generateHybridEncryptionKeyPairStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.generateRandomBytesStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.hybridDecryptStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.hybridEncryptStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.step
import org.wfanet.panelmatch.client.internal.exchangeWorkflow

@RunWith(JUnit4::class)
class ExchangeWorkflowDependencyGraphTest {

  @Test
  fun markStepAsCompletedThrowsWhenStepHasIncompletePrerequisiteSteps() {
    val graph = ExchangeWorkflowDependencyGraph.fromWorkflow(WORKFLOW)

    val exception =
      assertFailsWith<IllegalStateException> {
        graph.markStepAsCompleted(MP_EXPORT_PUBLIC_KEY_STEP)
      }

    assertThat(exception)
      .hasMessageThat()
      .contains("Step ${MP_EXPORT_PUBLIC_KEY_STEP.stepId} has incomplete prerequisite steps")
  }

  @Test
  fun markStepAsInProgressThrowsWhenStepHasIncompletePrerequisiteSteps() {
    val graph = ExchangeWorkflowDependencyGraph.fromWorkflow(WORKFLOW)

    val exception =
      assertFailsWith<IllegalStateException> {
        graph.markStepAsInProgress(EDP_ENCRYPT_RANDOM_BYTES_STEP)
      }

    assertThat(exception)
      .hasMessageThat()
      .contains("Step ${EDP_ENCRYPT_RANDOM_BYTES_STEP.stepId} has incomplete prerequisite steps")
  }

  @Test
  fun hasRemainingStepsReturnsTrueForStepWithNoDependencies() {
    val graph =
      ExchangeWorkflowDependencyGraph.fromWorkflow(
        exchangeWorkflow { steps += step { party = DATA_PROVIDER } }
      )

    assertThat(graph.hasRemainingSteps(DATA_PROVIDER)).isTrue()
  }

  @Test
  fun hasRemainingStepsReturnsTrueForNewlyInitializedGraph() {
    val graph = ExchangeWorkflowDependencyGraph.fromWorkflow(WORKFLOW)

    assertThat(graph.hasRemainingSteps(DATA_PROVIDER)).isTrue()
    assertThat(graph.hasRemainingSteps(MODEL_PROVIDER)).isTrue()
  }

  @Test
  fun hasRemainingStepsReturnsTrueWhenAvailableStepsAreInProgress() {
    val graph = ExchangeWorkflowDependencyGraph.fromWorkflow(WORKFLOW)

    graph.markStepAsCompleted(MP_GENERATE_KEY_PAIR_STEP)
    graph.markStepAsCompleted(EDP_GENERATE_RANDOM_BYTES_STEP)

    assertThat(graph.hasRemainingSteps(DATA_PROVIDER)).isTrue()
    assertThat(graph.hasRemainingSteps(MODEL_PROVIDER)).isTrue()
  }

  @Test
  fun hasRemainingStepsReturnsFalseWhenAllStepsAreCompleted() {
    val graph = ExchangeWorkflowDependencyGraph.fromWorkflow(WORKFLOW)

    for (step in WORKFLOW.stepsList) {
      graph.markStepAsCompleted(step.stepId)
    }

    assertThat(graph.hasRemainingSteps(DATA_PROVIDER)).isFalse()
    assertThat(graph.hasRemainingSteps(MODEL_PROVIDER)).isFalse()
  }

  @Test
  fun hasRemainingStepsReturnsTrueForMpAndFalseForEdp() {
    val graph = ExchangeWorkflowDependencyGraph.fromWorkflow(WORKFLOW)

    graph.markStepAsCompleted(MP_GENERATE_KEY_PAIR_STEP)
    graph.markStepAsCompleted(MP_EXPORT_PUBLIC_KEY_STEP)
    graph.markStepAsCompleted(EDP_IMPORT_PUBLIC_KEY_STEP)
    graph.markStepAsCompleted(EDP_GENERATE_RANDOM_BYTES_STEP)
    graph.markStepAsCompleted(EDP_ENCRYPT_RANDOM_BYTES_STEP)
    graph.markStepAsCompleted(EDP_EXPORT_ENCRYPTED_BYTES_STEP)

    assertThat(graph.hasRemainingSteps(DATA_PROVIDER)).isFalse()
    assertThat(graph.hasRemainingSteps(MODEL_PROVIDER)).isTrue()
  }

  @Test
  fun getUnblockedStepReturnsStepForNewlyInitializedGraph() {
    val graph = ExchangeWorkflowDependencyGraph.fromWorkflow(WORKFLOW)

    assertThat(graph.getUnblockedStep(DATA_PROVIDER))
      .isEqualTo(IndexedStep(step = EDP_GENERATE_RANDOM_BYTES_STEP, index = 3))
    assertThat(graph.getUnblockedStep(MODEL_PROVIDER))
      .isEqualTo(IndexedStep(step = MP_GENERATE_KEY_PAIR_STEP, index = 0))
  }

  @Test
  fun getUnblockedStepReturnsStepWhosePrerequisitesAreCompleted() {
    val graph = ExchangeWorkflowDependencyGraph.fromWorkflow(WORKFLOW)

    graph.markStepAsCompleted(MP_GENERATE_KEY_PAIR_STEP)
    graph.markStepAsCompleted(MP_EXPORT_PUBLIC_KEY_STEP)
    graph.markStepAsCompleted(EDP_IMPORT_PUBLIC_KEY_STEP)
    graph.markStepAsCompleted(EDP_GENERATE_RANDOM_BYTES_STEP)

    assertThat(graph.getUnblockedStep(DATA_PROVIDER))
      .isEqualTo(IndexedStep(step = EDP_ENCRYPT_RANDOM_BYTES_STEP, index = 4))
    assertThat(graph.getUnblockedStep(MODEL_PROVIDER)).isNull()
  }

  @Test
  fun getUnblockedStepReturnsNullWhenPrerequisitesAreInProgress() {
    val graph = ExchangeWorkflowDependencyGraph.fromWorkflow(WORKFLOW)

    graph.markStepAsCompleted(MP_GENERATE_KEY_PAIR_STEP)
    graph.markStepAsCompleted(MP_EXPORT_PUBLIC_KEY_STEP)
    graph.markStepAsCompleted(EDP_IMPORT_PUBLIC_KEY_STEP)
    graph.markStepAsCompleted(EDP_GENERATE_RANDOM_BYTES_STEP)
    graph.markStepAsInProgress(EDP_ENCRYPT_RANDOM_BYTES_STEP)

    assertThat(graph.getUnblockedStep(DATA_PROVIDER)).isNull()
    assertThat(graph.getUnblockedStep(MODEL_PROVIDER)).isNull()
  }

  @Test
  fun getUnblockedStepReturnsNullWhenAllStepsAreCompleted() {
    val graph = ExchangeWorkflowDependencyGraph.fromWorkflow(WORKFLOW)

    for (step in WORKFLOW.stepsList) {
      graph.markStepAsCompleted(step.stepId)
    }

    assertThat(graph.getUnblockedStep(DATA_PROVIDER)).isNull()
    assertThat(graph.getUnblockedStep(MODEL_PROVIDER)).isNull()
  }

  @Test
  fun graphIsTraversedInExpectedOrder() {
    val graph = ExchangeWorkflowDependencyGraph.fromWorkflow(WORKFLOW)

    graph.assertUnblockedStepsAndAdvanceGraph(
      edpStep = EDP_GENERATE_RANDOM_BYTES_STEP,
      mpStep = MP_GENERATE_KEY_PAIR_STEP,
    )
    graph.assertUnblockedStepsAndAdvanceGraph(edpStep = null, mpStep = MP_EXPORT_PUBLIC_KEY_STEP)
    graph.assertUnblockedStepsAndAdvanceGraph(edpStep = EDP_IMPORT_PUBLIC_KEY_STEP, mpStep = null)
    graph.assertUnblockedStepsAndAdvanceGraph(
      edpStep = EDP_ENCRYPT_RANDOM_BYTES_STEP,
      mpStep = null,
    )
    graph.assertUnblockedStepsAndAdvanceGraph(
      edpStep = EDP_EXPORT_ENCRYPTED_BYTES_STEP,
      mpStep = null,
    )
    graph.assertUnblockedStepsAndAdvanceGraph(
      edpStep = null,
      mpStep = MP_IMPORT_ENCRYPTED_BYTES_STEP,
    )
    graph.assertUnblockedStepsAndAdvanceGraph(edpStep = null, mpStep = MP_DECRYPT_RANDOM_BYTES_STEP)

    assertThat(graph.hasRemainingSteps(DATA_PROVIDER)).isFalse()
    assertThat(graph.hasRemainingSteps(MODEL_PROVIDER)).isFalse()
  }

  private fun ExchangeWorkflowDependencyGraph.assertUnblockedStepsAndAdvanceGraph(
    edpStep: ExchangeWorkflow.Step?,
    mpStep: ExchangeWorkflow.Step?,
  ) {
    assertThat(getUnblockedStep(DATA_PROVIDER)?.step).isEqualTo(edpStep)
    assertThat(getUnblockedStep(MODEL_PROVIDER)?.step).isEqualTo(mpStep)
    edpStep?.let { markStepAsCompleted(it) }
    mpStep?.let { markStepAsCompleted(it) }
  }

  companion object {

    private val MP_GENERATE_KEY_PAIR_STEP = step {
      stepId = "mp-generate-key-pair"
      party = MODEL_PROVIDER
      generateHybridEncryptionKeyPairStep = generateHybridEncryptionKeyPairStep {}
      outputLabels["private-key"] = "mp-private-key"
      outputLabels["public-key"] = "mp-public-key"
    }

    private val MP_EXPORT_PUBLIC_KEY_STEP = step {
      stepId = "mp-export-public-key"
      party = MODEL_PROVIDER
      copyToSharedStorageStep = copyToSharedStorageStep {}
      inputLabels["public-key"] = "mp-public-key"
      outputLabels["public-key"] = "shared-public-key"
    }

    private val EDP_IMPORT_PUBLIC_KEY_STEP = step {
      stepId = "edp-import-public-key"
      party = DATA_PROVIDER
      copyFromSharedStorageStep = copyFromSharedStorageStep {}
      inputLabels["public-key"] = "shared-public-key"
      outputLabels["public-key"] = "edp-public-key"
    }

    private val EDP_GENERATE_RANDOM_BYTES_STEP = step {
      stepId = "edp-generate-random-bytes"
      party = DATA_PROVIDER
      generateRandomBytesStep = generateRandomBytesStep {}
      outputLabels["random-bytes"] = "edp-random-bytes"
    }

    private val EDP_ENCRYPT_RANDOM_BYTES_STEP = step {
      stepId = "edp-encrypt-random-bytes"
      party = DATA_PROVIDER
      hybridEncryptStep = hybridEncryptStep {}
      inputLabels["plaintext-data"] = "edp-random-bytes"
      inputLabels["encryption-key"] = "edp-public-key"
      outputLabels["encrypted-data"] = "edp-encrypted-bytes"
    }

    private val EDP_EXPORT_ENCRYPTED_BYTES_STEP = step {
      stepId = "edp-export-encrypted-bytes"
      party = DATA_PROVIDER
      copyToSharedStorageStep = copyToSharedStorageStep {}
      inputLabels["encrypted-bytes"] = "edp-encrypted-bytes"
      outputLabels["encrypted-bytes"] = "shared-encrypted-bytes"
    }

    private val MP_IMPORT_ENCRYPTED_BYTES_STEP = step {
      stepId = "mp-import-encrypted-bytes"
      party = MODEL_PROVIDER
      copyFromSharedStorageStep = copyFromSharedStorageStep {}
      inputLabels["encrypted-bytes"] = "shared-encrypted-bytes"
      outputLabels["encrypted-bytes"] = "mp-encrypted-bytes"
    }

    private val MP_DECRYPT_RANDOM_BYTES_STEP = step {
      stepId = "mp-decrypt-random-bytes"
      party = MODEL_PROVIDER
      hybridDecryptStep = hybridDecryptStep {}
      inputLabels["encrypted-data"] = "mp-encrypted-bytes"
      inputLabels["decryption-key"] = "mp-private-key"
      outputLabels["decrypted-data"] = "mp-random-bytes"
    }

    private val WORKFLOW = exchangeWorkflow {
      steps += MP_GENERATE_KEY_PAIR_STEP
      steps += MP_EXPORT_PUBLIC_KEY_STEP
      steps += EDP_IMPORT_PUBLIC_KEY_STEP
      steps += EDP_GENERATE_RANDOM_BYTES_STEP
      steps += EDP_ENCRYPT_RANDOM_BYTES_STEP
      steps += EDP_EXPORT_ENCRYPTED_BYTES_STEP
      steps += MP_IMPORT_ENCRYPTED_BYTES_STEP
      steps += MP_DECRYPT_RANDOM_BYTES_STEP
    }

    private fun ExchangeWorkflowDependencyGraph.markStepAsInProgress(step: ExchangeWorkflow.Step) {
      markStepAsInProgress(step.stepId)
    }

    private fun ExchangeWorkflowDependencyGraph.markStepAsCompleted(step: ExchangeWorkflow.Step) {
      markStepAsCompleted(step.stepId)
    }
  }
}
