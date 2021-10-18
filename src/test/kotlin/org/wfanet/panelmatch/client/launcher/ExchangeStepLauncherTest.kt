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

package org.wfanet.panelmatch.client.launcher

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.stub
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.verifyZeroInteractions
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Party.DATA_PROVIDER
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Party.MODEL_PROVIDER
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.step
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.exchangeStep
import org.wfanet.measurement.api.v2alpha.exchangeWorkflow
import org.wfanet.panelmatch.client.launcher.ApiClient.ClaimedExchangeStep
import org.wfanet.panelmatch.common.secrets.SecretMap

private const val RECURRING_EXCHANGE_ID = "some-recurring-exchange-id"
private const val EXCHANGE_ID = "some-exchange-id"
private const val EXCHANGE_STEP_ID = "some-step-id"
private const val EXCHANGE_STEP_ATTEMPT_ID = "some-attempt-id"

private val EXCHANGE_WORKFLOW = exchangeWorkflow {
  steps += step { stepId = "step0" }
  steps +=
    step {
      stepId = "step1"
      party = MODEL_PROVIDER
    }
  steps +=
    step {
      stepId = "step2"
      party = DATA_PROVIDER
    }
}
private val SERIALIZED_EXCHANGE_WORKFLOW = EXCHANGE_WORKFLOW.toByteString()

private val EXCHANGE_STEP_ATTEMPT_KEY: ExchangeStepAttemptKey =
  ExchangeStepAttemptKey(
    recurringExchangeId = RECURRING_EXCHANGE_ID,
    exchangeId = EXCHANGE_ID,
    exchangeStepId = EXCHANGE_STEP_ID,
    exchangeStepAttemptId = EXCHANGE_STEP_ATTEMPT_ID
  )

private val EXCHANGE_STEP_KEY =
  ExchangeStepKey(
    recurringExchangeId = RECURRING_EXCHANGE_ID,
    exchangeId = EXCHANGE_ID,
    exchangeStepId = EXCHANGE_STEP_ID
  )

private val EXCHANGE_STEP: ExchangeStep = exchangeStep {
  name = EXCHANGE_STEP_KEY.toName()
  state = ExchangeStep.State.READY_FOR_RETRY
  stepIndex = 2
  serializedExchangeWorkflow = SERIALIZED_EXCHANGE_WORKFLOW
}

private object ValidExchangeWorkflows : SecretMap {
  override suspend fun get(key: String): ByteString? {
    return if (key == RECURRING_EXCHANGE_ID) SERIALIZED_EXCHANGE_WORKFLOW else null
  }
}

@RunWith(JUnit4::class)
class ExchangeStepLauncherTest {
  private val apiClient: ApiClient = mock()
  private val jobLauncher: JobLauncher = mock()
  private val validator = ExchangeStepValidator(DATA_PROVIDER, ValidExchangeWorkflows)

  @Test
  fun `findAndRunExchangeStep with no ExchangeTask`() {
    apiClient.stub { onBlocking { claimExchangeStep() }.thenReturn(null) }

    val launcher = ExchangeStepLauncher(apiClient, validator, jobLauncher)
    runBlocking { launcher.findAndRunExchangeStep() }

    verifyBlocking(apiClient) { claimExchangeStep() }
  }

  @Test
  fun `findAndRunExchangeStep with valid ExchangeTask`() {
    apiClient.stub {
      onBlocking { claimExchangeStep() }
        .thenReturn(ClaimedExchangeStep(EXCHANGE_STEP, EXCHANGE_STEP_ATTEMPT_KEY))
    }

    val launcher = ExchangeStepLauncher(apiClient, validator, jobLauncher)
    runBlocking { launcher.findAndRunExchangeStep() }

    verifyBlocking(apiClient) { claimExchangeStep() }

    val (exchangeStepCaptor, attemptCaptor) =
      argumentCaptor(ExchangeStep::class, ExchangeStepAttemptKey::class)
    verifyBlocking(jobLauncher) { execute(exchangeStepCaptor.capture(), attemptCaptor.capture()) }
    assertThat(exchangeStepCaptor.firstValue).isEqualTo(EXCHANGE_STEP)
    assertThat(attemptCaptor.firstValue).isEqualTo(EXCHANGE_STEP_ATTEMPT_KEY)
  }

  @Test
  fun `findAndRunExchangeStep with unrecognized ExchangeWorkflow`() {
    val invalidExchangeWorkflow = EXCHANGE_WORKFLOW.copy { steps += step {} }
    val invalidExchangeStep =
      EXCHANGE_STEP.copy { serializedExchangeWorkflow = invalidExchangeWorkflow.toByteString() }

    apiClient.stub {
      onBlocking { claimExchangeStep() }
        .thenReturn(ClaimedExchangeStep(invalidExchangeStep, EXCHANGE_STEP_ATTEMPT_KEY))
    }

    val launcher = ExchangeStepLauncher(apiClient, validator, jobLauncher)
    runBlocking { launcher.findAndRunExchangeStep() }

    verifyBlocking(apiClient) { claimExchangeStep() }

    verifyBlocking(apiClient) {
      val keyCaptor = argumentCaptor<ExchangeStepAttemptKey>()
      val stateCaptor = argumentCaptor<ExchangeStepAttempt.State>()
      val messagesCaptor = argumentCaptor<Iterable<String>>()

      finishExchangeStepAttempt(
        keyCaptor.capture(),
        stateCaptor.capture(),
        messagesCaptor.capture()
      )

      assertThat(keyCaptor.firstValue).isEqualTo(EXCHANGE_STEP_ATTEMPT_KEY)
      assertThat(stateCaptor.firstValue).isEqualTo(ExchangeStepAttempt.State.FAILED_STEP)
      assertThat(messagesCaptor.firstValue)
        .containsExactly("Serialized ExchangeWorkflow unrecognized")
    }

    verifyZeroInteractions(jobLauncher)
  }

  @Test
  fun `findAndRunExchangeStep with wrong party`() {
    val invalidExchangeStep = EXCHANGE_STEP.copy { stepIndex = 1 }

    apiClient.stub {
      onBlocking { claimExchangeStep() }
        .thenReturn(ClaimedExchangeStep(invalidExchangeStep, EXCHANGE_STEP_ATTEMPT_KEY))
    }

    val launcher = ExchangeStepLauncher(apiClient, validator, jobLauncher)
    runBlocking { launcher.findAndRunExchangeStep() }

    verifyBlocking(apiClient) { claimExchangeStep() }

    verifyBlocking(apiClient) {
      val keyCaptor = argumentCaptor<ExchangeStepAttemptKey>()
      val stateCaptor = argumentCaptor<ExchangeStepAttempt.State>()
      val messagesCaptor = argumentCaptor<Iterable<String>>()

      finishExchangeStepAttempt(
        keyCaptor.capture(),
        stateCaptor.capture(),
        messagesCaptor.capture()
      )

      assertThat(keyCaptor.firstValue).isEqualTo(EXCHANGE_STEP_ATTEMPT_KEY)
      assertThat(stateCaptor.firstValue).isEqualTo(ExchangeStepAttempt.State.FAILED_STEP)
      assertThat(messagesCaptor.firstValue)
        .containsExactly("Party for step 'step1' was not DATA_PROVIDER")
    }

    verifyZeroInteractions(jobLauncher)
  }
}
