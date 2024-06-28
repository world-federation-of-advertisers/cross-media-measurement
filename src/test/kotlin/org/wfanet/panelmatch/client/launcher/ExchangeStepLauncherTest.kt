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
import java.time.LocalDate
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
import org.wfanet.panelmatch.client.common.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.step
import org.wfanet.panelmatch.client.internal.exchangeWorkflow
import org.wfanet.panelmatch.client.launcher.ApiClient.ClaimedExchangeStep
import org.wfanet.panelmatch.common.testing.runBlockingTest

private const val RECURRING_EXCHANGE_ID = "some-recurring-exchange-id"
private const val EXCHANGE_ID = "some-exchange-id"
private const val EXCHANGE_STEP_ID = "some-step-id"
private const val EXCHANGE_STEP_ATTEMPT_ID = "some-attempt-id"

private val EXCHANGE_STEP_ATTEMPT_KEY: ExchangeStepAttemptKey =
  ExchangeStepAttemptKey(
    recurringExchangeId = RECURRING_EXCHANGE_ID,
    exchangeId = EXCHANGE_ID,
    stepId = EXCHANGE_STEP_ID,
    attemptId = EXCHANGE_STEP_ATTEMPT_ID,
    simpleName = "some-name",
  )

private val DATE = LocalDate.now()

private val WORKFLOW = exchangeWorkflow {
  steps += step {}
  steps += step {}
  steps += step { stepId = "this-step-will-run" }
  steps += step {}
}

private val CLAIMED_EXCHANGE_STEP: ClaimedExchangeStep =
  ClaimedExchangeStep(
    attemptKey = EXCHANGE_STEP_ATTEMPT_KEY,
    exchangeDate = DATE,
    stepIndex = 2,
    workflow = WORKFLOW,
    workflowFingerprint = WORKFLOW.toByteString(),
  )

@RunWith(JUnit4::class)
class ExchangeStepLauncherTest {
  private val apiClient: ApiClient = mock()
  private val stepExecutor: ExchangeStepExecutor = mock()
  private val launcher = ExchangeStepLauncher(apiClient, stepExecutor)

  @Test
  fun noExchangeTask() = runBlockingTest {
    whenever(apiClient.claimExchangeStep()).thenReturn(null)

    launcher.findAndRunExchangeStep()

    verifyBlocking(apiClient) { claimExchangeStep() }
    verifyNoInteractions(stepExecutor)
  }

  @Test
  fun validExchangeTask() = runBlockingTest {
    whenever(apiClient.claimExchangeStep()).thenReturn(CLAIMED_EXCHANGE_STEP)

    launcher.findAndRunExchangeStep()

    verifyBlocking(stepExecutor) {
      val stepCaptor = argumentCaptor<ClaimedExchangeStep>()

      execute(stepCaptor.capture())

      assertThat(stepCaptor.firstValue).isEqualTo(CLAIMED_EXCHANGE_STEP)
    }
  }
}
