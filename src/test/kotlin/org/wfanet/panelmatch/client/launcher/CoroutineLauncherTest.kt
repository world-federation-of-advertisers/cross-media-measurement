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

import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.common.CountDownLatch
import org.wfanet.measurement.kingdom.service.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.launcher.testing.buildStep
import org.wfanet.panelmatch.common.testing.runBlockingTest

@RunWith(JUnit4::class)
class CoroutineLauncherTest {
  private val stepExecutor = mock<ExchangeStepExecutor>()
  private val launcher = CoroutineLauncher(stepExecutor = stepExecutor)

  @Test
  fun `launches`() = runBlockingTest {
    val workflowStep = buildStep(ExchangeWorkflow.Step.StepCase.ENCRYPT_STEP)
    val step =
      ExchangeStep.newBuilder()
        .apply {
          name = "some-exchange-step-name"
          signedExchangeWorkflowBuilder.data = workflowStep.toByteString()
        }
        .build()

    val startLatch = CountDownLatch(1)
    val middleLatch = CountDownLatch(1)
    val endLatch = CountDownLatch(1)
    whenever(stepExecutor.execute(any(), any())).thenAnswer {
      runBlocking {
        startLatch.countDown()
        middleLatch.await()
        endLatch.countDown()
      }
    }

    val attemptKey = ExchangeStepAttemptKey("w", "x", "y", "z")
    launcher.execute(step, attemptKey)

    startLatch.await()
    middleLatch.countDown()
    endLatch.await()

    verify(stepExecutor).execute(attemptKey, workflowStep)
  }
}
