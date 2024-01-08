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

import java.time.LocalDate
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.StepKt.commutativeDeterministicEncryptStep
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.step
import org.wfanet.measurement.common.CountDownLatch
import org.wfanet.panelmatch.client.launcher.ExchangeStepValidator.ValidatedExchangeStep
import org.wfanet.panelmatch.client.launcher.testing.buildWorkflow
import org.wfanet.panelmatch.common.testing.runBlockingTest

@RunWith(JUnit4::class)
class CoroutineLauncherTest {
  private val stepExecutor = mock<ExchangeStepExecutor>()

  @Test
  fun launches() = runBlockingTest {
    val launcher = CoroutineLauncher(stepExecutor = stepExecutor)
    val workflowStep = step {
      this.commutativeDeterministicEncryptStep = commutativeDeterministicEncryptStep {}
    }
    val workflow = buildWorkflow(workflowStep, "some-edp", "some-mp")

    val startLatch1 = CountDownLatch(1)
    val middleLatch1 = CountDownLatch(1)
    val endLatch1 = CountDownLatch(1)
    val startLatch2 = CountDownLatch(1)
    val middleLatch2 = CountDownLatch(1)
    val endLatch2 = CountDownLatch(1)

    val attemptKey1 = CanonicalExchangeStepAttemptKey("a", "b", "c", "d")
    val attemptKey2 = CanonicalExchangeStepAttemptKey("w", "x", "y", "z")

    whenever(stepExecutor.execute(any(), eq(attemptKey1))).thenAnswer {
      runBlocking {
        startLatch1.countDown()
        middleLatch1.await()
        endLatch1.countDown()
      }
    }
    whenever(stepExecutor.execute(any(), eq(attemptKey2))).thenAnswer {
      runBlocking {
        startLatch2.countDown()
        middleLatch2.await()
        endLatch2.countDown()
      }
    }
    val date = LocalDate.of(2021, 11, 29)

    val validatedExchangeStep = ValidatedExchangeStep(workflow, workflow.getSteps(0), date)

    launcher.execute(validatedExchangeStep, attemptKey1)
    launcher.execute(validatedExchangeStep, attemptKey2)

    startLatch1.await()
    startLatch2.await()
    middleLatch1.countDown()
    middleLatch2.countDown()
    endLatch1.await()
    endLatch2.await()

    val stepCaptor = argumentCaptor<ValidatedExchangeStep>()
    val attemptKeyCaptor = argumentCaptor<CanonicalExchangeStepAttemptKey>()
    verify(stepExecutor, times(2)).execute(stepCaptor.capture(), attemptKeyCaptor.capture())
  }
}
