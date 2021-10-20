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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.verifyZeroInteractions
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepKey
import org.wfanet.measurement.api.v2alpha.exchangeStep
import org.wfanet.panelmatch.client.launcher.ApiClient.ClaimedExchangeStep
import org.wfanet.panelmatch.client.launcher.InvalidExchangeStepException.FailureType.PERMANENT
import org.wfanet.panelmatch.client.launcher.InvalidExchangeStepException.FailureType.TRANSIENT
import org.wfanet.panelmatch.common.testing.runBlockingTest
import org.wfanet.panelmatch.common.toByteString

private const val RECURRING_EXCHANGE_ID = "some-recurring-exchange-id"
private const val EXCHANGE_ID = "some-exchange-id"
private const val EXCHANGE_STEP_ID = "some-step-id"
private const val EXCHANGE_STEP_ATTEMPT_ID = "some-attempt-id"

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
  serializedExchangeWorkflow = "some-serialized-exchange-workflow".toByteString()
}

private val CLAIMED_EXCHANGE_STEP = ClaimedExchangeStep(EXCHANGE_STEP, EXCHANGE_STEP_ATTEMPT_KEY)

@RunWith(JUnit4::class)
class ExchangeStepLauncherTest {
  private val apiClient: ApiClient = mock()
  private val jobLauncher: JobLauncher = mock()
  private val validator: ExchangeStepValidator = mock()
  private val launcher = ExchangeStepLauncher(apiClient, validator, jobLauncher)

  @Test
  fun noExchangeTask() = runBlockingTest {
    whenever(apiClient.claimExchangeStep()).thenReturn(null)

    launcher.findAndRunExchangeStep()

    verifyBlocking(apiClient) { claimExchangeStep() }
    verifyZeroInteractions(jobLauncher, validator)
  }

  @Test
  fun validExchangeTask() = runBlockingTest {
    whenever(apiClient.claimExchangeStep()).thenReturn(CLAIMED_EXCHANGE_STEP)

    launcher.findAndRunExchangeStep()

    verifyBlocking(jobLauncher) {
      val exchangeStepCaptor = argumentCaptor<ExchangeStep>()
      val attemptCaptor = argumentCaptor<ExchangeStepAttemptKey>()

      execute(exchangeStepCaptor.capture(), attemptCaptor.capture())

      assertThat(exchangeStepCaptor.firstValue).isEqualTo(EXCHANGE_STEP)
      assertThat(attemptCaptor.firstValue).isEqualTo(EXCHANGE_STEP_ATTEMPT_KEY)
    }

    verifyBlocking(validator) {
      val exchangeStepCaptor = argumentCaptor<ExchangeStep>()

      validate(exchangeStepCaptor.capture())

      assertThat(exchangeStepCaptor.firstValue).isEqualTo(EXCHANGE_STEP)
    }
  }

  @Test
  fun permanentInvalidExchangeStepException() = runBlockingTest {
    whenever(apiClient.claimExchangeStep()).thenReturn(CLAIMED_EXCHANGE_STEP)

    val message = "some-message"
    whenever(validator.validate(any())).thenThrow(InvalidExchangeStepException(PERMANENT, message))

    launcher.findAndRunExchangeStep()

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
      assertThat(messagesCaptor.firstValue).containsExactly(message)
    }

    verifyZeroInteractions(jobLauncher)
  }

  @Test
  fun transientInvalidExchangeStepException() = runBlockingTest {
    whenever(apiClient.claimExchangeStep()).thenReturn(CLAIMED_EXCHANGE_STEP)

    val message = "some-message"
    whenever(validator.validate(any())).thenThrow(InvalidExchangeStepException(TRANSIENT, message))

    launcher.findAndRunExchangeStep()

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
      assertThat(stateCaptor.firstValue).isEqualTo(ExchangeStepAttempt.State.FAILED)
      assertThat(messagesCaptor.firstValue).containsExactly(message)
    }

    verifyZeroInteractions(jobLauncher)
  }

  @Test
  fun genericExceptionInLauncher() = runBlockingTest {
    whenever(apiClient.claimExchangeStep()).thenReturn(CLAIMED_EXCHANGE_STEP)

    val message = "some-message"
    whenever(jobLauncher.execute(any(), any())).thenThrow(RuntimeException(message))

    launcher.findAndRunExchangeStep()

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
      assertThat(stateCaptor.firstValue).isEqualTo(ExchangeStepAttempt.State.FAILED)
      assertThat(messagesCaptor.firstValue).containsExactly(message)
    }
  }
}
