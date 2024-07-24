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

package org.wfanet.panelmatch.client.launcher

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.time.LocalDate
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.kotlin.any
import org.mockito.kotlin.times
import org.mockito.kotlin.whenever
import org.wfanet.panelmatch.client.common.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.internal.ExchangeStepAttempt
import org.wfanet.panelmatch.client.internal.exchangeWorkflow
import org.wfanet.panelmatch.common.testing.runBlockingTest

@RunWith(JUnit4::class)
class ApiClientsTest {

  private val mockClient = mock<ApiClient>()

  @Test
  fun withMaxParallelClaimedExchangeStepsThrowsWhenStepsIsLessThanOne() {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        mockClient.withMaxParallelClaimedExchangeSteps(0)
      }
    assertThat(exception)
      .hasMessageThat()
      .contains("maxParallelClaimedExchangeSteps must be at least 1, but was 0")
  }

  @Test
  fun withMaxParallelClaimedExchangeSteps_claimExchangeStepCallsDelegate() = runBlockingTest {
    whenever(mockClient.claimExchangeStep()).thenReturn(STEP)
    val client = mockClient.withMaxParallelClaimedExchangeSteps(100)

    val step = client.claimExchangeStep()

    assertThat(step).isEqualTo(STEP)
    verify(mockClient).claimExchangeStep()
  }

  @Test
  fun withMaxParallelClaimedExchangeSteps_appendLogEntryCallsDelegate() = runBlockingTest {
    whenever(mockClient.appendLogEntry(any(), any())).thenReturn(null)
    val client = mockClient.withMaxParallelClaimedExchangeSteps(100)

    client.appendLogEntry(STEP.attemptKey, listOf("Some log message"))

    verify(mockClient).appendLogEntry(STEP.attemptKey, listOf("Some log message"))
  }

  @Test
  fun withMaxParallelClaimedExchangeSteps_finishExchangeStepAttemptCallsDelegate() =
    runBlockingTest {
      whenever(mockClient.claimExchangeStep()).thenReturn(STEP)
      whenever(mockClient.finishExchangeStepAttempt(any(), any(), any())).thenReturn(null)
      val client = mockClient.withMaxParallelClaimedExchangeSteps(100)
      client.claimExchangeStep()

      client.finishExchangeStepAttempt(STEP.attemptKey, ExchangeStepAttempt.State.SUCCEEDED)

      verify(mockClient)
        .finishExchangeStepAttempt(
          STEP.attemptKey,
          ExchangeStepAttempt.State.SUCCEEDED,
          emptyList(),
        )
    }

  @Test
  fun withMaxParallelClaimedExchangeSteps_canClaimExchangeStepsUpToSpecifiedMax() =
    runBlockingTest {
      whenever(mockClient.claimExchangeStep()).thenReturn(STEP)
      val client = mockClient.withMaxParallelClaimedExchangeSteps(3)

      val step1 = client.claimExchangeStep()
      val step2 = client.claimExchangeStep()
      val step3 = client.claimExchangeStep()
      val step4 = client.claimExchangeStep()

      assertThat(step1).isEqualTo(STEP)
      assertThat(step2).isEqualTo(STEP)
      assertThat(step3).isEqualTo(STEP)
      assertThat(step4).isNull()
      verify(mockClient, times(3)).claimExchangeStep()
    }

  @Test
  fun withMaxParallelClaimedExchangeSteps_whenDelegateReturnsNullNoPermitsAreExhausted() =
    runBlockingTest {
      whenever(mockClient.claimExchangeStep()).thenReturn(null).thenReturn(null).thenReturn(STEP)
      val client = mockClient.withMaxParallelClaimedExchangeSteps(1)

      val step1 = client.claimExchangeStep()
      val step2 = client.claimExchangeStep()
      val step3 = client.claimExchangeStep()
      val step4 = client.claimExchangeStep()

      assertThat(step1).isNull()
      assertThat(step2).isNull()
      assertThat(step3).isEqualTo(STEP)
      assertThat(step4).isNull()
      verify(mockClient, times(3)).claimExchangeStep()
    }

  @Test
  fun withMaxParallelClaimedExchangeSteps_finishingStepsAllowsAdditionalStepsToBeClaimed() =
    runBlockingTest {
      whenever(mockClient.claimExchangeStep()).thenReturn(STEP)
      whenever(mockClient.finishExchangeStepAttempt(any(), any(), any())).thenReturn(null)
      val client = mockClient.withMaxParallelClaimedExchangeSteps(1)

      val step1 = client.claimExchangeStep()
      val step2 = client.claimExchangeStep()

      assertThat(step1).isEqualTo(STEP)
      assertThat(step2).isNull()

      client.finishExchangeStepAttempt(STEP.attemptKey, ExchangeStepAttempt.State.SUCCEEDED)

      val step3 = client.claimExchangeStep()
      val step4 = client.claimExchangeStep()

      assertThat(step3).isEqualTo(STEP)
      assertThat(step4).isNull()
    }

  @Test
  fun withMaxParallelClaimedExchangeSteps_propagatesExceptionFromBaseClient() = runBlockingTest {
    val exception = IllegalStateException("Something went wrong")
    whenever(mockClient.claimExchangeStep()).thenThrow(exception)
    val client = mockClient.withMaxParallelClaimedExchangeSteps(100)

    val actual = assertFailsWith<IllegalStateException> { client.claimExchangeStep() }
    assertThat(actual).isEqualTo(exception)
  }

  @Test
  fun withMaxParallelClaimedExchangeSteps_doesNotExhaustPermitsWhenAnExceptionIsThrown() =
    runBlockingTest {
      val exception = IllegalStateException("Something went wrong")
      whenever(mockClient.claimExchangeStep()).thenThrow(exception).thenReturn(STEP)
      val client = mockClient.withMaxParallelClaimedExchangeSteps(1)

      assertFailsWith<IllegalStateException> { client.claimExchangeStep() }

      val step1 = client.claimExchangeStep()
      val step2 = client.claimExchangeStep()

      assertThat(step1).isEqualTo(STEP)
      assertThat(step2).isNull()
      verify(mockClient, times(2)).claimExchangeStep()
    }

  companion object {
    private val STEP =
      ApiClient.ClaimedExchangeStep(
        attemptKey =
          ExchangeStepAttemptKey(
            recurringExchangeId = "some-recurring-exchange-id",
            exchangeId = "2024-07-01",
            stepId = "some-step-id",
            attemptId = "1",
          ),
        exchangeDate = LocalDate.parse("2024-07-01"),
        stepIndex = 0,
        workflow = exchangeWorkflow {},
        workflowFingerprint = ByteString.EMPTY,
      )
  }
}
