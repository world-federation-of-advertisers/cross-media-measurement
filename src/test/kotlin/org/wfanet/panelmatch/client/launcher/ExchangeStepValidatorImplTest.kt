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
import com.google.protobuf.ByteString
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.panelmatch.client.common.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Party.DATA_PROVIDER
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Party.MODEL_PROVIDER
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.step
import org.wfanet.panelmatch.client.internal.copy
import org.wfanet.panelmatch.client.internal.exchangeWorkflow
import org.wfanet.panelmatch.client.launcher.InvalidExchangeStepException.FailureType.PERMANENT
import org.wfanet.panelmatch.client.launcher.InvalidExchangeStepException.FailureType.TRANSIENT
import org.wfanet.panelmatch.common.Fingerprinters.sha256
import org.wfanet.panelmatch.common.secrets.testing.TestSecretMap
import org.wfanet.panelmatch.common.testing.runBlockingTest

private val TEST_INSTANT = Instant.now()

private val TODAY = TEST_INSTANT.atZone(ZoneOffset.UTC).toLocalDate()
private val FIRST_EXCHANGE_DATE = TODAY.minusDays(10)

private const val RECURRING_EXCHANGE_ID = "some-recurring-exchange-id"
private const val EXCHANGE_ID = "some-exchange-id"
private const val EXCHANGE_STEP_ID = "some-exchange-step-id"
private const val EXCHANGE_STEP_ATTEMPT_ID = "some-exchange-step-attempt-id"

private const val OTHER_RECURRING_EXCHANGE_ID = "some-other-recurring-exchange-id"
private const val MISSING_RECURRING_EXCHANGE_ID = "some-missing-recurring-exchange-id"

private val EXCHANGE_WORKFLOW = exchangeWorkflow {
  firstExchangeDate = FIRST_EXCHANGE_DATE.toProtoDate()

  steps += step { party = MODEL_PROVIDER }
  steps += step { party = DATA_PROVIDER }
}

private val VALID_EXCHANGE_WORKFLOWS =
  TestSecretMap(
    RECURRING_EXCHANGE_ID to EXCHANGE_WORKFLOW.toByteString(),
    OTHER_RECURRING_EXCHANGE_ID to ByteString.EMPTY,
  )

private val ATTEMPT_KEY =
  ExchangeStepAttemptKey(
    recurringExchangeId = RECURRING_EXCHANGE_ID,
    exchangeId = EXCHANGE_ID,
    stepId = EXCHANGE_STEP_ID,
    attemptId = EXCHANGE_STEP_ATTEMPT_ID,
  )

private val EXCHANGE_STEP =
  ApiClient.ClaimedExchangeStep(
    attemptKey = ATTEMPT_KEY,
    exchangeDate = TODAY,
    stepIndex = 1,
    workflow = EXCHANGE_WORKFLOW,
    workflowFingerprint = sha256(EXCHANGE_WORKFLOW.toByteString()),
  )

@RunWith(JUnit4::class)
class ExchangeStepValidatorImplTest {

  private val validator =
    ExchangeStepValidatorImpl(
      DATA_PROVIDER,
      VALID_EXCHANGE_WORKFLOWS,
      Clock.fixed(TEST_INSTANT, ZoneOffset.UTC),
    )

  private fun validate(exchangeStep: ApiClient.ClaimedExchangeStep) = runBlockingTest {
    validator.validate(exchangeStep)
  }

  private fun assertValidationFailsPermanently(exchangeStep: ApiClient.ClaimedExchangeStep) {
    val e = assertFailsWith<InvalidExchangeStepException> { validate(exchangeStep) }
    assertThat(e.type).isEqualTo(PERMANENT)
  }

  private fun assertValidationFailsTransiently(exchangeStep: ApiClient.ClaimedExchangeStep) {
    val e = assertFailsWith<InvalidExchangeStepException> { validate(exchangeStep) }
    assertThat(e.type).isEqualTo(TRANSIENT)
  }

  @Test
  fun success() {
    validate(EXCHANGE_STEP) // Does not throw
  }

  @Test
  fun successOnFirstDay() {
    val exchangeStep = EXCHANGE_STEP.copy(exchangeDate = FIRST_EXCHANGE_DATE)
    validate(exchangeStep) // Does not throw
  }

  @Test
  fun differentExchangeWorkflow() {
    val wrongExchangeWorkflow =
      EXCHANGE_WORKFLOW.copy { firstExchangeDate = FIRST_EXCHANGE_DATE.minusDays(1).toProtoDate() }
    val wrongExchangeStep =
      EXCHANGE_STEP.copy(workflowFingerprint = sha256(wrongExchangeWorkflow.toByteString()))
    assertValidationFailsPermanently(wrongExchangeStep)
  }

  @Test
  fun missingRecurringExchange() {
    val wrongExchangeStep =
      EXCHANGE_STEP.copy(
        attemptKey = ATTEMPT_KEY.copy(recurringExchangeId = MISSING_RECURRING_EXCHANGE_ID)
      )
    assertValidationFailsTransiently(wrongExchangeStep)
  }

  @Test
  fun invalidExchangeWorkflow() {
    val exchangeStep =
      EXCHANGE_STEP.copy(
        attemptKey = ATTEMPT_KEY.copy(recurringExchangeId = OTHER_RECURRING_EXCHANGE_ID)
      )
    assertValidationFailsPermanently(exchangeStep)
  }

  @Test
  fun dateBeforeStart() {
    val exchangeStep = EXCHANGE_STEP.copy(exchangeDate = FIRST_EXCHANGE_DATE.minusDays(1))
    assertValidationFailsPermanently(exchangeStep)
  }

  @Test
  fun futureDate() {
    val exchangeStep = EXCHANGE_STEP.copy(exchangeDate = TODAY.plusDays(1))
    assertValidationFailsTransiently(exchangeStep)
  }

  @Test
  fun wrongParty() {
    val exchangeStep = EXCHANGE_STEP.copy(stepIndex = 0)
    assertValidationFailsPermanently(exchangeStep)
  }

  @Test
  fun negativeStepIndex() {
    val exchangeStep = EXCHANGE_STEP.copy(stepIndex = -1)
    assertValidationFailsPermanently(exchangeStep)
  }

  @Test
  fun tooHighStepIndex() {
    val exchangeStep = EXCHANGE_STEP.copy(stepIndex = EXCHANGE_WORKFLOW.stepsCount)
    assertValidationFailsPermanently(exchangeStep)
  }
}
