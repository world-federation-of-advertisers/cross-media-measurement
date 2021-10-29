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
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Party.DATA_PROVIDER
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Party.MODEL_PROVIDER
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.step
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.exchangeStep
import org.wfanet.measurement.api.v2alpha.exchangeWorkflow
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.panelmatch.client.launcher.InvalidExchangeStepException.FailureType.PERMANENT
import org.wfanet.panelmatch.client.launcher.InvalidExchangeStepException.FailureType.TRANSIENT
import org.wfanet.panelmatch.common.secrets.testing.TestSecretMap
import org.wfanet.panelmatch.common.testing.runBlockingTest

private val TEST_INSTANT = Instant.now()

private val TODAY = TEST_INSTANT.atZone(ZoneOffset.UTC).toLocalDate()
private val FIRST_EXCHANGE_DATE = TODAY.minusDays(10)

private const val RECURRING_EXCHANGE_ID = "some-recurring-exchange-id"
private const val EXCHANGE_ID = "some-exchange-id"
private const val EXCHANGE_STEP_ID = "some-exchange-step-id"

private const val OTHER_RECURRING_EXCHANGE_ID = "some-other-recurring-exchange-id"

private val EXCHANGE_WORKFLOW = exchangeWorkflow {
  firstExchangeDate = FIRST_EXCHANGE_DATE.toProtoDate()

  steps += step { party = MODEL_PROVIDER }
  steps += step { party = DATA_PROVIDER }
}

private val SERIALIZED_EXCHANGE_WORKFLOW = EXCHANGE_WORKFLOW.toByteString()

private val VALID_EXCHANGE_WORKFLOWS =
  TestSecretMap(
    mapOf(
      RECURRING_EXCHANGE_ID to SERIALIZED_EXCHANGE_WORKFLOW,
      OTHER_RECURRING_EXCHANGE_ID to ByteString.EMPTY
    )
  )

private val EXCHANGE_STEP = exchangeStep {
  name = ExchangeStepKey(RECURRING_EXCHANGE_ID, EXCHANGE_ID, EXCHANGE_STEP_ID).toName()
  stepIndex = 1
  serializedExchangeWorkflow = SERIALIZED_EXCHANGE_WORKFLOW
  exchangeDate = TODAY.toProtoDate()
}

@RunWith(JUnit4::class)
class ExchangeStepValidatorImplTest {

  private val validator =
    ExchangeStepValidatorImpl(
      DATA_PROVIDER,
      VALID_EXCHANGE_WORKFLOWS,
      Clock.fixed(TEST_INSTANT, ZoneOffset.UTC)
    )

  private fun validate(exchangeStep: ExchangeStep) = runBlockingTest {
    validator.validate(exchangeStep)
  }

  private fun assertValidationFailsPermanently(exchangeStep: ExchangeStep) {
    val e = assertFailsWith<InvalidExchangeStepException> { validate(exchangeStep) }
    assertThat(e.type).isEqualTo(PERMANENT)
  }

  private fun assertValidationFailsTransiently(exchangeStep: ExchangeStep) {
    val e = assertFailsWith<InvalidExchangeStepException> { validate(exchangeStep) }
    assertThat(e.type).isEqualTo(TRANSIENT)
  }

  @Test
  fun success() {
    validate(EXCHANGE_STEP) // Does not throw
  }

  @Test
  fun successOnFirstDay() {
    val exchangeStep = EXCHANGE_STEP.copy { exchangeDate = FIRST_EXCHANGE_DATE.toProtoDate() }
    validate(exchangeStep) // Does not throw
  }

  @Test
  fun differentExchangeWorkflow() {
    val wrongExchangeWorkflow =
      EXCHANGE_WORKFLOW.copy { firstExchangeDate = FIRST_EXCHANGE_DATE.minusDays(1).toProtoDate() }
    val wrongExchangeStep =
      EXCHANGE_STEP.copy { serializedExchangeWorkflow = wrongExchangeWorkflow.toByteString() }
    assertValidationFailsPermanently(wrongExchangeStep)
  }

  @Test
  fun invalidExchangeWorkflow() {
    val exchangeStep =
      EXCHANGE_STEP.copy {
        name = ExchangeStepKey(OTHER_RECURRING_EXCHANGE_ID, EXCHANGE_ID, EXCHANGE_STEP_ID).toName()
      }
    assertValidationFailsPermanently(exchangeStep)
  }

  @Test
  fun dateBeforeStart() {
    val exchangeStep =
      EXCHANGE_STEP.copy { exchangeDate = FIRST_EXCHANGE_DATE.minusDays(1).toProtoDate() }
    assertValidationFailsPermanently(exchangeStep)
  }

  @Test
  fun futureDate() {
    val exchangeStep = EXCHANGE_STEP.copy { exchangeDate = TODAY.plusDays(1).toProtoDate() }
    assertValidationFailsTransiently(exchangeStep)
  }

  @Test
  fun wrongParty() {
    val exchangeStep = EXCHANGE_STEP.copy { stepIndex = 0 }
    assertValidationFailsPermanently(exchangeStep)
  }

  @Test
  fun negativeStepIndex() {
    val exchangeStep = EXCHANGE_STEP.copy { stepIndex = -1 }
    assertValidationFailsPermanently(exchangeStep)
  }

  @Test
  fun tooHighStepIndex() {
    val exchangeStep = EXCHANGE_STEP.copy { stepIndex = EXCHANGE_WORKFLOW.stepsCount }
    assertValidationFailsPermanently(exchangeStep)
  }
}
