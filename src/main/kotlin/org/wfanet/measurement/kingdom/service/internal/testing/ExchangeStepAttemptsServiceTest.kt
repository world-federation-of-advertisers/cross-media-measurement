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

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.type.Date
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.lang.IllegalArgumentException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.FinishExchangeStepAttemptRequest

private const val FIXED_GENERATED_INTERNAL_ID = 2345L
private const val FIXED_GENERATED_EXTERNAL_ID = 6789L
private val idGenerator =
  FixedIdGenerator(InternalId(FIXED_GENERATED_INTERNAL_ID), ExternalId(FIXED_GENERATED_EXTERNAL_ID))
private const val STEP_INDEX = 1

private val DATE =
  Date.newBuilder()
    .apply {
      year = 2021
      month = 1
      day = 15
    }
    .build()

@RunWith(JUnit4::class)
abstract class ExchangeStepAttemptsServiceTest {

  /** Creates a /ExchangeStepAttempts service implementation using [idGenerator]. */
  protected abstract fun newService(idGenerator: IdGenerator): ExchangeStepAttemptsCoroutineImplBase

  private lateinit var exchangeStepAttemptsService: ExchangeStepAttemptsCoroutineImplBase

  @Before
  fun initService() {
    exchangeStepAttemptsService = newService(idGenerator)
  }

  @Test
  fun `finishExchangeStepAttempt fails for missing date`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        exchangeStepAttemptsService.finishExchangeStepAttempt(
          FinishExchangeStepAttemptRequest.getDefaultInstance()
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Date must be provided in the request.")
  }

  @Test
  fun `finishExchangeStepAttempt fails without exchange step attempt`() = runBlocking {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        exchangeStepAttemptsService.finishExchangeStepAttempt(
          FinishExchangeStepAttemptRequest.newBuilder()
            .apply {
              externalRecurringExchangeId = 1L
              stepIndex = STEP_INDEX
              attemptNumber = 1
              date = DATE
            }
            .build()
        )
      }

    assertThat(exception).hasMessageThat().contains("Attempt for Step: $STEP_INDEX not found.")
  }

  @Test
  fun `finishExchangeStepAttempt succeeds`() = runBlocking {
    // TODO(yunyeng): Add test once underlying services complete.
  }

  @Test
  fun `finishExchangeStepAttempt fails temporarily`() = runBlocking {
    // TODO(yunyeng): Add test once underlying services complete.
  }

  @Test
  fun `finishExchangeStepAttempt fails permanently`() = runBlocking {
    // TODO(yunyeng): Add test once underlying services complete.
  }
}
