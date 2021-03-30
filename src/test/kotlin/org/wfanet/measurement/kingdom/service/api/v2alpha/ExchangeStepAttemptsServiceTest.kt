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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.AppendLogEntryRequest
import org.wfanet.measurement.api.v2alpha.CreateExchangeStepAttemptRequest
import org.wfanet.measurement.api.v2alpha.FinishExchangeStepAttemptRequest

@RunWith(JUnit4::class)
class ExchangeStepAttemptsServiceTest {

  private val service = ExchangeStepAttemptsService()

  @Test
  fun appendLogEntry() = runBlocking<Unit> {
    assertFailsWith(NotImplementedError::class) {
      service.appendLogEntry(AppendLogEntryRequest.getDefaultInstance())
    }
  }

  @Test
  fun createExchangeStepAttempt() = runBlocking<Unit> {
    assertFailsWith(NotImplementedError::class) {
      service.createExchangeStepAttempt(CreateExchangeStepAttemptRequest.getDefaultInstance())
    }
  }

  @Test
  fun finishExchangeStepAttempt() = runBlocking<Unit> {
    assertFailsWith(NotImplementedError::class) {
      service.finishExchangeStepAttempt(FinishExchangeStepAttemptRequest.getDefaultInstance())
    }
  }
}
