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
import kotlin.jvm.Throws
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow

/** Determines whether an ExchangeStep is valid and can be safely executed. */
interface ExchangeStepValidator {
  data class ValidatedExchangeStep(
    val workflow: ExchangeWorkflow,
    val step: ExchangeWorkflow.Step,
    val date: LocalDate
  )

  /** Throws [InvalidExchangeStepException] if [exchangeStep] is invalid. */
  @Throws(InvalidExchangeStepException::class)
  suspend fun validate(exchangeStep: ExchangeStep): ValidatedExchangeStep
}
