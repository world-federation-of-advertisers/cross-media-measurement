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

package org.wfanet.panelmatch.client.launcher.testing

import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Step
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.StepKt.inputStep
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.exchangeIdentifiers
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.step
import org.wfanet.measurement.api.v2alpha.exchangeWorkflow

fun buildWorkflow(
  testedStep: Step,
  dataProviderName: String,
  modelProviderName: String
): ExchangeWorkflow {
  return exchangeWorkflow {
    steps += testedStep

    exchangeIdentifiers = exchangeIdentifiers {
      dataProvider = dataProviderName
      modelProvider = modelProviderName
    }
  }
}

fun inputStep(label: Pair<String, String>): Step {
  return step {
    inputStep = inputStep {}
    outputLabels[label.first] = label.second
  }
}
