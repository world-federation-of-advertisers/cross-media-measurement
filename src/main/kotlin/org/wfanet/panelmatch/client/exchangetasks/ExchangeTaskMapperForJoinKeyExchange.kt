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

package org.wfanet.panelmatch.client.exchangetasks

import java.time.Duration
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.panelmatch.client.storage.Storage
import org.wfanet.panelmatch.protocol.common.Cryptor

/** Maps join key exchange steps to exchange tasks */
class ExchangeTaskMapperForJoinKeyExchange(
  val deterministicCommutativeCryptor: Cryptor,
  val retryDuration: Duration = Duration.ofMillis(100),
  val sharedStorage: Storage,
  val privateStorage: Storage
) : ExchangeTaskMapper {

  override suspend fun getExchangeTaskForStep(step: ExchangeWorkflow.Step): ExchangeTask {
    return when (step.getStepCase()) {
      ExchangeWorkflow.Step.StepCase.ENCRYPT_STEP ->
        CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
      ExchangeWorkflow.Step.StepCase.REENCRYPT_STEP ->
        CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
      ExchangeWorkflow.Step.StepCase.DECRYPT_STEP ->
        CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
      ExchangeWorkflow.Step.StepCase.INPUT_STEP ->
        InputTask(
          sharedStorage = sharedStorage,
          privateStorage = privateStorage,
          step = step,
          retryDuration = retryDuration
        )
      ExchangeWorkflow.Step.StepCase.INTERSECT_AND_VALIDATE_STEP ->
        IntersectValidateTask(
          maxSize = step.intersectAndValidateStep.maxSize,
          minimumOverlap = step.intersectAndValidateStep.minimumOverlap
        )
      else -> error("Unsupported step type")
    }
  }
}
