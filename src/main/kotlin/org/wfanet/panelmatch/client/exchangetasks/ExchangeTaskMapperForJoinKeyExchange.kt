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

import java.time.Clock
import java.time.Duration
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Step.StepCase
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient
import org.wfanet.panelmatch.protocol.common.DeterministicCommutativeCipher

/** Maps join key exchange steps to exchange tasks */
class ExchangeTaskMapperForJoinKeyExchange(
  private val deterministicCommutativeCryptor: DeterministicCommutativeCipher,
  private val sharedStorage: VerifiedStorageClient,
  private val privateStorage: VerifiedStorageClient,
  private val throttler: Throttler =
    MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(100))
) : ExchangeTaskMapper {

  override suspend fun getExchangeTaskForStep(step: ExchangeWorkflow.Step): ExchangeTask {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    return when (step.stepCase) {
      StepCase.ENCRYPT_STEP -> CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
      StepCase.REENCRYPT_STEP ->
        CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
      StepCase.DECRYPT_STEP -> CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
      StepCase.INPUT_STEP ->
        InputTask(
          sharedStorage = sharedStorage,
          privateStorage = privateStorage,
          step = step,
          throttler = throttler
        )
      StepCase.INTERSECT_AND_VALIDATE_STEP ->
        IntersectValidateTask(
          maxSize = step.intersectAndValidateStep.maxSize,
          minimumOverlap = step.intersectAndValidateStep.minimumOverlap
        )
      StepCase.EXECUTE_PRIVATE_MEMBERSHIP_QUERIES_STEP -> TODO()
      StepCase.BUILD_PRIVATE_MEMBERSHIP_QUERIES_STEP -> TODO()
      StepCase.DECRYPT_PRIVATE_MEMBERSHIP_QUERY_RESULTS_STEP -> TODO()
      else -> error("Unsupported step type")
    }
  }
}
