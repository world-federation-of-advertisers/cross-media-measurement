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

import com.google.protobuf.ByteString
import java.time.Clock
import java.time.Duration
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Step.StepCase
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.panelmatch.client.privatemembership.PrivateMembershipCryptor
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient
import org.wfanet.panelmatch.common.crypto.DeterministicCommutativeCipher

/** Maps join key exchange steps to exchange tasks */
class ExchangeTaskMapperForJoinKeyExchange(
  private val getDeterministicCommutativeCryptor: () -> DeterministicCommutativeCipher,
  private val getPrivateMembershipCryptor: (ByteString) -> PrivateMembershipCryptor,
  private val privateStorage: VerifiedStorageClient,
  private val throttler: Throttler =
    MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(100))
) : ExchangeTaskMapper {

  override suspend fun getExchangeTaskForStep(step: ExchangeWorkflow.Step): ExchangeTask {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    return when (step.stepCase) {
      StepCase.ENCRYPT_STEP ->
        CryptorExchangeTask.forEncryption(getDeterministicCommutativeCryptor())
      StepCase.REENCRYPT_STEP ->
        CryptorExchangeTask.forReEncryption(getDeterministicCommutativeCryptor())
      StepCase.DECRYPT_STEP ->
        CryptorExchangeTask.forDecryption(getDeterministicCommutativeCryptor())
      StepCase.INPUT_STEP -> InputTask(storage = privateStorage, step = step, throttler = throttler)
      StepCase.INTERSECT_AND_VALIDATE_STEP ->
        IntersectValidateTask(
          maxSize = step.intersectAndValidateStep.maxSize,
          minimumOverlap = step.intersectAndValidateStep.minimumOverlap
        )
      StepCase.GENERATE_COMMUTATIVE_DETERMINISTIC_KEY_STEP ->
        GenerateSymmetricKeyTask(generateKey = getDeterministicCommutativeCryptor()::generateKey)
      StepCase.GENERATE_SERIALIZED_RLWE_KEYS_STEP -> {
        val privateMembershipCryptor =
          getPrivateMembershipCryptor(step.generateSerializedRlweKeysStep.serializedParameters)
        GenerateAsymmetricKeysTask(generateKeys = privateMembershipCryptor::generateKeys)
      }
      StepCase.GENERATE_CERTIFICATE_STEP -> TODO()
      StepCase.EXECUTE_PRIVATE_MEMBERSHIP_QUERIES_STEP -> TODO()
      StepCase.BUILD_PRIVATE_MEMBERSHIP_QUERIES_STEP -> TODO()
      StepCase.DECRYPT_PRIVATE_MEMBERSHIP_QUERY_RESULTS_STEP -> TODO()
      StepCase.COPY_FROM_SHARED_STORAGE_STEP -> TODO()
      StepCase.COPY_TO_SHARED_STORAGE_STEP -> TODO()
      else -> error("Unsupported step type")
    }
  }
}
