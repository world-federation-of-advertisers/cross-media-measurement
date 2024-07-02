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

import org.wfanet.panelmatch.client.common.ExchangeContext
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Step.StepCase

/** Maps join key exchange steps to exchange tasks */
abstract class ExchangeTaskMapper {

  suspend fun getExchangeTaskForStep(context: ExchangeContext): ExchangeTask {
    return with(context) {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (step.stepCase) {
        StepCase.COMMUTATIVE_DETERMINISTIC_ENCRYPT_STEP -> commutativeDeterministicEncrypt()
        StepCase.COMMUTATIVE_DETERMINISTIC_REENCRYPT_STEP -> commutativeDeterministicReEncrypt()
        StepCase.COMMUTATIVE_DETERMINISTIC_DECRYPT_STEP -> commutativeDeterministicDecrypt()
        StepCase.GENERATE_LOOKUP_KEYS_STEP -> generateLookupKeys()
        StepCase.INPUT_STEP -> input()
        StepCase.COPY_FROM_PREVIOUS_EXCHANGE_STEP -> copyFromPreviousExchange()
        StepCase.GENERATE_COMMUTATIVE_DETERMINISTIC_KEY_STEP ->
          generateCommutativeDeterministicEncryptionKey()
        StepCase.GENERATE_SERIALIZED_RLWE_KEY_PAIR_STEP -> generateSerializedRlweKeyPair()
        StepCase.GENERATE_CERTIFICATE_STEP -> generateExchangeCertificate()
        StepCase.INTERSECT_AND_VALIDATE_STEP -> intersectAndValidate()
        StepCase.PREPROCESS_EVENTS_STEP -> preprocessEvents()
        StepCase.EXECUTE_PRIVATE_MEMBERSHIP_QUERIES_STEP -> executePrivateMembershipQueries()
        StepCase.BUILD_PRIVATE_MEMBERSHIP_QUERIES_STEP -> buildPrivateMembershipQueries()
        StepCase.DECRYPT_PRIVATE_MEMBERSHIP_QUERY_RESULTS_STEP -> decryptMembershipResults()
        StepCase.COPY_FROM_SHARED_STORAGE_STEP -> copyFromSharedStorage()
        StepCase.COPY_TO_SHARED_STORAGE_STEP -> copyToSharedStorage()
        StepCase.HYBRID_ENCRYPT_STEP -> hybridEncrypt()
        StepCase.HYBRID_DECRYPT_STEP -> hybridDecrypt()
        StepCase.GENERATE_HYBRID_ENCRYPTION_KEY_PAIR_STEP -> generateHybridEncryptionKeyPair()
        StepCase.GENERATE_RANDOM_BYTES_STEP -> generateRandomBytes()
        StepCase.ASSIGN_JOIN_KEY_IDS_STEP -> assignJoinKeyIds()
        StepCase.STEP_NOT_SET ->
          throw IllegalArgumentException("Unsupported step type: ${step.stepCase}")
      }
    }
  }

  /** Returns the task that commutative encrypts. */
  abstract suspend fun ExchangeContext.commutativeDeterministicEncrypt(): ExchangeTask

  /** Returns the task that commutative decrypts. */
  abstract suspend fun ExchangeContext.commutativeDeterministicDecrypt(): ExchangeTask

  /** Returns the task that commutative re-encrypts. */
  abstract suspend fun ExchangeContext.commutativeDeterministicReEncrypt(): ExchangeTask

  /** Returns the task that generates a commutative encryption key. */
  abstract suspend fun ExchangeContext.generateCommutativeDeterministicEncryptionKey(): ExchangeTask

  /** Returns the task that preprocesses events. */
  abstract suspend fun ExchangeContext.preprocessEvents(): ExchangeTask

  /** Returns the task that builds private membership queries. */
  abstract suspend fun ExchangeContext.buildPrivateMembershipQueries(): ExchangeTask

  /** Returns the task that executes the private membership queries. */
  abstract suspend fun ExchangeContext.executePrivateMembershipQueries(): ExchangeTask

  /** Returns the task that decrypts the private membership queries. */
  abstract suspend fun ExchangeContext.decryptMembershipResults(): ExchangeTask

  /** Returns the task that generates serialized rlwe keys. */
  abstract suspend fun ExchangeContext.generateSerializedRlweKeyPair(): ExchangeTask

  /** Returns the task that generates a certificate. */
  abstract suspend fun ExchangeContext.generateExchangeCertificate(): ExchangeTask

  /** Returns the task that generates lookup keys. */
  abstract suspend fun ExchangeContext.generateLookupKeys(): ExchangeTask

  /** Returns the task that validates the step. */
  abstract suspend fun ExchangeContext.intersectAndValidate(): ExchangeTask

  /** Returns the task that gets the input. */
  abstract suspend fun ExchangeContext.input(): ExchangeTask

  /** Returns the task that copies from previous [Exchange]. */
  abstract suspend fun ExchangeContext.copyFromPreviousExchange(): ExchangeTask

  /** Returns the task that copies to a shared storage. */
  abstract suspend fun ExchangeContext.copyToSharedStorage(): ExchangeTask

  /** Returns the task that copies from the shared storage. */
  abstract suspend fun ExchangeContext.copyFromSharedStorage(): ExchangeTask

  /** Returns the task that encrypts using hybrid encryption. */
  abstract suspend fun ExchangeContext.hybridEncrypt(): ExchangeTask

  /** Returns the task that decrypts using hybrid encryption. */
  abstract suspend fun ExchangeContext.hybridDecrypt(): ExchangeTask

  /** Returns the task that generates hybrid encryption keys. */
  abstract suspend fun ExchangeContext.generateHybridEncryptionKeyPair(): ExchangeTask

  /** Returns the task that generates random bytes. */
  abstract suspend fun ExchangeContext.generateRandomBytes(): ExchangeTask

  /** Returns the task that assigns ids to each [JoinKey]. */
  abstract suspend fun ExchangeContext.assignJoinKeyIds(): ExchangeTask
}
