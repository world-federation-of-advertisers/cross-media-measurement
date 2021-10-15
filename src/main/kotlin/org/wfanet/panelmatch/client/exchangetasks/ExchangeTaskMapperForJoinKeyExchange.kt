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
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Step.StepCase
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.panelmatch.client.privatemembership.CreateQueriesParameters
import org.wfanet.panelmatch.client.privatemembership.PrivateMembershipCryptor
import org.wfanet.panelmatch.client.privatemembership.QueryResultsDecryptor
import org.wfanet.panelmatch.client.storage.StorageFactory
import org.wfanet.panelmatch.common.compression.CompressorFactory
import org.wfanet.panelmatch.common.crypto.DeterministicCommutativeCipher

/** Maps join key exchange steps to exchange tasks */
abstract class ExchangeTaskMapperForJoinKeyExchange : ExchangeTaskMapper {
  abstract val compressorFactory: CompressorFactory
  abstract val deterministicCommutativeCryptor: DeterministicCommutativeCipher
  abstract val getPrivateMembershipCryptor: (ByteString) -> PrivateMembershipCryptor
  abstract val queryResultsDecryptor: QueryResultsDecryptor
  abstract val privateStorage: StorageFactory
  abstract val inputTaskThrottler: Throttler

  override suspend fun getExchangeTaskForStep(step: ExchangeWorkflow.Step): ExchangeTask {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    return when (step.stepCase) {
      StepCase.ENCRYPT_STEP -> CryptorExchangeTask.forEncryption(deterministicCommutativeCryptor)
      StepCase.REENCRYPT_STEP ->
        CryptorExchangeTask.forReEncryption(deterministicCommutativeCryptor)
      StepCase.DECRYPT_STEP -> CryptorExchangeTask.forDecryption(deterministicCommutativeCryptor)
      StepCase.INPUT_STEP -> getInputStepTask(step)
      StepCase.INTERSECT_AND_VALIDATE_STEP -> getIntersectAndValidateStepTask(step)
      StepCase.GENERATE_COMMUTATIVE_DETERMINISTIC_KEY_STEP ->
        GenerateSymmetricKeyTask(generateKey = deterministicCommutativeCryptor::generateKey)
      StepCase.GENERATE_SERIALIZED_RLWE_KEYS_STEP -> getGenerateSerializedRLWEKeysStepTask(step)
      StepCase.GENERATE_CERTIFICATE_STEP -> TODO()
      StepCase.EXECUTE_PRIVATE_MEMBERSHIP_QUERIES_STEP -> TODO()
      StepCase.BUILD_PRIVATE_MEMBERSHIP_QUERIES_STEP -> getBuildPrivateMembershipQueriesTask(step)
      StepCase.DECRYPT_PRIVATE_MEMBERSHIP_QUERY_RESULTS_STEP ->
        getDecryptMembershipResultsTask(step)
      StepCase.COPY_FROM_SHARED_STORAGE_STEP -> TODO()
      StepCase.COPY_TO_SHARED_STORAGE_STEP -> TODO()
      else -> error("Unsupported step type")
    }
  }

  private fun getInputStepTask(step: ExchangeWorkflow.Step): ExchangeTask {
    require(step.stepCase == StepCase.INPUT_STEP)
    require(step.inputLabelsMap.isEmpty())
    val blobKey = step.outputLabelsMap.values.single()
    return InputTask(
      storage = privateStorage.build(),
      blobKey = blobKey,
      throttler = inputTaskThrottler
    )
  }

  private fun getIntersectAndValidateStepTask(step: ExchangeWorkflow.Step): ExchangeTask {
    require(step.stepCase == StepCase.INTERSECT_AND_VALIDATE_STEP)
    return IntersectValidateTask(
      maxSize = step.intersectAndValidateStep.maxSize,
      minimumOverlap = step.intersectAndValidateStep.minimumOverlap
    )
  }

  private fun getGenerateSerializedRLWEKeysStepTask(step: ExchangeWorkflow.Step): ExchangeTask {
    require(step.stepCase == StepCase.GENERATE_SERIALIZED_RLWE_KEYS_STEP)
    val privateMembershipCryptor =
      getPrivateMembershipCryptor(step.generateSerializedRlweKeysStep.serializedParameters)
    return GenerateAsymmetricKeysTask(generateKeys = privateMembershipCryptor::generateKeys)
  }

  private fun getBuildPrivateMembershipQueriesTask(step: ExchangeWorkflow.Step): ExchangeTask {
    require(step.stepCase == StepCase.BUILD_PRIVATE_MEMBERSHIP_QUERIES_STEP)
    val privateMembershipCryptor =
      getPrivateMembershipCryptor(step.buildPrivateMembershipQueriesStep.serializedParameters)
    val outputs =
      BuildPrivateMembershipQueriesTask.Outputs(
        encryptedQueryBundlesFileCount =
          step.buildPrivateMembershipQueriesStep.encryptedQueryBundleFileCount,
        encryptedQueryBundlesFileName = step.outputLabelsMap.getValue("encrypted-queries"),
        queryIdAndJoinKeysFileCount =
          step.buildPrivateMembershipQueriesStep.queryIdAndPanelistKeyFileCount,
        queryIdAndJoinKeysFileName = step.outputLabelsMap.getValue("query-decryption-keys"),
      )
    return BuildPrivateMembershipQueriesTask(
      storageFactory = privateStorage,
      parameters =
        CreateQueriesParameters(
          numShards = step.buildPrivateMembershipQueriesStep.numShards,
          numBucketsPerShard = step.buildPrivateMembershipQueriesStep.numBucketsPerShard,
          maxQueriesPerShard = step.buildPrivateMembershipQueriesStep.numQueriesPerShard,
          padQueries = step.buildPrivateMembershipQueriesStep.addPaddingQueries,
        ),
      privateMembershipCryptor = privateMembershipCryptor,
      outputs = outputs,
    )
  }

  private fun getDecryptMembershipResultsTask(step: ExchangeWorkflow.Step): ExchangeTask {
    require(step.stepCase == StepCase.DECRYPT_PRIVATE_MEMBERSHIP_QUERY_RESULTS_STEP)
    val outputs =
      DecryptPrivateMembershipResultsTask.Outputs(
        keyedDecryptedEventDataSetFileCount =
          step.decryptPrivateMembershipQueryResultsStep.decryptEventDataSetFileCount,
        keyedDecryptedEventDataSetFileName = step.outputLabelsMap.getValue("decrypted-event-data"),
      )
    return DecryptPrivateMembershipResultsTask(
      storageFactory = privateStorage,
      serializedParameters = step.decryptPrivateMembershipQueryResultsStep.serializedParameters,
      queryResultsDecryptor = queryResultsDecryptor,
      compressorFactory = compressorFactory,
      outputs = outputs,
    )
  }
}
