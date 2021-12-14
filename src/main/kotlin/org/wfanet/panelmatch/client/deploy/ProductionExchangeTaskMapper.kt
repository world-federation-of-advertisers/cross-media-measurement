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

package org.wfanet.panelmatch.client.deploy

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptions
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.panelmatch.client.common.ExchangeContext
import org.wfanet.panelmatch.client.exchangetasks.ApacheBeamContext
import org.wfanet.panelmatch.client.exchangetasks.ApacheBeamTask
import org.wfanet.panelmatch.client.exchangetasks.CopyFromPreviousExchangeTask
import org.wfanet.panelmatch.client.exchangetasks.CopyFromSharedStorageTask
import org.wfanet.panelmatch.client.exchangetasks.CopyToSharedStorageTask
import org.wfanet.panelmatch.client.exchangetasks.CryptorExchangeTask
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTask
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapper
import org.wfanet.panelmatch.client.exchangetasks.GenerateAsymmetricKeysTask
import org.wfanet.panelmatch.client.exchangetasks.GenerateExchangeCertificateTask
import org.wfanet.panelmatch.client.exchangetasks.GenerateSymmetricKeyTask
import org.wfanet.panelmatch.client.exchangetasks.InputTask
import org.wfanet.panelmatch.client.exchangetasks.IntersectValidateTask
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyHashingExchangeTask
import org.wfanet.panelmatch.client.exchangetasks.buildPrivateMembershipQueries
import org.wfanet.panelmatch.client.exchangetasks.decryptPrivateMembershipResults
import org.wfanet.panelmatch.client.exchangetasks.executePrivateMembershipQueries
import org.wfanet.panelmatch.client.privatemembership.CreateQueriesParameters
import org.wfanet.panelmatch.client.privatemembership.EvaluateQueriesParameters
import org.wfanet.panelmatch.client.privatemembership.JniPrivateMembershipCryptor
import org.wfanet.panelmatch.client.privatemembership.JniQueryEvaluator
import org.wfanet.panelmatch.client.privatemembership.JniQueryPreparer
import org.wfanet.panelmatch.client.privatemembership.JniQueryResultsDecryptor
import org.wfanet.panelmatch.client.storage.PrivateStorageSelector
import org.wfanet.panelmatch.client.storage.SharedStorageSelector
import org.wfanet.panelmatch.common.ShardedFileName
import org.wfanet.panelmatch.common.certificates.CertificateManager
import org.wfanet.panelmatch.common.crypto.JniDeterministicCommutativeCipher

class ProductionExchangeTaskMapper(
  private val inputTaskThrottler: Throttler,
  private val privateStorageSelector: PrivateStorageSelector,
  private val sharedStorageSelector: SharedStorageSelector,
  private val certificateManager: CertificateManager,
  private val pipelineOptions: PipelineOptions
) : ExchangeTaskMapper() {
  override suspend fun ExchangeContext.encrypt(): ExchangeTask {
    return CryptorExchangeTask.forEncryption(JniDeterministicCommutativeCipher())
  }

  override suspend fun ExchangeContext.decrypt(): ExchangeTask {
    return CryptorExchangeTask.forDecryption(JniDeterministicCommutativeCipher())
  }

  override suspend fun ExchangeContext.reEncrypt(): ExchangeTask {
    return CryptorExchangeTask.forReEncryption(JniDeterministicCommutativeCipher())
  }

  override suspend fun ExchangeContext.generateEncryptionKey(): ExchangeTask {
    return GenerateSymmetricKeyTask(JniDeterministicCommutativeCipher()::generateKey)
  }

  override suspend fun ExchangeContext.buildPrivateMembershipQueries(): ExchangeTask {
    check(step.stepCase == ExchangeWorkflow.Step.StepCase.BUILD_PRIVATE_MEMBERSHIP_QUERIES_STEP)
    val stepDetails = step.buildPrivateMembershipQueriesStep
    val privateMembershipCryptor = JniPrivateMembershipCryptor(stepDetails.serializedParameters)

    val outputManifests =
      mapOf(
        "encrypted-queries" to stepDetails.encryptedQueryBundleFileCount,
        "query-to-ids-map" to stepDetails.queryIdToIdsFileCount,
      )

    val parameters =
      CreateQueriesParameters(
        numShards = stepDetails.numShards,
        numBucketsPerShard = stepDetails.numBucketsPerShard,
        maxQueriesPerShard = stepDetails.numQueriesPerShard,
        padQueries = stepDetails.addPaddingQueries,
      )

    return apacheBeamTaskFor(outputManifests) {
      buildPrivateMembershipQueries(parameters, privateMembershipCryptor)
    }
  }

  override suspend fun ExchangeContext.executePrivateMembershipQueries(): ExchangeTask {
    check(step.stepCase == ExchangeWorkflow.Step.StepCase.EXECUTE_PRIVATE_MEMBERSHIP_QUERIES_STEP)
    val stepDetails = step.executePrivateMembershipQueriesStep

    val parameters =
      EvaluateQueriesParameters(
        numShards = stepDetails.numShards,
        numBucketsPerShard = stepDetails.numBucketsPerShard,
        maxQueriesPerShard = stepDetails.maxQueriesPerShard
      )

    val queryResultsEvaluator = JniQueryEvaluator(stepDetails.serializedParameters)

    val outputManifests = mapOf("encrypted-results" to stepDetails.encryptedQueryResultFileCount)

    return apacheBeamTaskFor(outputManifests) {
      executePrivateMembershipQueries(parameters, queryResultsEvaluator)
    }
  }

  override suspend fun ExchangeContext.decryptMembershipResults(): ExchangeTask {
    check(
      step.stepCase == ExchangeWorkflow.Step.StepCase.DECRYPT_PRIVATE_MEMBERSHIP_QUERY_RESULTS_STEP
    )
    val stepDetails = step.decryptPrivateMembershipQueryResultsStep

    val outputManifests = mapOf("decrypted-event-data" to stepDetails.decryptEventDataSetFileCount)

    return apacheBeamTaskFor(outputManifests) {
      decryptPrivateMembershipResults(stepDetails.serializedParameters, JniQueryResultsDecryptor())
    }
  }

  override suspend fun ExchangeContext.generateSymmetricKey(): ExchangeTask {
    return GenerateSymmetricKeyTask(JniDeterministicCommutativeCipher()::generateKey)
  }

  override suspend fun ExchangeContext.generateSerializedRlweKeys(): ExchangeTask {
    check(step.stepCase == ExchangeWorkflow.Step.StepCase.GENERATE_SERIALIZED_RLWE_KEYS_STEP)
    val privateMembershipCryptor =
      JniPrivateMembershipCryptor(step.generateSerializedRlweKeysStep.serializedParameters)
    return GenerateAsymmetricKeysTask(generateKeys = privateMembershipCryptor::generateKeys)
  }

  override suspend fun ExchangeContext.generateExchangeCertificate(): ExchangeTask {
    return GenerateExchangeCertificateTask(certificateManager, exchangeDateKey)
  }

  override suspend fun ExchangeContext.generateLookupKeys(): ExchangeTask {
    return JoinKeyHashingExchangeTask.forHashing(JniQueryPreparer())
  }

  override suspend fun ExchangeContext.intersectAndValidate(): ExchangeTask {
    check(step.stepCase == ExchangeWorkflow.Step.StepCase.INTERSECT_AND_VALIDATE_STEP)

    val maxSize = step.intersectAndValidateStep.maxSize
    val maximumNewItemsAllowed = step.intersectAndValidateStep.maximumNewItemsAllowed

    return IntersectValidateTask(
      maxSize = maxSize,
      maximumNewItemsAllowed = maximumNewItemsAllowed,
      isFirstExchange = date == workflow.firstExchangeDate.toLocalDate()
    )
  }

  override suspend fun ExchangeContext.input(): ExchangeTask {
    check(step.stepCase == ExchangeWorkflow.Step.StepCase.INPUT_STEP)
    require(step.inputLabelsMap.isEmpty())
    val blobKey = step.outputLabelsMap.values.single()
    return InputTask(
      storage = privateStorageSelector.getStorageClient(exchangeDateKey),
      throttler = inputTaskThrottler,
      blobKey = blobKey,
    )
  }

  override suspend fun ExchangeContext.copyFromPreviousExchange(): ExchangeTask {
    check(step.stepCase == ExchangeWorkflow.Step.StepCase.COPY_FROM_PREVIOUS_EXCHANGE_STEP)

    val previousBlobKey = step.inputLabelsMap.getValue("input")

    if (exchangeDateKey.date == workflow.firstExchangeDate.toLocalDate()) {
      return InputTask(
        previousBlobKey,
        inputTaskThrottler,
        privateStorageSelector.getStorageClient(exchangeDateKey)
      )
    }

    return CopyFromPreviousExchangeTask(
      privateStorageSelector,
      workflow.repetitionSchedule,
      exchangeDateKey,
      previousBlobKey
    )
  }

  override suspend fun ExchangeContext.copyFromSharedStorage(): ExchangeTask {
    check(step.stepCase == ExchangeWorkflow.Step.StepCase.COPY_FROM_SHARED_STORAGE_STEP)
    val source = sharedStorageSelector.getSharedStorage(workflow.exchangeIdentifiers.storage, this)
    val destination = privateStorageSelector.getStorageClient(exchangeDateKey)
    return CopyFromSharedStorageTask(
      source,
      destination,
      step.copyFromSharedStorageStep.copyOptions,
      step.inputLabelsMap.values.single(),
      step.outputLabelsMap.values.single()
    )
  }

  override suspend fun ExchangeContext.copyToSharedStorage(): ExchangeTask {
    check(step.stepCase == ExchangeWorkflow.Step.StepCase.COPY_TO_SHARED_STORAGE_STEP)
    val source = privateStorageSelector.getStorageClient(exchangeDateKey)
    val destination =
      sharedStorageSelector.getSharedStorage(workflow.exchangeIdentifiers.storage, this)
    return CopyToSharedStorageTask(
      source,
      destination,
      step.copyToSharedStorageStep.copyOptions,
      step.inputLabelsMap.values.single(),
      step.outputLabelsMap.values.single()
    )
  }

  private suspend fun ExchangeContext.apacheBeamTaskFor(
    outputManifests: Map<String, Int>,
    execute: suspend ApacheBeamContext.() -> Unit
  ): ApacheBeamTask {
    return ApacheBeamTask(
      Pipeline.create(pipelineOptions),
      privateStorageSelector.getStorageFactory(exchangeDateKey),
      step.inputLabelsMap,
      outputManifests.mapValues { (k, v) -> ShardedFileName(step.outputLabelsMap.getValue(k), v) },
      execute
    )
  }
}
