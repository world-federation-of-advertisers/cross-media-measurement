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

import com.google.protobuf.kotlin.toByteStringUtf8
import java.time.format.DateTimeFormatter
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptions
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.panelmatch.client.common.ExchangeContext
import org.wfanet.panelmatch.client.common.TaskParameters
import org.wfanet.panelmatch.client.eventpreprocessing.HardCodedDeterministicCommutativeCipherKeyProvider
import org.wfanet.panelmatch.client.eventpreprocessing.HardCodedHkdfPepperProvider
import org.wfanet.panelmatch.client.eventpreprocessing.HardCodedIdentifierHashPepperProvider
import org.wfanet.panelmatch.client.eventpreprocessing.JniEventPreprocessor
import org.wfanet.panelmatch.client.eventpreprocessing.PreprocessingParameters
import org.wfanet.panelmatch.client.exchangetasks.ApacheBeamContext
import org.wfanet.panelmatch.client.exchangetasks.ApacheBeamTask
import org.wfanet.panelmatch.client.exchangetasks.AssignJoinKeyIdsTask
import org.wfanet.panelmatch.client.exchangetasks.CopyFromPreviousExchangeTask
import org.wfanet.panelmatch.client.exchangetasks.CopyFromSharedStorageTask
import org.wfanet.panelmatch.client.exchangetasks.CopyToSharedStorageTask
import org.wfanet.panelmatch.client.exchangetasks.DeterministicCommutativeCipherTask
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTask
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapper
import org.wfanet.panelmatch.client.exchangetasks.GenerateAsymmetricKeyPairTask
import org.wfanet.panelmatch.client.exchangetasks.GenerateHybridEncryptionKeyPairTask
import org.wfanet.panelmatch.client.exchangetasks.HybridDecryptTask
import org.wfanet.panelmatch.client.exchangetasks.HybridEncryptTask
import org.wfanet.panelmatch.client.exchangetasks.InputTask
import org.wfanet.panelmatch.client.exchangetasks.IntersectValidateTask
import org.wfanet.panelmatch.client.exchangetasks.JoinKeyHashingExchangeTask
import org.wfanet.panelmatch.client.exchangetasks.ProducerTask
import org.wfanet.panelmatch.client.exchangetasks.buildPrivateMembershipQueries
import org.wfanet.panelmatch.client.exchangetasks.copyFromSharedStorage
import org.wfanet.panelmatch.client.exchangetasks.copyToSharedStorage
import org.wfanet.panelmatch.client.exchangetasks.decryptPrivateMembershipResults
import org.wfanet.panelmatch.client.exchangetasks.executePrivateMembershipQueries
import org.wfanet.panelmatch.client.exchangetasks.preprocessEvents
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Step.CopyOptions
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
import org.wfanet.panelmatch.common.crypto.generateSecureRandomByteString

/**
 * Concrete [ExchangeTaskMapper] implementation.
 *
 * [makePipelineOptions] must return a different object on each invocation.
 */
open class ProductionExchangeTaskMapper(
  private val inputTaskThrottler: Throttler,
  private val privateStorageSelector: PrivateStorageSelector,
  private val sharedStorageSelector: SharedStorageSelector,
  private val certificateManager: CertificateManager,
  private val makePipelineOptions: () -> PipelineOptions,
  private val taskContext: TaskParameters,
) : ExchangeTaskMapper() {
  override suspend fun ExchangeContext.commutativeDeterministicEncrypt(): ExchangeTask {
    return DeterministicCommutativeCipherTask.forEncryption(JniDeterministicCommutativeCipher())
  }

  override suspend fun ExchangeContext.commutativeDeterministicDecrypt(): ExchangeTask {
    return DeterministicCommutativeCipherTask.forDecryption(JniDeterministicCommutativeCipher())
  }

  override suspend fun ExchangeContext.commutativeDeterministicReEncrypt(): ExchangeTask {
    return DeterministicCommutativeCipherTask.forReEncryption(JniDeterministicCommutativeCipher())
  }

  override suspend fun ExchangeContext.generateCommutativeDeterministicEncryptionKey():
    ExchangeTask {
    val cipher = JniDeterministicCommutativeCipher()
    return ProducerTask("symmetric-key") { cipher.generateKey() }
  }

  override suspend fun ExchangeContext.preprocessEvents(): ExchangeTask {
    check(step.stepCase == ExchangeWorkflow.Step.StepCase.PREPROCESS_EVENTS_STEP)
    val eventPreprocessor = JniEventPreprocessor()
    val deterministicCommutativeCipherKeyProvider =
      ::HardCodedDeterministicCommutativeCipherKeyProvider
    val hkdfPepperProvider = ::HardCodedHkdfPepperProvider
    val identifierHashPepperProvider = ::HardCodedIdentifierHashPepperProvider
    val preprocessingParameters: PreprocessingParameters =
      requireNotNull(taskContext.get(PreprocessingParameters::class)) {
        "PreprocessingParameters must be set in taskContext"
      }
    val outputsManifests = mapOf("preprocessed-event-data" to preprocessingParameters.fileCount)
    return apacheBeamTaskFor(outputsManifests, emptyList()) {
      preprocessEvents(
        eventPreprocessor = eventPreprocessor,
        deterministicCommutativeCipherKeyProvider = deterministicCommutativeCipherKeyProvider,
        identifierPepperProvider = identifierHashPepperProvider,
        hkdfPepperProvider = hkdfPepperProvider,
        maxByteSize = preprocessingParameters.maxByteSize,
      )
    }
  }

  override suspend fun ExchangeContext.buildPrivateMembershipQueries(): ExchangeTask {
    check(step.stepCase == ExchangeWorkflow.Step.StepCase.BUILD_PRIVATE_MEMBERSHIP_QUERIES_STEP)
    val stepDetails = step.buildPrivateMembershipQueriesStep
    val privateMembershipCryptor = JniPrivateMembershipCryptor(stepDetails.parameters)

    val outputManifests =
      mapOf(
        "encrypted-queries" to stepDetails.encryptedQueryBundleFileCount,
        "query-to-ids-map" to stepDetails.queryIdToIdsFileCount,
      )

    val parameters =
      CreateQueriesParameters(
        numShards = stepDetails.shardCount,
        numBucketsPerShard = stepDetails.bucketsPerShard,
        maxQueriesPerShard = stepDetails.queriesPerShard,
        padQueries = stepDetails.addPaddingQueries,
      )
    // TODO: remove this functionality v2.0.0
    // For backwards compatibility for workflows without discarded-join-keys
    val outputLabels: List<String> =
      if ("discarded-join-keys" in step.outputLabelsMap) {
        listOf("discarded-join-keys")
      } else {
        emptyList()
      }
    return apacheBeamTaskFor(outputManifests, outputLabels) {
      buildPrivateMembershipQueries(parameters, privateMembershipCryptor)
    }
  }

  override suspend fun ExchangeContext.executePrivateMembershipQueries(): ExchangeTask {
    check(step.stepCase == ExchangeWorkflow.Step.StepCase.EXECUTE_PRIVATE_MEMBERSHIP_QUERIES_STEP)
    val stepDetails = step.executePrivateMembershipQueriesStep

    val parameters =
      EvaluateQueriesParameters(
        numShards = stepDetails.shardCount,
        numBucketsPerShard = stepDetails.bucketsPerShard,
        maxQueriesPerShard = stepDetails.maxQueriesPerShard,
      )

    val queryResultsEvaluator = JniQueryEvaluator(stepDetails.parameters)

    val outputManifests = mapOf("encrypted-results" to stepDetails.encryptedQueryResultFileCount)
    val outputLabels = listOf("padding-nonces")

    return apacheBeamTaskFor(outputManifests, outputLabels) {
      executePrivateMembershipQueries(parameters, queryResultsEvaluator)
    }
  }

  override suspend fun ExchangeContext.decryptMembershipResults(): ExchangeTask {
    check(
      step.stepCase == ExchangeWorkflow.Step.StepCase.DECRYPT_PRIVATE_MEMBERSHIP_QUERY_RESULTS_STEP
    )
    val stepDetails = step.decryptPrivateMembershipQueryResultsStep

    val outputManifests = mapOf("decrypted-event-data" to stepDetails.decryptEventDataSetFileCount)

    return apacheBeamTaskFor(outputManifests, emptyList()) {
      decryptPrivateMembershipResults(stepDetails.parameters, JniQueryResultsDecryptor())
    }
  }

  override suspend fun ExchangeContext.generateSerializedRlweKeyPair(): ExchangeTask {
    check(step.stepCase == ExchangeWorkflow.Step.StepCase.GENERATE_SERIALIZED_RLWE_KEY_PAIR_STEP)
    val privateMembershipCryptor =
      JniPrivateMembershipCryptor(step.generateSerializedRlweKeyPairStep.parameters)
    return GenerateAsymmetricKeyPairTask(generateKeys = privateMembershipCryptor::generateKeys)
  }

  override suspend fun ExchangeContext.generateExchangeCertificate(): ExchangeTask {
    return ProducerTask("certificate-resource-name") {
      certificateManager.createForExchange(exchangeDateKey).toByteStringUtf8()
    }
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
      isFirstExchange = date == workflow.firstExchangeDate.toLocalDate(),
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
        privateStorageSelector.getStorageClient(exchangeDateKey),
      )
    }

    return CopyFromPreviousExchangeTask(
      privateStorageSelector,
      workflow.repetitionSchedule,
      exchangeDateKey,
      previousBlobKey,
    )
  }

  override suspend fun ExchangeContext.copyFromSharedStorage(): ExchangeTask {
    check(step.stepCase == ExchangeWorkflow.Step.StepCase.COPY_FROM_SHARED_STORAGE_STEP)

    val copyOptions = step.copyFromSharedStorageStep.copyOptions
    val destinationBlobKey = step.outputLabelsMap.values.single()
    val sourceBlobKey = step.inputLabelsMap.values.single()
    val source =
      sharedStorageSelector.getVerifyingStorage(
        sourceBlobKey,
        workflow.exchangeIdentifiers.storage,
        this,
      )

    return when (copyOptions.labelType) {
      CopyOptions.LabelType.BLOB -> {
        CopyFromSharedStorageTask(
          source = source,
          destination = privateStorageSelector.getStorageClient(exchangeDateKey),
          copyOptions = copyOptions,
          sourceBlobKey = sourceBlobKey,
          destinationBlobKey = destinationBlobKey,
        )
      }
      CopyOptions.LabelType.MANIFEST -> {
        apacheBeamTaskFor(
          outputManifests = mapOf(),
          outputBlobs = emptyList(),
          skipReadInput = true,
        ) {
          copyFromSharedStorage(
            source = source,
            destinationFactory = privateStorageSelector.getStorageFactory(exchangeDateKey),
            copyOptions = copyOptions,
            sourceManifestBlobKey = sourceBlobKey,
            destinationManifestBlobKey = destinationBlobKey,
          )
        }
      }
      else -> error("Unrecognized CopyOptions: $copyOptions")
    }
  }

  override suspend fun ExchangeContext.copyToSharedStorage(): ExchangeTask {
    check(step.stepCase == ExchangeWorkflow.Step.StepCase.COPY_TO_SHARED_STORAGE_STEP)

    val copyOptions = step.copyToSharedStorageStep.copyOptions
    val destination =
      sharedStorageSelector.getSigningStorage(workflow.exchangeIdentifiers.storage, this)
    val sourceLabel = step.inputLabelsMap.keys.single { it != "certificate-resource-name" }
    val sourceBlobKey = step.inputLabelsMap.getValue(sourceLabel)
    val destinationBlobKey = step.outputLabelsMap.values.single()

    return when (copyOptions.labelType) {
      CopyOptions.LabelType.BLOB -> {
        CopyToSharedStorageTask(
          source = privateStorageSelector.getStorageClient(exchangeDateKey),
          destination = destination,
          copyOptions = copyOptions,
          sourceBlobKey = sourceBlobKey,
          destinationBlobKey = destinationBlobKey,
        )
      }
      CopyOptions.LabelType.MANIFEST -> {
        apacheBeamTaskFor(outputManifests = emptyMap(), outputBlobs = emptyList()) {
          copyToSharedStorage(
            sourceFactory = privateStorageSelector.getStorageFactory(exchangeDateKey),
            destination = destination,
            copyOptions = copyOptions,
            sourceManifestLabel = sourceLabel,
            destinationManifestBlobKey = destinationBlobKey,
          )
        }
      }
      else -> error("Unrecognized CopyOptions: $copyOptions")
    }
  }

  override suspend fun ExchangeContext.hybridEncrypt(): ExchangeTask {
    return HybridEncryptTask()
  }

  override suspend fun ExchangeContext.hybridDecrypt(): ExchangeTask {
    return HybridDecryptTask()
  }

  override suspend fun ExchangeContext.generateHybridEncryptionKeyPair(): ExchangeTask {
    return GenerateHybridEncryptionKeyPairTask()
  }

  override suspend fun ExchangeContext.generateRandomBytes(): ExchangeTask {
    val numBytes = step.generateRandomBytesStep.byteCount
    return ProducerTask("random-bytes") { generateSecureRandomByteString(numBytes) }
  }

  override suspend fun ExchangeContext.assignJoinKeyIds(): ExchangeTask {
    return AssignJoinKeyIdsTask()
  }

  private suspend fun ExchangeContext.apacheBeamTaskFor(
    outputManifests: Map<String, Int>,
    outputBlobs: List<String>,
    skipReadInput: Boolean = false,
    execute: suspend ApacheBeamContext.() -> Unit,
  ): ApacheBeamTask {
    val pipelineOptions = makePipelineOptions()
    pipelineOptions.jobName =
      listOf(
          exchangeDateKey.recurringExchangeId,
          exchangeDateKey.date.format(DateTimeFormatter.ofPattern("yyyyMMdd")),
          step.stepId,
          attemptKey.attemptId,
        )
        .joinToString("-")
        .replace('_', '-')

    return ApacheBeamTask(
      Pipeline.create(pipelineOptions),
      privateStorageSelector.getStorageFactory(exchangeDateKey),
      step.inputLabelsMap,
      outputBlobs.associateWith { step.outputLabelsMap.getValue(it) },
      outputManifests.mapValues { (k, v) -> ShardedFileName(step.outputLabelsMap.getValue(k), v) },
      skipReadInput,
      execute,
    )
  }
}
