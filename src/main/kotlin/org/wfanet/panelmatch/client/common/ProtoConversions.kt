// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.common

import com.google.protobuf.Message
import kotlin.reflect.KMutableProperty
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow as V2AlphaExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.common.ProtoReflection.getDefaultInstance
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.buildPrivateMembershipQueriesStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.copyFromPreviousExchangeStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.copyFromSharedStorageStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.copyOptions
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.copyToSharedStorageStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.decryptPrivateMembershipQueryResultsStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.executePrivateMembershipQueriesStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.generateRandomBytesStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.generateSerializedRlweKeyPairStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.intersectAndValidateStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.exchangeIdentifiers
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.schedule
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.step
import org.wfanet.panelmatch.client.internal.exchangeWorkflow

/** Converts this [V2AlphaExchangeWorkflow] into a panel match internal [ExchangeWorkflow]. */
fun V2AlphaExchangeWorkflow.toInternal(): ExchangeWorkflow {
  val source = this
  return exchangeWorkflow {
    steps += source.stepsList.map { it.toInternal() }
    exchangeIdentifiers = source.exchangeIdentifiers.toInternal()
    firstExchangeDate = source.firstExchangeDate
    repetitionSchedule = source.repetitionSchedule.toInternal()
  }
}

private fun V2AlphaExchangeWorkflow.Step.toInternal(): ExchangeWorkflow.Step {
  val source = this
  return step {
    stepId = source.stepId
    party = source.party.toInternal()
    inputLabels.putAll(source.inputLabelsMap)
    outputLabels.putAll(source.outputLabelsMap)

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (source.stepCase) {
      V2AlphaExchangeWorkflow.Step.StepCase.COPY_FROM_SHARED_STORAGE_STEP ->
        copyStep(source.copyFromSharedStorageStep)
      V2AlphaExchangeWorkflow.Step.StepCase.COPY_TO_SHARED_STORAGE_STEP ->
        copyStep(source.copyToSharedStorageStep)
      V2AlphaExchangeWorkflow.Step.StepCase.INTERSECT_AND_VALIDATE_STEP ->
        copyStep(source.intersectAndValidateStep)
      V2AlphaExchangeWorkflow.Step.StepCase.COMMUTATIVE_DETERMINISTIC_ENCRYPT_STEP ->
        this::commutativeDeterministicEncryptStep.setDefaultInstance()
      V2AlphaExchangeWorkflow.Step.StepCase.COMMUTATIVE_DETERMINISTIC_REENCRYPT_STEP ->
        this::commutativeDeterministicReencryptStep.setDefaultInstance()
      V2AlphaExchangeWorkflow.Step.StepCase.COMMUTATIVE_DETERMINISTIC_DECRYPT_STEP ->
        this::commutativeDeterministicDecryptStep.setDefaultInstance()
      V2AlphaExchangeWorkflow.Step.StepCase.INPUT_STEP -> this::inputStep.setDefaultInstance()
      V2AlphaExchangeWorkflow.Step.StepCase.GENERATE_COMMUTATIVE_DETERMINISTIC_KEY_STEP ->
        this::generateCommutativeDeterministicKeyStep.setDefaultInstance()
      V2AlphaExchangeWorkflow.Step.StepCase.GENERATE_SERIALIZED_RLWE_KEY_PAIR_STEP ->
        copyStep(source.generateSerializedRlweKeyPairStep)
      V2AlphaExchangeWorkflow.Step.StepCase.EXECUTE_PRIVATE_MEMBERSHIP_QUERIES_STEP ->
        copyStep(source.executePrivateMembershipQueriesStep)
      V2AlphaExchangeWorkflow.Step.StepCase.BUILD_PRIVATE_MEMBERSHIP_QUERIES_STEP ->
        copyStep(source.buildPrivateMembershipQueriesStep)
      V2AlphaExchangeWorkflow.Step.StepCase.DECRYPT_PRIVATE_MEMBERSHIP_QUERY_RESULTS_STEP ->
        copyStep(source.decryptPrivateMembershipQueryResultsStep)
      V2AlphaExchangeWorkflow.Step.StepCase.GENERATE_CERTIFICATE_STEP ->
        this::generateCertificateStep.setDefaultInstance()
      V2AlphaExchangeWorkflow.Step.StepCase.PREPROCESS_EVENTS_STEP ->
        this::preprocessEventsStep.setDefaultInstance()
      V2AlphaExchangeWorkflow.Step.StepCase.COPY_FROM_PREVIOUS_EXCHANGE_STEP ->
        copyStep(source.copyFromPreviousExchangeStep)
      V2AlphaExchangeWorkflow.Step.StepCase.GENERATE_LOOKUP_KEYS_STEP ->
        this::generateLookupKeysStep.setDefaultInstance()
      V2AlphaExchangeWorkflow.Step.StepCase.HYBRID_ENCRYPT_STEP ->
        this::hybridEncryptStep.setDefaultInstance()
      V2AlphaExchangeWorkflow.Step.StepCase.HYBRID_DECRYPT_STEP ->
        this::hybridDecryptStep.setDefaultInstance()
      V2AlphaExchangeWorkflow.Step.StepCase.GENERATE_HYBRID_ENCRYPTION_KEY_PAIR_STEP ->
        this::generateHybridEncryptionKeyPairStep.setDefaultInstance()
      V2AlphaExchangeWorkflow.Step.StepCase.GENERATE_RANDOM_BYTES_STEP ->
        copyStep(source.generateRandomBytesStep)
      V2AlphaExchangeWorkflow.Step.StepCase.ASSIGN_JOIN_KEY_IDS_STEP ->
        this::assignJoinKeyIdsStep.setDefaultInstance()
      V2AlphaExchangeWorkflow.Step.StepCase.STEP_NOT_SET ->
        error("Invalid step case: ${source.stepCase}")
    }
  }
}

private inline fun <reified T : Message> KMutableProperty<T>.setDefaultInstance() {
  setter.call(getDefaultInstance<T>())
}

private fun ExchangeWorkflowKt.StepKt.Dsl.copyStep(
  v2AlphaStep: V2AlphaExchangeWorkflow.Step.CopyFromSharedStorageStep
) {
  copyFromSharedStorageStep = copyFromSharedStorageStep {
    copyOptions = v2AlphaStep.copyOptions.toInternal()
  }
}

private fun ExchangeWorkflowKt.StepKt.Dsl.copyStep(
  v2AlphaStep: V2AlphaExchangeWorkflow.Step.CopyToSharedStorageStep
) {
  copyToSharedStorageStep = copyToSharedStorageStep {
    copyOptions = v2AlphaStep.copyOptions.toInternal()
  }
}

private fun ExchangeWorkflowKt.StepKt.Dsl.copyStep(
  v2AlphaStep: V2AlphaExchangeWorkflow.Step.IntersectAndValidateStep
) {
  intersectAndValidateStep = intersectAndValidateStep {
    maxSize = v2AlphaStep.maxSize
    maximumNewItemsAllowed = v2AlphaStep.maximumNewItemsAllowed
  }
}

private fun ExchangeWorkflowKt.StepKt.Dsl.copyStep(
  v2AlphaStep: V2AlphaExchangeWorkflow.Step.GenerateSerializedRlweKeyPairStep
) {
  generateSerializedRlweKeyPairStep = generateSerializedRlweKeyPairStep {
    parameters = v2AlphaStep.parameters
  }
}

private fun ExchangeWorkflowKt.StepKt.Dsl.copyStep(
  v2AlphaStep: V2AlphaExchangeWorkflow.Step.ExecutePrivateMembershipQueriesStep
) {
  executePrivateMembershipQueriesStep = executePrivateMembershipQueriesStep {
    parameters = v2AlphaStep.parameters
    encryptedQueryResultFileCount = v2AlphaStep.encryptedQueryResultFileCount
    shardCount = v2AlphaStep.shardCount
    bucketsPerShard = v2AlphaStep.bucketsPerShard
    maxQueriesPerShard = v2AlphaStep.maxQueriesPerShard
  }
}

private fun ExchangeWorkflowKt.StepKt.Dsl.copyStep(
  v2AlphaStep: V2AlphaExchangeWorkflow.Step.BuildPrivateMembershipQueriesStep
) {
  buildPrivateMembershipQueriesStep = buildPrivateMembershipQueriesStep {
    parameters = v2AlphaStep.parameters
    encryptedQueryBundleFileCount = v2AlphaStep.encryptedQueryBundleFileCount
    queryIdToIdsFileCount = v2AlphaStep.queryIdToIdsFileCount
    shardCount = v2AlphaStep.shardCount
    bucketsPerShard = v2AlphaStep.bucketsPerShard
    queriesPerShard = v2AlphaStep.queriesPerShard
    addPaddingQueries = v2AlphaStep.addPaddingQueries
  }
}

private fun ExchangeWorkflowKt.StepKt.Dsl.copyStep(
  v2AlphaStep: V2AlphaExchangeWorkflow.Step.DecryptPrivateMembershipQueryResultsStep
) {
  decryptPrivateMembershipQueryResultsStep = decryptPrivateMembershipQueryResultsStep {
    parameters = v2AlphaStep.parameters
    decryptEventDataSetFileCount = v2AlphaStep.decryptEventDataSetFileCount
  }
}

private fun ExchangeWorkflowKt.StepKt.Dsl.copyStep(
  v2AlphaStep: V2AlphaExchangeWorkflow.Step.CopyFromPreviousExchangeStep
) {
  copyFromPreviousExchangeStep = copyFromPreviousExchangeStep {
    previousBlobKey = v2AlphaStep.previousBlobKey
  }
}

private fun ExchangeWorkflowKt.StepKt.Dsl.copyStep(
  v2AlphaStep: V2AlphaExchangeWorkflow.Step.GenerateRandomBytesStep
) {
  generateRandomBytesStep = generateRandomBytesStep { byteCount = v2AlphaStep.byteCount }
}

private fun V2AlphaExchangeWorkflow.Step.CopyOptions.toInternal():
  ExchangeWorkflow.Step.CopyOptions {
  val source = this
  return copyOptions {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    labelType =
      when (source.labelType) {
        V2AlphaExchangeWorkflow.Step.CopyOptions.LabelType.BLOB ->
          ExchangeWorkflow.Step.CopyOptions.LabelType.BLOB
        V2AlphaExchangeWorkflow.Step.CopyOptions.LabelType.MANIFEST ->
          ExchangeWorkflow.Step.CopyOptions.LabelType.MANIFEST
        V2AlphaExchangeWorkflow.Step.CopyOptions.LabelType.LABEL_TYPE_UNSPECIFIED,
        V2AlphaExchangeWorkflow.Step.CopyOptions.LabelType.UNRECOGNIZED ->
          error("Invalid label type: ${source.labelType}")
      }
  }
}

private fun V2AlphaExchangeWorkflow.ExchangeIdentifiers.toInternal():
  ExchangeWorkflow.ExchangeIdentifiers {
  val source = this
  return exchangeIdentifiers {
    dataProviderId = requireNotNull(DataProviderKey.fromName(source.dataProvider)).dataProviderId
    modelProviderId =
      requireNotNull(ModelProviderKey.fromName(source.modelProvider)).modelProviderId
    sharedStorageOwner = source.sharedStorageOwner.toInternal()

    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    storage =
      when (source.storage) {
        V2AlphaExchangeWorkflow.StorageType.GOOGLE_CLOUD_STORAGE ->
          ExchangeWorkflow.StorageType.GOOGLE_CLOUD_STORAGE
        V2AlphaExchangeWorkflow.StorageType.AMAZON_S3 -> ExchangeWorkflow.StorageType.AMAZON_S3
        V2AlphaExchangeWorkflow.StorageType.STORAGE_TYPE_UNSPECIFIED,
        V2AlphaExchangeWorkflow.StorageType.UNRECOGNIZED ->
          ExchangeWorkflow.StorageType.STORAGE_TYPE_UNSPECIFIED
      }
  }
}

private fun V2AlphaExchangeWorkflow.Schedule.toInternal(): ExchangeWorkflow.Schedule {
  val source = this
  return schedule { cronExpression = source.cronExpression }
}

private fun V2AlphaExchangeWorkflow.Party.toInternal(): ExchangeWorkflow.Party {
  return when (this) {
    V2AlphaExchangeWorkflow.Party.DATA_PROVIDER -> ExchangeWorkflow.Party.DATA_PROVIDER
    V2AlphaExchangeWorkflow.Party.MODEL_PROVIDER -> ExchangeWorkflow.Party.MODEL_PROVIDER
    V2AlphaExchangeWorkflow.Party.PARTY_UNSPECIFIED,
    V2AlphaExchangeWorkflow.Party.UNRECOGNIZED -> error("Invalid party: $this")
  }
}
