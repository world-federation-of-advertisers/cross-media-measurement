/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.Parser
import java.nio.file.Paths
import java.security.cert.X509Certificate
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.computation.KAnonymityParams
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.NoiseParams.NoiseType
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.StorageParams
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * Application for fulfilling results in the CMMS.
 *
 * This class extends [BaseTeeApplication] and is responsible for processing work items related to
 * results fulfillment. It orchestrates the retrieval of storage configurations, certificate and key
 * loading, and the invocation of the [ResultsFulfiller] to process requisitions.
 *
 * @param subscriptionId The subscription ID for the queue subscriber.
 * @param queueSubscriber The [QueueSubscriber] instance for receiving work items.
 * @param parser The protobuf [Parser] for [WorkItem] messages.
 * @param workItemsClient gRPC client stub for [WorkItemsGrpcKt.WorkItemsCoroutineStub].
 * @param workItemAttemptsClient gRPC client stub for
 *   [WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub].
 * @param requisitionMetadataStub used to sync [Requisition]s with RequisitionMetadataStorage
 * @param impressionMetadataStub used to get impressions metadata from ImpressionMetadataStorage
 * @param requisitionStubFactory Factory for creating requisition stubs.
 * @param kmsClient The Tink [KmsClient] for key management.
 * @param getImpressionsMetadataStorageConfig Lambda to obtain [StorageConfig] for impressions
 *   metadata.
 * @param getImpressionsStorageConfig Lambda to obtain [StorageConfig] for impressions.
 * @param getRequisitionsStorageConfig Lambda to obtain [StorageConfig] for requisitions.
 * @param modelLineInfoMap map of model line to [ModelLineInfo]
 * @param pipelineConfiguration Configuration for the event processing pipeline.
 * @param metrics Metrics recorder for telemetry.
 * @constructor Initializes the application with all required dependencies for result fulfillment.
 */
class ResultsFulfillerApp(
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<WorkItem>,
  workItemsClient: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  workItemAttemptsClient: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub,
  private val requisitionMetadataStub: RequisitionMetadataServiceCoroutineStub,
  private val impressionMetadataStub: ImpressionMetadataServiceCoroutineStub,
  private val requisitionStubFactory: RequisitionStubFactory,
  private val kmsClients: MutableMap<String, KmsClient>,
  private val getImpressionsMetadataStorageConfig: (StorageParams) -> StorageConfig,
  private val getImpressionsStorageConfig: (StorageParams) -> StorageConfig,
  private val getRequisitionsStorageConfig: (StorageParams) -> StorageConfig,
  private val modelLineInfoMap: Map<String, ModelLineInfo>,
  private val pipelineConfiguration: PipelineConfiguration = DEFAULT_PIPELINE_CONFIGURATION,
  private val metrics: ResultsFulfillerMetrics,
) :
  BaseTeeApplication(
    subscriptionId = subscriptionId,
    queueSubscriber = queueSubscriber,
    parser = parser,
    workItemsStub = workItemsClient,
    workItemAttemptsStub = workItemAttemptsClient,
  ) {

  override suspend fun runWork(message: Any) {
    val workItemParams = message.unpack(WorkItemParams::class.java)
    val fulfillerParams = workItemParams.appParams.unpack(ResultsFulfillerParams::class.java)
    val requisitionsBlobUri = workItemParams.dataPathParams.dataPath

    val storageParams = fulfillerParams.storageParams

    val requisitionsStorageConfig = getRequisitionsStorageConfig(storageParams)
    val impressionsMetadataStorageConfig = getImpressionsMetadataStorageConfig(storageParams)
    val impressionsStorageConfig = getImpressionsStorageConfig(storageParams)

    // Load grouped requisitions from storage
    val groupedRequisitions =
      loadGroupedRequisitions(requisitionsBlobUri, requisitionsStorageConfig)
    val requisitionsStub = requisitionStubFactory.buildRequisitionsStub(fulfillerParams)

    val requisitionFulfillmentStubsMap =
      requisitionStubFactory.buildRequisitionFulfillmentStubs(fulfillerParams)
    val dataProviderCertificateKey =
      checkNotNull(
        DataProviderCertificateKey.fromName(fulfillerParams.consentParams.edpCertificateName)
      )
    val consentCertificateFile =
      checkNotNull(
          getRuntimePath(Paths.get(fulfillerParams.consentParams.resultCsCertDerResourcePath))
        )
        .toFile()
    val consentPrivateKeyFile =
      checkNotNull(
          getRuntimePath(Paths.get(fulfillerParams.consentParams.resultCsPrivateKeyDerResourcePath))
        )
        .toFile()
    val encryptionPrivateKeyFile =
      checkNotNull(
          getRuntimePath(Paths.get(fulfillerParams.consentParams.privateEncryptionKeyResourcePath))
        )
        .toFile()
    val consentCertificate: X509Certificate =
      consentCertificateFile.inputStream().use { input -> readCertificate(input) }
    val consentPrivateEncryptionKey =
      readPrivateKey(consentPrivateKeyFile.readByteString(), consentCertificate.publicKey.algorithm)
    val dataProviderResultSigningKeyHandle =
      SigningKeyHandle(consentCertificate, consentPrivateEncryptionKey)

    val kmsClient = kmsClients[fulfillerParams.dataProvider]
    requireNotNull(kmsClient) { "KMS client not found for ${fulfillerParams.dataProvider}" }

    val impressionsDataSourceProvider =
      ImpressionDataSourceProvider(
        impressionMetadataStub = impressionMetadataStub,
        dataProvider = fulfillerParams.dataProvider,
        impressionsMetadataStorageConfig = impressionsMetadataStorageConfig,
      )
    val noiseSelector =
      when (fulfillerParams.noiseParams.noiseType) {
        NoiseType.NONE -> NoNoiserSelector()
        NoiseType.CONTINUOUS_GAUSSIAN -> ContinuousGaussianNoiseSelector()
        else -> throw Exception("Invalid noise type ${fulfillerParams.noiseParams.noiseType}")
      }

    val kAnonymityParams: KAnonymityParams? =
      if (fulfillerParams.hasKAnonymityParams()) {
        require(fulfillerParams.kAnonymityParams.minUsers > 0) {
          "k-anonymity minUsers must be greater than 0, got ${fulfillerParams.kAnonymityParams.minUsers}"
        }
        require(fulfillerParams.kAnonymityParams.minImpressions > 0) {
          "k-anonymity minImpressions must be greater than 0, got ${fulfillerParams.kAnonymityParams.minImpressions}"
        }
        require(fulfillerParams.kAnonymityParams.reachMaxFrequencyPerUser > 0) {
          "k-anonymity reachMaxFrequencyPerUser must be greater than 0, got ${fulfillerParams.kAnonymityParams.reachMaxFrequencyPerUser}"
        }
        KAnonymityParams(
          minUsers = fulfillerParams.kAnonymityParams.minUsers,
          minImpressions = fulfillerParams.kAnonymityParams.minImpressions,
          reachMaxFrequencyPerUser = fulfillerParams.kAnonymityParams.reachMaxFrequencyPerUser,
        )
      } else {
        null
      }

    require(
      fulfillerParams.impressionMaxFrequencyPerUser >= -1 &&
        fulfillerParams.impressionMaxFrequencyPerUser <= Byte.MAX_VALUE
    ) {
      "impressionMaxFrequencyPerUser must be between -1 and ${Byte.MAX_VALUE}, got ${fulfillerParams.impressionMaxFrequencyPerUser}"
    }

    val fulfillerSelector =
      DefaultFulfillerSelector(
        requisitionsStub = requisitionsStub,
        requisitionFulfillmentStubMap = requisitionFulfillmentStubsMap,
        dataProviderCertificateKey = dataProviderCertificateKey,
        dataProviderSigningKeyHandle = dataProviderResultSigningKeyHandle,
        noiserSelector = noiseSelector,
        kAnonymityParams = kAnonymityParams,
        // When -1, treat as no frequency cap. When 0 or unset, use measurement spec value.
        overrideImpressionMaxFrequencyPerUser =
          if (fulfillerParams.impressionMaxFrequencyPerUser == -1) {
            -1
          } else {
            fulfillerParams.impressionMaxFrequencyPerUser.takeIf { it > 0 }
          },
      )

    ResultsFulfiller(
        dataProvider = fulfillerParams.dataProvider,
        requisitionMetadataStub = requisitionMetadataStub,
        requisitionsStub = requisitionsStub,
        privateEncryptionKey = loadPrivateKey(encryptionPrivateKeyFile),
        groupedRequisitions = groupedRequisitions,
        modelLineInfoMap = modelLineInfoMap,
        pipelineConfiguration = pipelineConfiguration,
        impressionDataSourceProvider = impressionsDataSourceProvider,
        impressionsStorageConfig = impressionsStorageConfig,
        kmsClient = kmsClient,
        fulfillerSelector = fulfillerSelector,
        metrics = metrics,
      )
      .fulfillRequisitions()
  }

  /**
   * Loads [GroupedRequisitions] from blob storage using the provided URI.
   *
   * Validates that the blob exists and is in the expected serialized `Any` format.
   *
   * @param requisitionsBlobUri The URI of the blob containing grouped requisitions.
   * @param requisitionsStorageConfig Storage configuration for reading grouped requisitions.
   * @return The parsed [GroupedRequisitions] payload.
   * @throws ImpressionReadException If the blob is missing or has an invalid format.
   */
  private suspend fun loadGroupedRequisitions(
    requisitionsBlobUri: String,
    requisitionsStorageConfig: StorageConfig,
  ): GroupedRequisitions {
    val storageClientUri = SelectedStorageClient.parseBlobUri(requisitionsBlobUri)
    val requisitionsStorageClient =
      SelectedStorageClient(
        storageClientUri,
        requisitionsStorageConfig.rootDirectory,
        requisitionsStorageConfig.projectId,
      )

    val requisitionBytes: ByteString =
      requisitionsStorageClient.getBlob(storageClientUri.key)?.read()?.flatten()
        ?: throw ImpressionReadException(
          storageClientUri.key,
          ImpressionReadException.Code.BLOB_NOT_FOUND,
        )

    return try {
      Any.parseFrom(requisitionBytes).unpack(GroupedRequisitions::class.java)
    } catch (e: Exception) {
      throw ImpressionReadException(
        storageClientUri.key,
        ImpressionReadException.Code.INVALID_FORMAT,
      )
    }
  }

  companion object {
    private val cpuCount = Runtime.getRuntime().availableProcessors()
    private val DEFAULT_PIPELINE_CONFIGURATION =
      PipelineConfiguration(
        batchSize = 256,
        channelCapacity = 64,
        threadPoolSize = cpuCount,
        workers = cpuCount,
      )
  }
}
