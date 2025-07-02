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
import com.google.protobuf.Parser
import com.google.protobuf.TypeRegistry
import java.nio.file.Paths
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.time.ZoneOffset
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.NoiseParams.NoiseType
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams.StorageParams
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication

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
 * @param requisitionStubFactory Factory for creating requisition stubs.
 * @param kmsClient The Tink [KmsClient] for key management.
 * @param typeRegistry The protobuf [TypeRegistry] for message unpacking. Should have all necessary
 *   descriptors registered to unpack a [LabeledImpression.event].
 * @param getImpressionsMetadataStorageConfig Lambda to obtain [StorageConfig] for impressions
 *   metadata.
 * @param getImpressionsStorageConfig Lambda to obtain [StorageConfig] for impressions.
 * @param getRequisitionsStorageConfig Lambda to obtain [StorageConfig] for requisitions.
 * @constructor Initializes the application with all required dependencies for result fulfillment.
 */
class ResultsFulfillerApp(
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<WorkItem>,
  workItemsClient: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  workItemAttemptsClient: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub,
  private val requisitionStubFactory: RequisitionStubFactory,
  private val kmsClient: KmsClient,
  private val typeRegistry: TypeRegistry,
  private val getImpressionsMetadataStorageConfig: (StorageParams) -> StorageConfig,
  private val getImpressionsStorageConfig: (StorageParams) -> StorageConfig,
  private val getRequisitionsStorageConfig: (StorageParams) -> StorageConfig,
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
    val requisitionsStub = requisitionStubFactory.buildRequisitionsStub(fulfillerParams)
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

    val eventReader =
      EventReader(
        kmsClient = kmsClient,
        impressionDekStorageConfig = impressionsMetadataStorageConfig,
        impressionsStorageConfig = impressionsStorageConfig,
        labeledImpressionsDekPrefix =
          fulfillerParams.storageParams.labeledImpressionsBlobDetailsUriPrefix,
      )
    val noiseSelector =
      when (fulfillerParams.noiseParams.noiseType) {
        NoiseType.NONE -> NoNoiserSelector()
        NoiseType.CONTINUOUS_GAUSSIAN -> ContinuousGaussianNoiseSelector()
        else -> throw Exception("Invalid noise type ${fulfillerParams.noiseParams.noiseType}")
      }
    ResultsFulfiller(
        loadPrivateKey(encryptionPrivateKeyFile),
        requisitionsStub,
        dataProviderCertificateKey,
        dataProviderResultSigningKeyHandle,
        typeRegistry,
        requisitionsBlobUri = requisitionsBlobUri,
        requisitionsStorageConfig = requisitionsStorageConfig,
        random = SecureRandom(),
        zoneId = ZoneOffset.UTC,
        noiserSelector = noiseSelector,
        eventReader = eventReader,
      )
      .fulfillRequisitions()
  }
}
