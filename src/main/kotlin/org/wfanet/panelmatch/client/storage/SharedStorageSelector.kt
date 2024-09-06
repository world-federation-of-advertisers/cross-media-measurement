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

package org.wfanet.panelmatch.client.storage

import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.common.ExchangeContext
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.StorageType
import org.wfanet.panelmatch.client.storage.StorageDetails.PlatformCase
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.certificates.CertificateManager
import org.wfanet.panelmatch.common.storage.StorageFactory
import org.wfanet.panelmatch.protocol.namedSignature

private val EXPLICITLY_SUPPORTED_STORAGE_TYPES = setOf(PlatformCase.AWS, PlatformCase.GCS)

/**
 * Builds [VerifyingStorageClient]s and [SigningStorageClient]s for shared blobs within a particular
 * Exchange.
 *
 * @param certificateManager passed into VerifiedStorageClient
 * @param sharedStorageFactories provides [StorageFactories][StorageFactory] by storage type
 * @param storageDetailsProvider securely holds [StorageDetails] used with [sharedStorageFactories]
 */
class SharedStorageSelector(
  private val certificateManager: CertificateManager,
  private val sharedStorageFactories:
    Map<PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory>,
  private val storageDetailsProvider: StorageDetailsProvider,
) {

  /**
   * Builds and returns a new [VerifyingStorageClient] for reading blobs from shared storage and
   * verifying their signatures. Note that for non-manifest blobs, the returned client can only be
   * used to verify [blobKey]. For manifest blobs, it can also be used to verify all blob shards.
   */
  suspend fun getVerifyingStorage(
    blobKey: String,
    storageType: StorageType,
    context: ExchangeContext,
  ): VerifyingStorageClient {
    val storageDetails = storageDetailsProvider.get(context.recurringExchangeId)
    validateStorageType(storageType, storageDetails)

    val storageFactory = getStorageFactory(storageDetails, context.exchangeDateKey)
    val storageClient = storageFactory.build()
    val namedSignature = storageClient.getBlobSignature(blobKey)
    val x509 =
      certificateManager.getCertificate(context.exchangeDateKey, namedSignature.certificateName)
    return VerifyingStorageClient(storageFactory, x509)
  }

  /**
   * Builds and returns a new [SigningStorageClient] for writing blobs and their signatures to
   * shared storage.
   */
  suspend fun getSigningStorage(
    storageType: StorageType,
    context: ExchangeContext,
  ): SigningStorageClient {
    val storageDetails = storageDetailsProvider.get(context.recurringExchangeId)
    validateStorageType(storageType, storageDetails)

    val storageFactory = getStorageFactory(storageDetails, context.exchangeDateKey)
    val (x509, privateKey, certName) =
      certificateManager.getExchangeKeyPair(context.exchangeDateKey)
    val signatureTemplate = namedSignature { certificateName = certName }
    return SigningStorageClient(storageFactory, x509, privateKey, signatureTemplate)
  }

  /** Builds and returns a new [StorageClient] for reading and writing blobs in shared storage. */
  suspend fun getStorageClient(exchangeDateKey: ExchangeDateKey): StorageClient {
    val storageDetails = storageDetailsProvider.get(exchangeDateKey.recurringExchangeId)
    val storageFactory = getStorageFactory(storageDetails, exchangeDateKey)
    return storageFactory.build()
  }

  private fun getStorageFactory(
    storageDetails: StorageDetails,
    exchangeDateKey: ExchangeDateKey,
  ): StorageFactory {
    val platform = storageDetails.platformCase
    val buildStorageFactory =
      requireNotNull(sharedStorageFactories[platform]) {
        "Missing private StorageFactory for $platform"
      }
    return buildStorageFactory(storageDetails, exchangeDateKey)
  }

  private fun validateStorageType(storageType: StorageType, storageDetails: StorageDetails) {
    require(storageDetails.visibility == StorageDetails.Visibility.SHARED)
    val platform = storageDetails.platformCase
    when (storageType) {
      StorageType.GOOGLE_CLOUD_STORAGE -> require(platform == PlatformCase.GCS)
      StorageType.AMAZON_S3 -> require(platform == PlatformCase.AWS)
      StorageType.STORAGE_TYPE_UNSPECIFIED,
      StorageType.UNRECOGNIZED -> require(platform !in EXPLICITLY_SUPPORTED_STORAGE_TYPES)
    // TODO(world-federation-of-advertisers/cross-media-measurement-api#73): throw
    //   IllegalArgumentException("$storageType unsupported")
    //   once StorageType.FILE and StorageType.CUSTOM are added.
    }
  }
}
