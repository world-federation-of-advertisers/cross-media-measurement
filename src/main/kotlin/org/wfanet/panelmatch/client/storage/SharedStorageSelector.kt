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

import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.StorageType
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.StorageType.AMAZON_S3
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.StorageType.GOOGLE_CLOUD_STORAGE
import org.wfanet.panelmatch.client.common.ExchangeContext
import org.wfanet.panelmatch.client.storage.StorageDetails.PlatformCase
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.certificates.CertificateManager

private val EXPLICITLY_SUPPORTED_STORAGE_TYPES = setOf(PlatformCase.AWS, PlatformCase.GCS)

/**
 * Builds [VerifiedStorageClient]s for shared blobs within a particular Exchange.
 *
 * @param certificateManager passed into VerifiedStorageClient
 * @param sharedStorageFactories provides [StorageFactories][StorageFactory] by storage type
 * @param storageDetailsProvider securely holds [StorageDetails] used with [sharedStorageFactories]
 */
class SharedStorageSelector(
  private val certificateManager: CertificateManager,
  private val sharedStorageFactories:
    Map<PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory>,
  private val storageDetailsProvider: StorageDetailsProvider
) {

  /** Builds a [VerifiedStorageClient] for the current Exchange. */
  suspend fun getSharedStorage(
    storageType: StorageType,
    context: ExchangeContext
  ): VerifiedStorageClient {
    val storageDetails = storageDetailsProvider.get(context.recurringExchangeId)
    require(storageDetails.visibility == StorageDetails.Visibility.SHARED)
    validateStorageType(storageType, storageDetails.platformCase)
    val storageClient = getStorageFactory(storageDetails, context).build()
    return VerifiedStorageClient(storageClient, context, certificateManager)
  }

  private fun getStorageFactory(
    storageDetails: StorageDetails,
    context: ExchangeContext
  ): StorageFactory {
    val platform = storageDetails.platformCase
    val buildStorageFactory =
      requireNotNull(sharedStorageFactories[platform]) {
        "Missing private StorageFactory for $platform"
      }
    return buildStorageFactory(storageDetails, context.exchangeDateKey)
  }

  private fun validateStorageType(storageType: StorageType, platform: PlatformCase) {
    when (storageType) {
      GOOGLE_CLOUD_STORAGE -> require(platform == PlatformCase.GCS)
      AMAZON_S3 -> require(platform == PlatformCase.AWS)
      StorageType.UNKNOWN_STORAGE_CLIENT, StorageType.UNRECOGNIZED ->
        require(platform !in EXPLICITLY_SUPPORTED_STORAGE_TYPES)
    // TODO(world-federation-of-advertisers/cross-media-measurement-api#73): throw
    //   IllegalArgumentException("$storageType unsupported")
    //   once StorageType.FILE and StorageType.CUSTOM are added.
    }
  }
}
