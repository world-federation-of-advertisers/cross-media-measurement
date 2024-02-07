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
import org.wfanet.panelmatch.client.storage.StorageDetails.PlatformCase
import org.wfanet.panelmatch.client.storage.StorageDetails.Visibility.PRIVATE
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.storage.StorageFactory

/**
 * Builds storage clients for the panel exchange workflow.
 *
 * @param[privateStorageFactories] is a map of storage factory constructors supported by our daemon.
 *   As not all types of StorageClients are expected to be supported by all EDPs and MPs, this gives
 *   each party the option to not depend on a StorageFactory they choose not to support.
 * @param[storageDetailsProvider] provides access to all information required to build private
 *   storage for all active exchanges. These are expected to be configured by a party at the time an
 *   exchange is first built and stored securely. This SecretMap is the abstraction used to retrieve
 *   those credentials.
 */
class PrivateStorageSelector(
  private val privateStorageFactories:
    Map<PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory>,
  private val storageDetailsProvider: StorageDetailsProvider,
) {
  /** Builds the appropriate [StorageFactory] for the current Exchange. */
  suspend fun getStorageFactory(key: ExchangeDateKey): StorageFactory {
    val storageDetails = storageDetailsProvider.get(key.recurringExchangeId)
    require(storageDetails.visibility == PRIVATE)
    return getStorageFactory(storageDetails, key)
  }

  /** Builds the appropriate [StorageClient] for the current Exchange. */
  suspend fun getStorageClient(key: ExchangeDateKey): StorageClient {
    return getStorageFactory(key).build()
  }

  private fun getStorageFactory(
    storageDetails: StorageDetails,
    key: ExchangeDateKey,
  ): StorageFactory {
    val platform = storageDetails.platformCase
    val buildStorageFactory =
      requireNotNull(privateStorageFactories[platform]) {
        "Missing private StorageFactory for $platform"
      }
    return buildStorageFactory(storageDetails, key)
  }
}
