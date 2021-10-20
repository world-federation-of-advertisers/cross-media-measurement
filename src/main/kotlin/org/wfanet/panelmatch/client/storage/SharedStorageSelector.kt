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

import com.google.common.collect.ImmutableMap
import org.wfanet.measurement.api.v2alpha.ExchangeKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.panelmatch.common.certificates.CertificateManager
import org.wfanet.panelmatch.common.secrets.SecretMap

/**
 * Builds storage clients for the panel exchange workflow.
 *
 * - [getSharedStorage] provides a [VerifiedStorageClient]
 *
 * The class takes in exchange-specific storage information ([sharedStorageInfo]) that is required
 * to build the appropriate storage clients for each exchange. We expect these values to be set when
 * an exchange is first created and are not shared with the Kingdom. These are currently keyed by
 * the [ExchangeStepAttemptKey.recurringExchangeId].
 *
 * @param[certificateManager]: An interface that both has access to the root trusted certificates
 * for each party as well as the certificate service API to be able to pull down and validate
 * secondary, per-exchange certificates that shared data will be signed with.
 * @param[ownerName]: The API resource name for the party running this binary.
 * @param[sharedStorageFactories] is a map of storage factory constructors supported by our daemon.
 * As not all types of StorageClients are expected to be supported by all EDPs and MPs, this gives
 * each party the option to not depend on a StorageFactory they choose not to support.
 * @param[sharedStorageInfo] provides access to all information required to build a shared storage
 * client for any active exchange. Shared storage is expected to be agrred-on and configured by both
 * parties as part of creating an exchange and credentials should be pre-exchanged and stored
 * securely by the time a recurring exchange starts. This SecretMap is the abstraction used to
 * retrieve those credentials.
 */
class SharedStorageSelector(
  private val certificateManager: CertificateManager,
  private val ownerName: String,
  private val sharedStorageFactories:
    ImmutableMap<StorageDetails.PlatformCase, (StorageDetails, ExchangeKey) -> StorageFactory>,
  private val sharedStorageInfo: SecretMap
) {

  private suspend fun getStorageFactory(
    storageDetails: StorageDetails,
    exchangeKey: ExchangeKey
  ): StorageFactory {
    val storageFactoryBuilder =
      requireNotNull(sharedStorageFactories[storageDetails.platformCase]) {
        "Missing private StorageFactory for ${storageDetails.platformCase}"
      }
    return storageFactoryBuilder(storageDetails, exchangeKey)
  }

  private suspend fun getStorageDetails(recurringExchangeId: String): StorageDetails {
    val storageDetails =
      StorageDetails.parseFrom(
        sharedStorageInfo.get(recurringExchangeId)
          ?: throw StorageNotFoundException(
            "No shared storage found for exchange $recurringExchangeId"
          )
      )

    require(storageDetails.visibility == StorageDetails.Visibility.SHARED)
    return storageDetails
  }

  /**
   * Makes an appropriate [VerifiedStorageClient] for the current exchange. Requires the exchange to
   * be active with shared storage recorded in our [sharedStorageInfo] secret map. Since shared
   * storage is the only storage that is verified, this is the only function that returns a Verified
   * client.
   *
   * @param[storageType] is grabbed from the exchange workflow to validate that our local
   * information is accurate.
   * @param[partnerName] The API resource name of the partner in this exchange. Required to look up
   * the certificate required to validate reads.
   * @param[ownerCertificateResourceName] Optional. The API resource name of the certificate that
   * has been created for this exchange. Only required for [CopyToSharedStorageTask] tasks, it is
   * expected to be passed through an input label for the tasks that need it. Tasks that do not
   * write to shared shared storage are expected to leave this as null so they don't need to depend
   * on the task that generates the certificate when they don't use it.
   */
  suspend fun getSharedStorage(
    storageType: ExchangeWorkflow.StorageType,
    attemptKey: ExchangeStepAttemptKey,
    partnerName: String,
    ownerCertificateResourceName: String?
  ): VerifiedStorageClient {
    val storageDetails = getStorageDetails(attemptKey.recurringExchangeId)
    when (storageType) {
      ExchangeWorkflow.StorageType.GOOGLE_CLOUD_STORAGE -> requireNotNull(storageDetails.gcs)
      ExchangeWorkflow.StorageType.AMAZON_S3 -> requireNotNull(storageDetails.aws)
      else -> throw IllegalArgumentException("No supported shared storage type specified.")
    }

    return getVerifiedStorageClient(
      storageDetails,
      ExchangeKey(attemptKey.recurringExchangeId, attemptKey.exchangeId),
      partnerName,
      ownerCertificateResourceName
    )
  }

  private suspend fun getVerifiedStorageClient(
    storageDetails: StorageDetails,
    exchangeKey: ExchangeKey,
    partnerName: String,
    ownerCertificateResourceName: String?
  ): VerifiedStorageClient {

    return VerifiedStorageClient(
      storageClient = getStorageFactory(storageDetails, exchangeKey).build(),
      exchangeKey = exchangeKey,
      ownerName,
      partnerName,
      ownerCertificateResourceName,
      certificateManager
    )
  }
}
