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

import org.wfanet.measurement.common.crypto.KeyStorageProvider
import org.wfanet.measurement.common.crypto.tink.TinkKeyId
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.storage.StorageDetailsProvider
import org.wfanet.panelmatch.common.secrets.MutableSecretMap
import org.wfanet.panelmatch.common.secrets.StorageClientSecretMap
import org.wfanet.panelmatch.common.storage.withPrefix

class DaemonStorageClientDefaults(
  rootStorageClient: StorageClient,
  tinkKeyUri: String,
  tinkStorageProvider: KeyStorageProvider<TinkKeyId, TinkPrivateKeyHandle>,
) {
  val validExchangeWorkflows: MutableSecretMap by lazy {
    StorageClientSecretMap(rootStorageClient.withPrefix("valid-exchange-workflows"))
  }

  val rootCertificates: MutableSecretMap by lazy {
    StorageClientSecretMap(rootStorageClient.withPrefix("root-x509-certificates"))
  }

  /** This can be customized per deployment. */
  val privateStorageInfo: StorageDetailsProvider by lazy {
    val storageClient = rootStorageClient.withPrefix("private-storage-info")
    StorageDetailsProvider(StorageClientSecretMap(storageClient))
  }

  /** This can be customized per deployment. */
  val sharedStorageInfo: StorageDetailsProvider by lazy {
    val storageClient = rootStorageClient.withPrefix("shared-storage-info")
    StorageDetailsProvider(StorageClientSecretMap(storageClient))
  }

  val privateKeys: MutableSecretMap by lazy {
    val kmsStorageClient = tinkStorageProvider.makeKmsStorageClient(rootStorageClient, tinkKeyUri)
    StorageClientSecretMap(kmsStorageClient.withPrefix("private-keys"))
  }
}
