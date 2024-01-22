// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.client.tools

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.security.cert.X509Certificate
import java.time.LocalDate
import kotlinx.coroutines.flow.flowOf
import org.wfanet.panelmatch.client.deploy.DaemonStorageClientDefaults
import org.wfanet.panelmatch.client.storage.PrivateStorageSelector
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetails.PlatformCase
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.storage.StorageFactory

class ConfigureResource(private val clientDefaults: DaemonStorageClientDefaults) {

  suspend fun addWorkflow(serializedWorkflow: ByteString, recurringExchangeId: String) {
    clientDefaults.validExchangeWorkflows.put(recurringExchangeId, serializedWorkflow)
  }

  suspend fun addRootCertificates(partnerId: String, certificate: X509Certificate) {
    clientDefaults.rootCertificates.put(partnerId, certificate.encoded.toByteString())
  }

  suspend fun addPrivateStorageInfo(recurringExchangeId: String, storageDetails: StorageDetails) {
    clientDefaults.privateStorageInfo.put(recurringExchangeId, storageDetails)
  }

  suspend fun addSharedStorageInfo(recurringExchangeId: String, storageDetails: StorageDetails) {
    clientDefaults.sharedStorageInfo.put(recurringExchangeId, storageDetails)
  }

  suspend fun provideWorkflowInput(
    recurringExchangeId: String,
    date: LocalDate,
    privateStorageFactories: Map<PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory>,
    blobKey: String,
    contents: ByteString,
  ) {
    val privateStorageSelector =
      PrivateStorageSelector(privateStorageFactories, clientDefaults.privateStorageInfo)
    val exchangeDateKey = ExchangeDateKey(recurringExchangeId, date)
    privateStorageSelector.getStorageClient(exchangeDateKey).writeBlob(blobKey, flowOf(contents))
  }
}
