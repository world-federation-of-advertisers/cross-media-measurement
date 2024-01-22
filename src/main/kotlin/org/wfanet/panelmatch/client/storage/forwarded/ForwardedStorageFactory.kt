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

package org.wfanet.panelmatch.client.storage.forwarded

import java.io.File
import org.wfanet.measurement.common.crypto.readCertificateCollection
import org.wfanet.measurement.common.grpc.buildTlsChannel
import org.wfanet.measurement.internal.testing.ForwardedStorageGrpcKt
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.forwarded.ForwardedStorageClient
import org.wfanet.panelmatch.client.loadtest.ForwardedStorageConfig
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.storage.StorageFactory

/**
 * Build a ForwardedStorageClient object using storageDetails information.
 *
 * @param [storageDetails] the proto message that contains the information to build a
 *   ForwardedStorageClient.
 * @param [exchangeDateKey] exchange workflow identifier used to disambiguate storage path for each
 *   exchange workflow.
 */
class ForwardedStorageFactory(
  private val storageDetails: StorageDetails,
  private val exchangeDateKey: ExchangeDateKey,
) : StorageFactory {

  override fun build(): StorageClient {

    val forwardedStorage = storageDetails.custom.details.unpack(ForwardedStorageConfig::class.java)
    return ForwardedStorageClient(
      ForwardedStorageGrpcKt.ForwardedStorageCoroutineStub(
        buildTlsChannel(
          forwardedStorage.target,
          readCertificateCollection(requireNotNull(File(forwardedStorage.certCollectionPath))),
          forwardedStorage.forwardedStorageCertHost,
        )
      )
    )
  }
}
