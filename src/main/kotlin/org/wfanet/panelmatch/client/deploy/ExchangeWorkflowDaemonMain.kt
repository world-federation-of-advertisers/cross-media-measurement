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

import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.tink.TinkKeyStorageProvider
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.common.ExchangeContext
import org.wfanet.panelmatch.client.storage.FileSystemStorageFactory
import org.wfanet.panelmatch.client.storage.GcsStorageFactory
import org.wfanet.panelmatch.client.storage.S3StorageFactory
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetails.PlatformCase
import org.wfanet.panelmatch.client.storage.StorageDetailsProvider
import org.wfanet.panelmatch.client.storage.StorageFactory
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.certificates.CertificateAuthority
import org.wfanet.panelmatch.common.secrets.MutableSecretMap
import org.wfanet.panelmatch.common.secrets.SecretMap
import org.wfanet.panelmatch.common.secrets.StorageClientSecretMap
import org.wfanet.panelmatch.common.storage.PrefixedStorageClient
import picocli.CommandLine

@CommandLine.Command(
  name = "ExchangeWorkflowDaemonMain",
  description = ["Daemon for executing ExchangeWorkflows"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private object MainExchangeWorkflowDaemon : ExchangeWorkflowDaemonFromFlags() {
  @CommandLine.Option(
    names = ["--tink-key-uri"],
    description = ["KMS URI for Tink"],
    required = true
  )
  private lateinit var tinkKeyUri: String

  /** This MUST be customized per deployment. */
  private val rootStorageClient: StorageClient
    get() = TODO("Customize this per deployment")

  /** This MUST be customized per deployment. */
  override val certificateAuthority: CertificateAuthority
    get() = TODO("Customize this per deployment")

  /** This can be customized per deployment. */
  override val validExchangeWorkflows: SecretMap by lazy {
    StorageClientSecretMap(PrefixedStorageClient(rootStorageClient, "valid-exchange-workflows"))
  }

  /** This can be customized per deployment. */
  override val privateKeys: MutableSecretMap by lazy {
    val tinkStorageProvider = TinkKeyStorageProvider()
    val kmsStorageClient = tinkStorageProvider.makeKmsStorageClient(rootStorageClient, tinkKeyUri)
    val prefixedKmsStorageClient = PrefixedStorageClient(kmsStorageClient, "private-keys")
    StorageClientSecretMap(prefixedKmsStorageClient)
  }

  /** This can be customized per deployment. */
  override val rootCertificates: SecretMap by lazy {
    StorageClientSecretMap(PrefixedStorageClient(rootStorageClient, "root-x509-certificates"))
  }

  /** This can be customized per deployment. */
  override val privateStorageInfo: StorageDetailsProvider by lazy {
    val storageClient = PrefixedStorageClient(rootStorageClient, "private-storage-info")
    StorageDetailsProvider(StorageClientSecretMap(storageClient))
  }

  /** This can be customized per deployment. */
  override val sharedStorageInfo: StorageDetailsProvider by lazy {
    val storageClient = PrefixedStorageClient(rootStorageClient, "shared-storage-info")
    StorageDetailsProvider(StorageClientSecretMap(storageClient))
  }

  /** This can be customized per deployment. */
  override val privateStorageFactories:
    Map<PlatformCase, ExchangeDateKey.(StorageDetails) -> StorageFactory> by lazy {
    mapOf(
      PlatformCase.FILE to ::FileSystemStorageFactory,
      PlatformCase.GCS to ::GcsStorageFactory,
      PlatformCase.AWS to ::S3StorageFactory,
    )
      .mapValues { (_, makeFactory) -> { storageDetails -> makeFactory(storageDetails, this) } }
  }

  /** This can be customized per deployment. */
  override val sharedStorageFactories:
    Map<PlatformCase, ExchangeContext.(StorageDetails) -> StorageFactory> by lazy {
    mapOf(
      PlatformCase.FILE to ::FileSystemStorageFactory,
      PlatformCase.GCS to ::GcsStorageFactory,
      PlatformCase.AWS to ::S3StorageFactory,
    )
      .mapValues { (_, makeFactory) ->
        { storageDetails ->
          blobSizeFlags.wrapStorageFactory(makeFactory(storageDetails, exchangeDateKey))
        }
      }
  }
}

/**
 * Reference implementation of a daemon for executing Exchange Workflows.
 *
 * Before using this, customize the overrides above.
 */
fun main(args: Array<String>) = commandLineMain(MainExchangeWorkflowDaemon, args)
