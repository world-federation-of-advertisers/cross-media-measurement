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

package org.wfanet.panelmatch.client.deploy.example.forwarded

import java.time.Clock
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.storage.forwarded.ForwardedStorageFromFlags
import org.wfanet.panelmatch.client.deploy.DaemonStorageClientDefaults
import org.wfanet.panelmatch.client.deploy.ExchangeWorkflowDaemonFromFlags
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetailsProvider
import org.wfanet.panelmatch.client.storage.forwarded.ForwardedStorageFactory
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.certificates.CertificateAuthority
import org.wfanet.panelmatch.common.certificates.CertificateManager
import org.wfanet.panelmatch.common.certificates.testing.TestCertificateManager
import org.wfanet.panelmatch.common.secrets.MutableSecretMap
import org.wfanet.panelmatch.common.secrets.SecretMap
import org.wfanet.panelmatch.common.storage.StorageFactory
import org.wfanet.panelmatch.common.storage.testing.FakeTinkKeyStorageProvider
import picocli.CommandLine

class ForwardedStorageExchangeWorkflowDaemon(override val clock: Clock = Clock.systemUTC()) :
  ExchangeWorkflowDaemonFromFlags() {

  @CommandLine.Mixin private lateinit var forwardedStorageFlags: ForwardedStorageFromFlags.Flags

  private val storageClient by lazy {
    logger.info { "Creating storage Client" }
    ForwardedStorageFromFlags(forwardedStorageFlags, flags.tlsFlags).storageClient
  }

  private val tinkKeyUri = "fake-tink-key-uri"

  private val defaults by lazy {
    DaemonStorageClientDefaults(storageClient, tinkKeyUri, FakeTinkKeyStorageProvider())
  }
  override val validExchangeWorkflows: SecretMap
    get() = defaults.validExchangeWorkflows

  override val rootCertificates: SecretMap
    get() = defaults.rootCertificates

  override val privateStorageInfo: StorageDetailsProvider
    get() = defaults.privateStorageInfo

  override val sharedStorageInfo: StorageDetailsProvider
    get() = defaults.sharedStorageInfo

  override val privateKeys: MutableSecretMap
    get() = defaults.privateKeys

  override val certificateAuthority: CertificateAuthority
    get() = TODO("Not yet implemented")

  // TODO(@marcopremier): Change this to let each Daemon has its own certificate
  override val certificateManager: CertificateManager = TestCertificateManager

  override fun makePipelineOptions(): PipelineOptions = PipelineOptionsFactory.create()

  override val privateStorageFactories:
    Map<StorageDetails.PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory> =
    mapOf(StorageDetails.PlatformCase.CUSTOM to ::ForwardedStorageFactory)

  override val sharedStorageFactories:
    Map<StorageDetails.PlatformCase, (StorageDetails, ExchangeDateKey) -> StorageFactory> =
    mapOf(StorageDetails.PlatformCase.CUSTOM to ::ForwardedStorageFactory)

  companion object {
    @JvmStatic
    fun main(args: Array<String>) {
      commandLineMain(ForwardedStorageExchangeWorkflowDaemon(), args)
    }
  }
}
