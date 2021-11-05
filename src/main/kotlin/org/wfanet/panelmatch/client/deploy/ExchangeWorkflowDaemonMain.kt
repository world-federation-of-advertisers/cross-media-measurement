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

import java.time.Clock
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.panelmatch.client.common.ExchangeContext
import org.wfanet.panelmatch.client.storage.FileSystemStorageFactory
import org.wfanet.panelmatch.client.storage.GcsStorageFactory
import org.wfanet.panelmatch.client.storage.S3StorageFactory
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetails.PlatformCase
import org.wfanet.panelmatch.client.storage.StorageFactory
import org.wfanet.panelmatch.common.ExchangeDateKey
import org.wfanet.panelmatch.common.certificates.CertificateAuthority
import org.wfanet.panelmatch.common.secrets.MutableSecretMap
import org.wfanet.panelmatch.common.secrets.SecretMap
import picocli.CommandLine

@CommandLine.Command(
  name = "ExchangeWorkflowDaemonMain",
  description = ["Daemon for executing ExchangeWorkflows"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private object MainExchangeWorkflowDaemon : ExchangeWorkflowDaemonFromFlags() {
  /**
   * This is one approach to providing [validExchangeWorkflows]. Some implementations may wish to
   * use a different method.
   */
  @CommandLine.Mixin
  lateinit var approvedWorkflowFlags: PlaintextApprovedWorkflowFileFlags
    private set

  @CommandLine.Mixin
  lateinit var blobSizeFlags: BlobSizeFlags
    private set

  override val clock: Clock = Clock.systemUTC()

  /** This can be customized per deployment. */
  override val validExchangeWorkflows: SecretMap by lazy {
    approvedWorkflowFlags.approvedExchangeWorkflows
  }

  /** This should be customized per deployment. */
  override val privateKeys: MutableSecretMap
    get() = TODO("Customize this per deployment")

  /** This should be customized per deployment. */
  override val rootCertificates: SecretMap
    get() = TODO("Customize this per deployment")

  /** This should be customized per deployment. */
  override val privateStorageInfo: SecretMap
    get() = TODO("Customize this per deployment")

  /** This should be customized per deployment. */
  override val sharedStorageInfo: SecretMap
    get() = TODO("Not yet implemented")

  /** This should be customized per deployment. */
  override val privateStorageFactories:
    Map<PlatformCase, ExchangeDateKey.(StorageDetails) -> StorageFactory> by lazy {
    mapOf(
      PlatformCase.FILE to ::FileSystemStorageFactory,
      PlatformCase.GCS to ::GcsStorageFactory,
      PlatformCase.AWS to ::S3StorageFactory,
    )
      .mapValues { (_, makeFactory) -> { storageDetails -> makeFactory(storageDetails, this) } }
  }

  /** This should be customized per deployment. */
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

  /** This should be customized per deployment. */
  override val certificateAuthority: CertificateAuthority
    get() = TODO("Not yet implemented")
}

/**
 * Reference implementation of a daemon for executing Exchange Workflows.
 *
 * Before using this, customize the overrides above.
 */
fun main(args: Array<String>) = commandLineMain(MainExchangeWorkflowDaemon, args)
