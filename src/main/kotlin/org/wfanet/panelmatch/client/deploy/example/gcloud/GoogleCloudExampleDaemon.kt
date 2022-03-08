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

package org.wfanet.panelmatch.client.deploy.example.gcloud

import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import java.util.Optional
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.tink.TinkKeyStorageProvider
import org.wfanet.measurement.gcloud.gcs.GcsFromFlags
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.deploy.CertificateAuthorityFlags
import org.wfanet.panelmatch.client.deploy.DaemonStorageClientDefaults
import org.wfanet.panelmatch.client.deploy.example.ExampleDaemon
import org.wfanet.panelmatch.client.storage.StorageDetailsProvider
import org.wfanet.panelmatch.common.certificates.gcloud.CertificateAuthority
import org.wfanet.panelmatch.common.certificates.gcloud.PrivateCaClient
import org.wfanet.panelmatch.common.secrets.MutableSecretMap
import org.wfanet.panelmatch.common.secrets.SecretMap
import picocli.CommandLine.Command
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option

private class PrivateCaFlags {
  @Option(
    names = ["--privateca-project-id"],
    description = ["Google Cloud PrivateCA project id"],
    required = true
  )
  lateinit var projectId: String
    private set

  @Option(
    names = ["--privateca-ca-location"],
    description = ["Google Cloud PrivateCA CA location"],
    required = true
  )
  lateinit var caLocation: String
    private set

  @Option(
    names = ["--privateca-pool-id"],
    description = ["Google Cloud PrivateCA pool id"],
    required = true
  )
  lateinit var poolId: String
    private set

  @Option(
    names = ["--privateca-ca-name"],
    description = ["Google Cloud PrivateCA CA name"],
    required = true
  )
  lateinit var certificateAuthorityName: String
    private set
}

@Command(
  name = "GoogleCloudExampleDaemon",
  description = ["Example daemon to execute ExchangeWorkflows on Google Cloud"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private class GoogleCloudExampleDaemon : ExampleDaemon() {
  @Mixin private lateinit var gcsFlags: GcsFromFlags.Flags
  @Mixin private lateinit var caFlags: CertificateAuthorityFlags
  @Mixin private lateinit var privateCaFlags: PrivateCaFlags

  override val rootStorageClient: StorageClient by lazy {
    GcsStorageClient.fromFlags(GcsFromFlags(gcsFlags))
  }

  /** This can be customized per deployment. */
  private val defaults by lazy {
    // Register GcpKmsClient before setting storage folders. Set GOOGLE_APPLICATION_CREDENTIALS.
    GcpKmsClient.register(Optional.of(tinkKeyUri), Optional.empty())
    DaemonStorageClientDefaults(rootStorageClient, tinkKeyUri, TinkKeyStorageProvider())
  }

  /** This can be customized per deployment. */
  override val validExchangeWorkflows: SecretMap
    get() = defaults.validExchangeWorkflows

  /** This can be customized per deployment. */
  override val privateKeys: MutableSecretMap
    get() = defaults.privateKeys

  /** This can be customized per deployment. */
  override val rootCertificates: SecretMap
    get() = defaults.rootCertificates

  /** This can be customized per deployment. */
  override val privateStorageInfo: StorageDetailsProvider
    get() = defaults.privateStorageInfo

  /** This can be customized per deployment. */
  override val sharedStorageInfo: StorageDetailsProvider
    get() = defaults.sharedStorageInfo

  override val certificateAuthority by lazy {
    CertificateAuthority(
      caFlags.context,
      privateCaFlags.projectId,
      privateCaFlags.caLocation,
      privateCaFlags.poolId,
      privateCaFlags.certificateAuthorityName,
      PrivateCaClient(),
    )
  }
}

/** Reference Google Cloud implementation of a daemon for executing Exchange Workflows. */
fun main(args: Array<String>) = commandLineMain(GoogleCloudExampleDaemon(), args)
