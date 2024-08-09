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
import kotlin.properties.Delegates
import org.apache.beam.runners.dataflow.DataflowRunner
import org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.options.SdkHarnessOptions
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.tink.TinkKeyStorageProvider
import org.wfanet.measurement.gcloud.gcs.GcsFromFlags
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.deploy.CertificateAuthorityFlags
import org.wfanet.panelmatch.client.deploy.DaemonStorageClientDefaults
import org.wfanet.panelmatch.client.deploy.example.ExampleDaemon
import org.wfanet.panelmatch.client.storage.StorageDetailsProvider
import org.wfanet.panelmatch.common.beam.BeamOptions
import org.wfanet.panelmatch.common.certificates.gcloud.CertificateAuthority
import org.wfanet.panelmatch.common.certificates.gcloud.PrivateCaClient
import org.wfanet.panelmatch.common.secrets.MutableSecretMap
import org.wfanet.panelmatch.common.secrets.SecretMap
import picocli.CommandLine.Command
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider

private class PrivateCaFlags {
  @Option(
    names = ["--privateca-project-id"],
    description = ["Google Cloud PrivateCA project id"],
    required = true,
  )
  lateinit var projectId: String
    private set

  @Option(
    names = ["--privateca-ca-location"],
    description = ["Google Cloud PrivateCA CA location"],
    required = true,
  )
  lateinit var caLocation: String
    private set

  @Option(
    names = ["--privateca-pool-id"],
    description = ["Google Cloud PrivateCA pool id"],
    required = true,
  )
  lateinit var poolId: String
    private set

  @Option(
    names = ["--privateca-ca-name"],
    description = ["Google Cloud PrivateCA CA name"],
    required = true,
  )
  lateinit var certificateAuthorityName: String
    private set
}

@Command(
  name = "GoogleCloudExampleDaemon",
  description = ["Example daemon to execute ExchangeWorkflows on Google Cloud"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
private class GoogleCloudExampleDaemon : ExampleDaemon() {
  @Mixin private lateinit var gcsFlags: GcsFromFlags.Flags
  @Mixin private lateinit var caFlags: CertificateAuthorityFlags
  @Mixin private lateinit var privateCaFlags: PrivateCaFlags

  @Option(
    names = ["--dataflow-project-id"],
    description = ["Google Cloud project name for Dataflow"],
    required = true,
  )
  lateinit var dataflowProjectId: String
    private set

  @Option(
    names = ["--dataflow-region"],
    description = ["Google Cloud region for Dataflow"],
    required = true,
  )
  lateinit var dataflowRegion: String
    private set

  @Option(
    names = ["--dataflow-service-account"],
    description = ["Service account for Dataflow"],
    required = true,
  )
  lateinit var dataflowServiceAccount: String
    private set

  @Option(
    names = ["--dataflow-temp-location"],
    description = ["Google Cloud temp location (GCS bucket) for Dataflow"],
    required = true,
  )
  lateinit var dataflowTempLocation: String
    private set

  @Option(
    names = ["--dataflow-worker-machine-type"],
    description = ["Dataflow worker machine type."],
    defaultValue = "n1-standard-1",
  )
  lateinit var dataflowWorkerMachineType: String
    private set

  @set:Option(
    names = ["--dataflow-disk-size"],
    description = ["Dataflow disk size in GB."],
    defaultValue = "30",
  )
  private var dataflowDiskSize by Delegates.notNull<Int>()

  @set:Option(
    names = ["--dataflow-max-num-workers"],
    description = ["Maximum number of workers to use in Dataflow."],
    defaultValue = "100",
  )
  private var dataflowMaxNumWorkers by Delegates.notNull<Int>()

  @set:Option(
    names = ["--s3-from-beam"],
    description = ["Whether to configure s3 access from Apache Beam."],
    defaultValue = "false",
  )
  private var s3FromBeam by Delegates.notNull<Boolean>()

  @Option(
    names = ["--dataflow-worker-logging-options-level"],
    description = ["Dataflow Worker Logging Options Level"],
    defaultValue = "INFO",
  )
  lateinit var dataflowWorkerLoggingOptionsLevel: DataflowWorkerLoggingOptions.Level
    private set

  @Option(
    names = ["--sdk-harness-options-log-level"],
    description = ["Sdk Harness Options Log Level"],
    defaultValue = "INFO",
  )
  lateinit var sdkHarnessOptionsLogLevel: SdkHarnessOptions.LogLevel
    private set

  override fun makePipelineOptions(): PipelineOptions {
    val baseOptions =
      PipelineOptionsFactory.`as`(BeamOptions::class.java).apply {
        runner = DataflowRunner::class.java
        project = dataflowProjectId
        region = dataflowRegion
        tempLocation = dataflowTempLocation
        serviceAccount = dataflowServiceAccount
        workerMachineType = dataflowWorkerMachineType
        maxNumWorkers = dataflowMaxNumWorkers
        diskSizeGb = dataflowDiskSize
        defaultWorkerLogLevel = dataflowWorkerLoggingOptionsLevel
        defaultSdkHarnessLogLevel = sdkHarnessOptionsLogLevel
      }
    return if (!s3FromBeam) {
      baseOptions
    } else {
      // aws-sdk-java-v2 casts responses to AwsSessionCredentials if its assumed you need a
      // sessionToken
      val awsCredentials =
        DefaultCredentialsProvider.create().resolveCredentials() as AwsSessionCredentials
      // TODO: Encrypt using KMS or store in Secrets
      // Think about moving this logic to a CredentialsProvider
      baseOptions.apply {
        awsAccessKey = awsCredentials.accessKeyId()
        awsSecretAccessKey = awsCredentials.secretAccessKey()
        awsSessionToken = awsCredentials.sessionToken()
      }
    }
  }

  override val rootStorageClient: StorageClient by lazy {
    GcsStorageClient.fromFlags(GcsFromFlags(gcsFlags))
  }

  /** This can be customized per deployment. */
  private val defaults by lazy {
    // Register GcpKmsClient before setting storage folders. Set GOOGLE_APPLICATION_CREDENTIALS.
    DaemonStorageClientDefaults(
      rootStorageClient,
      tinkKeyUri,
      TinkKeyStorageProvider(GcpKmsClient().withDefaultCredentials()),
    )
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
