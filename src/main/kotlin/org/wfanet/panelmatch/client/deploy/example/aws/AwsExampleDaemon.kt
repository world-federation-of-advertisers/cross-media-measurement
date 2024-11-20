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

package org.wfanet.panelmatch.client.deploy.example.aws

import com.google.crypto.tink.integration.awskms.AwsKmsClient
import kotlin.properties.Delegates
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.options.SdkHarnessOptions
import org.wfanet.measurement.aws.s3.S3StorageClient
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SignatureAlgorithm
import org.wfanet.measurement.common.crypto.generateKeyPair
import org.wfanet.measurement.common.crypto.tink.TinkKeyStorageProvider
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.deploy.CertificateAuthorityFlags
import org.wfanet.panelmatch.client.deploy.DaemonStorageClientDefaults
import org.wfanet.panelmatch.client.deploy.example.ExampleDaemon
import org.wfanet.panelmatch.client.launcher.ExchangeStepValidatorImpl
import org.wfanet.panelmatch.client.launcher.ExchangeTaskExecutor
import org.wfanet.panelmatch.client.storage.StorageDetailsProvider
import org.wfanet.panelmatch.common.beam.BeamOptions
import org.wfanet.panelmatch.common.certificates.aws.CertificateAuthority
import org.wfanet.panelmatch.common.certificates.aws.PrivateCaClient
import org.wfanet.panelmatch.common.secrets.MutableSecretMap
import org.wfanet.panelmatch.common.secrets.SecretMap
import picocli.CommandLine.Command
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient

@Command(
  name = "AwsExampleDaemon",
  description = ["Example daemon to execute ExchangeWorkflows on AWS"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
private class AwsExampleDaemon : ExampleDaemon() {
  @Mixin private lateinit var caFlags: CertificateAuthorityFlags

  @Option(
    names = ["--certificate-authority-arn"],
    description = ["AWS Certificate Authority ARN"],
    required = true,
  )
  lateinit var certificateAuthorityArn: String
    private set

  @Option(
    names = ["--certificate-authority-csr-signature-algorithm"],
    description =
      ["Signature algorithm to use for CSRs sent to the AWS CA. One of \${COMPLETION-CANDIDATES}"],
    required = true,
  )
  lateinit var certificateAuthorityCsrSignatureAlgorithm: SignatureAlgorithm
    private set

  @Option(
    names = ["--s3-storage-bucket"],
    description = ["The name of the s3 bucket used for default private storage."],
  )
  lateinit var s3Bucket: String
    private set

  @Option(names = ["--s3-region"], description = ["The region the s3 bucket is located in."])
  lateinit var s3Region: String
    private set

  @set:Option(
    names = ["--s3-from-beam"],
    description = ["Whether to configure s3 access from Apache Beam."],
    defaultValue = "false",
  )
  private var s3FromBeam by Delegates.notNull<Boolean>()

  override fun makePipelineOptions(): PipelineOptions {
    // TODO(jmolle): replace usage of DirectRunner.
    val baseOptions =
      PipelineOptionsFactory.`as`(BeamOptions::class.java).apply {
        runner = DirectRunner::class.java
        defaultSdkHarnessLogLevel = SdkHarnessOptions.LogLevel.INFO
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
    S3StorageClient(S3AsyncClient.builder().region(Region.of(s3Region)).build(), s3Bucket)
  }

  /** This can be customized per deployment. */
  private val defaults by lazy {
    // Register AwsKmsClient before setting storage folders.
    DaemonStorageClientDefaults(
      rootStorageClient,
      tinkKeyUri,
      TinkKeyStorageProvider(AwsKmsClient()),
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
      context = caFlags.context,
      certificateAuthorityArn = certificateAuthorityArn,
      client = PrivateCaClient(),
      signatureAlgorithm = certificateAuthorityCsrSignatureAlgorithm,
      generateKeyPair = { generateKeyPair(certificateAuthorityCsrSignatureAlgorithm.keyAlgorithm) },
    )
  }

  override val stepExecutor by lazy {
    ExchangeTaskExecutor(
      apiClient = apiClient,
      timeout = taskTimeout,
      privateStorageSelector = privateStorageSelector,
      exchangeTaskMapper = exchangeTaskMapper,
      validator = ExchangeStepValidatorImpl(identity.party, validExchangeWorkflows, clock),
    )
  }

  // TODO(world-federation-of-advertisers/cross-media-measurement#1929): Make this a real property
  //   on SignatureAlgorithm and replace this with the new property.
  private val SignatureAlgorithm.keyAlgorithm: String
    get() {
      return when (this) {
        SignatureAlgorithm.ECDSA_WITH_SHA256,
        SignatureAlgorithm.ECDSA_WITH_SHA384,
        SignatureAlgorithm.ECDSA_WITH_SHA512 -> "EC"
        SignatureAlgorithm.SHA_256_WITH_RSA_ENCRYPTION,
        SignatureAlgorithm.SHA_384_WITH_RSA_ENCRYPTION,
        SignatureAlgorithm.SHA_512_WITH_RSA_ENCRYPTION -> "RSA"
      }
    }
}

/** Reference Google Cloud implementation of a daemon for executing Exchange Workflows. */
fun main(args: Array<String>) = commandLineMain(AwsExampleDaemon(), args)
