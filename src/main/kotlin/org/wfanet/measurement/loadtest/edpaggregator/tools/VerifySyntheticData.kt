// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.edpaggregator.tools

import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.ByteString
import java.io.File
import java.util.logging.Logger
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.aws.kms.AwsKmsClientFactory
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.tink.AwsWebIdentityCredentials
import org.wfanet.measurement.common.crypto.tink.GCloudToAwsWifCredentials
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.gcloud.kms.GCloudToAwsKmsClientFactory
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option

@Command(
  name = "verify-synthetic-data",
  description = ["Verifies generated synthetic data can be decrypted and parsed."],
)
class VerifySyntheticData : Runnable {
  @Option(
    names = ["--kms-type"],
    description = ["Type of kms: \${COMPLETION-CANDIDATES}"],
    required = true,
  )
  lateinit var kmsType: KmsType
    private set

  @Option(names = ["--kek-uri"], description = ["The KMS kek uri."], required = true)
  lateinit var kekUri: String
    private set

  @Option(
    names = ["--local-storage-path"],
    description = ["Root path for local storage."],
    required = true,
  )
  private lateinit var storagePath: File

  @Option(
    names = ["--output-bucket"],
    description = ["The bucket name used during generation."],
    required = true,
  )
  lateinit var outputBucket: String
    private set

  @Option(
    names = ["--impression-metadata-base-path"],
    description = ["Base path where impressions are stored."],
    required = true,
  )
  lateinit var impressionMetadataBasePath: String
    private set

  @CommandLine.ArgGroup(exclusive = false, heading = "AWS flags (used with --kms-type=AWS)%n")
  var awsFlags: AwsFlags = AwsFlags()

  @CommandLine.ArgGroup(
    exclusive = false,
    heading = "GCP-to-AWS flags (used with --kms-type=GCP_TO_AWS)%n",
  )
  var gcpToAwsFlags: GcpToAwsFlags = GcpToAwsFlags()

  class AwsFlags {
    @Option(
      names = ["--aws-role-arn"],
      description = ["AWS IAM role ARN for STS AssumeRoleWithWebIdentity."],
      required = false,
      defaultValue = "",
    )
    var awsRoleArn: String = ""

    @Option(
      names = ["--aws-web-identity-token-file"],
      description = ["Path to the web identity token file."],
      required = false,
      defaultValue = "",
    )
    var awsWebIdentityTokenFile: String = ""

    @Option(
      names = ["--aws-role-session-name"],
      description = ["AWS STS role session name."],
      required = false,
      defaultValue = "verify-synthetic-data",
    )
    var awsRoleSessionName: String = "verify-synthetic-data"

    @Option(
      names = ["--aws-region"],
      description = ["AWS region for STS and KMS."],
      required = false,
      defaultValue = "",
    )
    var awsRegion: String = ""
  }

  class GcpToAwsFlags {
    @Option(
      names = ["--gcp-to-aws-role-arn"],
      description = ["AWS IAM role ARN to assume via GCP identity."],
      required = false,
      defaultValue = "",
    )
    var roleArn: String = ""

    @Option(
      names = ["--gcp-to-aws-role-session-name"],
      description = ["AWS role session name for GCP-to-AWS flow."],
      required = false,
      defaultValue = "verify-synthetic-data",
    )
    var roleSessionName: String = "verify-synthetic-data"

    @Option(
      names = ["--gcp-to-aws-region"],
      description = ["AWS region for GCP-to-AWS flow."],
      required = false,
      defaultValue = "",
    )
    var region: String = ""

    @Option(
      names = ["--gcp-audience"],
      description = ["GCP audience for WIF token exchange (Confidential Space workload pool)."],
      required = false,
      defaultValue = "",
    )
    var gcpAudience: String = ""

    @Option(
      names = ["--gcp-subject-token-type"],
      description = ["GCP subject token type."],
      required = false,
      defaultValue = "urn:ietf:params:oauth:token-type:jwt",
    )
    var subjectTokenType: String = "urn:ietf:params:oauth:token-type:jwt"

    @Option(
      names = ["--gcp-token-url"],
      description = ["GCP STS token endpoint URL."],
      required = false,
      defaultValue = "https://sts.googleapis.com/v1/token",
    )
    var tokenUrl: String = "https://sts.googleapis.com/v1/token"

    @Option(
      names = ["--gcp-credential-source-file"],
      description = ["Path to the GCP credential source file (e.g., attestation token)."],
      required = false,
      defaultValue = "/run/container_launcher/attestation_verifier_claims_token",
    )
    var credentialSourceFilePath: String =
      "/run/container_launcher/attestation_verifier_claims_token"

    @Option(
      names = ["--gcp-service-account-impersonation-url"],
      description = ["URL to impersonate a GCP service account for ID token generation."],
      required = false,
      defaultValue = "",
    )
    var serviceAccountImpersonationUrl: String = ""

    @Option(
      names = ["--aws-audience"],
      description = ["OIDC audience for the ID token presented to AWS IAM."],
      required = false,
      defaultValue = "",
    )
    var awsAudience: String = ""
  }

  override fun run() {
    val kmsClient: KmsClient =
      when (kmsType) {
        KmsType.FAKE -> FakeKmsClient()
        KmsType.GCP -> GcpKmsClient().withDefaultCredentials()
        KmsType.AWS -> {
          require(awsFlags.awsRoleArn.isNotEmpty()) {
            "--aws-role-arn is required when --kms-type=AWS"
          }
          require(awsFlags.awsWebIdentityTokenFile.isNotEmpty()) {
            "--aws-web-identity-token-file is required when --kms-type=AWS"
          }
          require(awsFlags.awsRegion.isNotEmpty()) {
            "--aws-region is required when --kms-type=AWS"
          }
          AwsKmsClientFactory()
            .getKmsClient(
              AwsWebIdentityCredentials(
                roleArn = awsFlags.awsRoleArn,
                webIdentityTokenFilePath = awsFlags.awsWebIdentityTokenFile,
                roleSessionName = awsFlags.awsRoleSessionName,
                region = awsFlags.awsRegion,
              )
            )
        }
        KmsType.GCP_TO_AWS -> {
          require(gcpToAwsFlags.roleArn.isNotEmpty()) {
            "--gcp-to-aws-role-arn is required when --kms-type=GCP_TO_AWS"
          }
          require(gcpToAwsFlags.region.isNotEmpty()) {
            "--gcp-to-aws-region is required when --kms-type=GCP_TO_AWS"
          }
          require(gcpToAwsFlags.gcpAudience.isNotEmpty()) {
            "--gcp-audience is required when --kms-type=GCP_TO_AWS"
          }
          require(gcpToAwsFlags.serviceAccountImpersonationUrl.isNotEmpty()) {
            "--gcp-service-account-impersonation-url is required when --kms-type=GCP_TO_AWS"
          }
          require(gcpToAwsFlags.awsAudience.isNotEmpty()) {
            "--aws-audience is required when --kms-type=GCP_TO_AWS"
          }
          GCloudToAwsKmsClientFactory()
            .getKmsClient(
              GCloudToAwsWifCredentials(
                gcloudAudience = gcpToAwsFlags.gcpAudience,
                subjectTokenType = gcpToAwsFlags.subjectTokenType,
                tokenUrl = gcpToAwsFlags.tokenUrl,
                credentialSourceFilePath = gcpToAwsFlags.credentialSourceFilePath,
                serviceAccountImpersonationUrl = gcpToAwsFlags.serviceAccountImpersonationUrl,
                roleArn = gcpToAwsFlags.roleArn,
                roleSessionName = gcpToAwsFlags.roleSessionName,
                region = gcpToAwsFlags.region,
                awsAudience = gcpToAwsFlags.awsAudience,
              )
            )
        }
      }

    val rootStorageClient = FileSystemStorageClient(storagePath)
    val bucketDir = storagePath.resolve(outputBucket).resolve(impressionMetadataBasePath)

    logger.info("Scanning for date shards in: $bucketDir")
    check(bucketDir.exists()) { "Directory does not exist: $bucketDir" }

    val dsDir = bucketDir.resolve("ds")
    check(dsDir.exists()) { "No ds/ directory found under: $bucketDir" }

    val dateDirs = dsDir.listFiles()?.filter { it.isDirectory }?.sorted() ?: emptyList()
    check(dateDirs.isNotEmpty()) { "No date shard directories found under: $dsDir" }

    logger.info("Found ${dateDirs.size} date shards")
    var totalImpressions = 0
    var totalDays = 0
    var errors = 0

    for (dateDir in dateDirs) {
      val date = dateDir.name
      val metadataFiles = dateDir.walkTopDown().filter { it.name == "metadata.binpb" }.toList()

      for (metadataFile in metadataFiles) {
        try {
          logger.info("\n=== Processing date: $date ===")

          val relativePath = metadataFile.relativeTo(storagePath.resolve(outputBucket)).path
          logger.info("Reading metadata from: $relativePath")

          val metadataBlob = runBlocking {
            rootStorageClient.getBlob("$outputBucket/$relativePath")
          }
          check(metadataBlob != null) { "Metadata blob not found: $outputBucket/$relativePath" }

          val blobDetailsBytes = runBlocking { metadataBlob.read().toList() }
          val blobDetails =
            BlobDetails.parseFrom(
              blobDetailsBytes.fold(ByteString.EMPTY) { acc, bs -> acc.concat(bs) }
            )

          logger.info("  Blob URI: ${blobDetails.blobUri}")
          logger.info("  KEK URI: ${blobDetails.encryptedDek.kekUri}")
          logger.info("  DEK type: ${blobDetails.encryptedDek.typeUrl}")
          logger.info("  DEK format: ${blobDetails.encryptedDek.protobufFormat}")
          logger.info("  Event group ref ID: ${blobDetails.eventGroupReferenceId}")
          logger.info("  Model line: ${blobDetails.modelLine}")
          logger.info(
            "  Interval: ${blobDetails.interval.startTime} - ${blobDetails.interval.endTime}"
          )

          val encryptedDek = blobDetails.encryptedDek
          check(encryptedDek.kekUri == kekUri) {
            "KEK URI mismatch: expected $kekUri, got ${encryptedDek.kekUri}"
          }

          val impressionsBlobUri = blobDetails.blobUri
          val selectedStorageClient = SelectedStorageClient(impressionsBlobUri, storagePath)
          val decryptionClient =
            selectedStorageClient.withEnvelopeEncryption(kmsClient, kekUri, encryptedDek.ciphertext)

          val impressionsBlobKey =
            impressionsBlobUri.removePrefix("file:///").removePrefix("$outputBucket/")

          logger.info("  Decrypting impressions from blob key: $impressionsBlobKey")

          val mesosClient = MesosRecordIoStorageClient(decryptionClient)
          val impressionsBlob = runBlocking { mesosClient.getBlob(impressionsBlobKey) }
          check(impressionsBlob != null) { "Impressions blob not found: $impressionsBlobKey" }

          val records = runBlocking { impressionsBlob.read().toList() }
          logger.info("  Decrypted ${records.size} impression records")

          for ((index, record) in records.withIndex()) {
            val impression = LabeledImpression.parseFrom(record)
            check(impression.vid > 0) { "Invalid VID: ${impression.vid}" }
            check(impression.hasEvent()) { "Missing event in impression $index" }
            check(impression.hasEventTime()) { "Missing event time in impression $index" }

            val testEvent = impression.event.unpack(TestEvent::class.java)
            check(testEvent != null) { "Failed to unpack TestEvent from impression $index" }

            if (index < 3) {
              logger.info(
                "  Record[$index]: vid=${impression.vid}, eventTime=${impression.eventTime}"
              )
            }
          }

          totalImpressions += records.size
          totalDays++
          logger.info("  PASS: $date - ${records.size} impressions verified")
        } catch (e: Exception) {
          errors++
          logger.severe("  FAIL: $date - ${e.message}")
          e.printStackTrace()
        }
      }
    }

    logger.info("\n========== VERIFICATION SUMMARY ==========")
    logger.info("  Days processed: $totalDays")
    logger.info("  Total impressions decrypted: $totalImpressions")
    logger.info("  Errors: $errors")
    if (errors == 0) {
      logger.info("  RESULT: ALL PASSED")
    } else {
      logger.severe("  RESULT: $errors FAILURES")
    }
    logger.info("==========================================\n")
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }

  init {
    AeadConfig.register()
    StreamingAeadConfig.register()
  }
}

fun main(args: Array<String>) = commandLineMain(VerifySyntheticData(), args)
