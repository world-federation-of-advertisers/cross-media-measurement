// Copyright 2025 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.common.crypto.tink.AwsWifCredentials
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.crypto.tink.withEnvelopeEncryption
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
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

  @Option(
    names = ["--aws-role-arn"],
    description = ["AWS IAM role ARN. Required when --kms-type=AWS."],
    required = false,
    defaultValue = "",
  )
  lateinit var awsRoleArn: String
    private set

  @Option(
    names = ["--aws-web-identity-token-file"],
    description = ["AWS web identity token file path. Required when --kms-type=AWS."],
    required = false,
    defaultValue = "",
  )
  lateinit var awsWebIdentityTokenFile: String
    private set

  @Option(
    names = ["--aws-role-session-name"],
    description = ["AWS STS role session name."],
    required = false,
    defaultValue = "verify-synthetic-data",
  )
  lateinit var awsRoleSessionName: String
    private set

  @Option(
    names = ["--aws-region"],
    description = ["AWS region. Required when --kms-type=AWS."],
    required = false,
    defaultValue = "",
  )
  lateinit var awsRegion: String
    private set

  override fun run() {
    val kmsClient: KmsClient =
      when (kmsType) {
        KmsType.FAKE -> {
          FakeKmsClient()
        }
        KmsType.GCP -> {
          GcpKmsClient().withDefaultCredentials()
        }
        KmsType.AWS -> {
          require(awsRoleArn.isNotEmpty()) { "--aws-role-arn is required when --kms-type=AWS" }
          require(awsWebIdentityTokenFile.isNotEmpty()) {
            "--aws-web-identity-token-file is required when --kms-type=AWS"
          }
          require(awsRegion.isNotEmpty()) { "--aws-region is required when --kms-type=AWS" }
          val awsConfig =
            AwsWifCredentials(
              roleArn = awsRoleArn,
              webIdentityTokenFilePath = awsWebIdentityTokenFile,
              roleSessionName = awsRoleSessionName,
              region = awsRegion,
            )
          AwsKmsClientFactory().getKmsClient(awsConfig)
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
      // Find metadata.binpb files recursively
      val metadataFiles = dateDir.walkTopDown().filter { it.name == "metadata.binpb" }.toList()

      for (metadataFile in metadataFiles) {
        try {
          logger.info("\n=== Processing date: $date ===")

          // Read metadata
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

          // Get encrypted DEK
          val encryptedDek = blobDetails.encryptedDek
          check(encryptedDek.kekUri == kekUri) {
            "KEK URI mismatch: expected $kekUri, got ${encryptedDek.kekUri}"
          }

          // Read and decrypt impressions
          val impressionsBlobUri = blobDetails.blobUri
          val selectedStorageClient = SelectedStorageClient(impressionsBlobUri, storagePath)
          val decryptionClient =
            selectedStorageClient.withEnvelopeEncryption(kmsClient, kekUri, encryptedDek.ciphertext)

          // Parse the blob key from the URI
          val impressionsBlobKey =
            impressionsBlobUri.removePrefix("file:///").removePrefix("$outputBucket/")

          logger.info("  Decrypting impressions from blob key: $impressionsBlobKey")

          val mesosClient = MesosRecordIoStorageClient(decryptionClient)
          val impressionsBlob = runBlocking { mesosClient.getBlob(impressionsBlobKey) }
          check(impressionsBlob != null) { "Impressions blob not found: $impressionsBlobKey" }

          val records = runBlocking { impressionsBlob.read().toList() }
          logger.info("  Decrypted ${records.size} impression records")

          // Parse and validate each record
          for ((index, record) in records.withIndex()) {
            val impression = LabeledImpression.parseFrom(record)
            check(impression.vid > 0) { "Invalid VID: ${impression.vid}" }
            check(impression.hasEvent()) { "Missing event in impression $index" }
            check(impression.hasEventTime()) { "Missing event time in impression $index" }

            // Try to unpack the event
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
