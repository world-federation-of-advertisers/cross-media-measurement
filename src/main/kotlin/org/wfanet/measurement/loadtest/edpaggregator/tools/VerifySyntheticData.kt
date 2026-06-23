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
import com.google.protobuf.Message
import com.google.protobuf.util.JsonFormat
import java.io.File
import java.util.logging.Logger
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.aws.kms.AwsKmsClientFactory
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.tink.AwsWebIdentityCredentials
import org.wfanet.measurement.common.crypto.tink.GCloudToAwsWifCredentials
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.gcloud.kms.GCloudToAwsKmsClientFactory
import org.wfanet.measurement.storage.SelectedStorageClient
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
    names = ["--fake-kek-keyset-file"],
    description =
      [
        "Path to the Tink keyset file backing the FAKE KEK. " +
          "Required when --kms-type=FAKE; the keyset must be the one written by " +
          "GenerateSyntheticData via its --fake-kek-keyset-file flag. " +
          "Must NOT be set for any other KMS type."
      ],
    required = false,
  )
  private var fakeKekKeysetFile: File? = null

  @Option(
    names = ["--scheme"],
    description =
      [
        "Storage URI scheme (e.g. gs://, file:///). " +
          "Used with --output-bucket and --base-path to scan for metadata files."
      ],
    required = false,
    defaultValue = DEFAULT_SCHEME,
  )
  lateinit var scheme: String
    private set

  @Option(
    names = ["--output-bucket"],
    description = ["The bucket name used during generation."],
    required = false,
    defaultValue = "",
  )
  var outputBucket: String = ""
    private set

  @Option(
    names = ["--base-path"],
    description = ["Base path where impressions are stored."],
    required = false,
    defaultValue = "",
  )
  var basePath: String = ""
    private set

  @Option(
    names = ["--local-storage-path"],
    description = ["Root path for local storage. Required when --scheme is file:///."],
    required = false,
  )
  private var storagePath: File? = null

  @Option(
    names = ["--metadata-uri"],
    description =
      [
        "URI of a metadata file to verify. Supports both binary protobuf (.binpb) and " +
          "JSON metadata formats. May be specified multiple times. " +
          "Mutually exclusive with --output-bucket/--base-path."
      ],
    required = false,
  )
  var metadataUris: List<String> = emptyList()
    private set

  @Option(
    names = ["--metadata-prefix"],
    description =
      [
        "Filename prefix used to identify metadata blobs during scans. Defaults to " +
          "\"metadata\" (matches files written by GenerateSyntheticData and " +
          "DataAvailabilitySync). Set to a different prefix when verifying data produced " +
          "by a writer that uses a different naming convention. Must not be empty. " +
          "Only consulted with --output-bucket/--base-path scans; ignored when " +
          "--metadata-uri is given."
      ],
    required = false,
    defaultValue = DEFAULT_METADATA_PREFIX,
  )
  lateinit var metadataPrefix: String
    private set

  @Option(
    names = ["--event-message-type-url"],
    description =
      [
        "Type URL of the event message type carried by each LabeledImpression. " +
          "Defaults to TestEvent.",
        "When set to a type other than TestEvent, --event-message-descriptor-set must be supplied.",
      ],
    required = false,
    defaultValue = GenerateSyntheticData.DEFAULT_EVENT_MESSAGE_TYPE_URL,
  )
  lateinit var eventMessageTypeUrl: String
    private set

  @Option(
    names = ["--event-message-descriptor-set"],
    description =
      [
        "Path to a serialized FileDescriptorSet containing the event message type referenced by " +
          "--event-message-type-url and its dependencies.",
        "May be specified multiple times.",
        "May be omitted only when the event message type is TestEvent (the default).",
      ],
    required = false,
  )
  var eventMessageDescriptorSetFiles: List<File> = emptyList()
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
    require(kmsType == KmsType.FAKE || fakeKekKeysetFile == null) {
      "--fake-kek-keyset-file is only valid when --kms-type=FAKE"
    }
    val kmsClient: KmsClient =
      when (kmsType) {
        KmsType.FAKE -> {
          val file =
            requireNotNull(fakeKekKeysetFile) {
              "--fake-kek-keyset-file is required when --kms-type=FAKE"
            }
          require(file.exists()) {
            "--fake-kek-keyset-file '$file' does not exist; run GenerateSyntheticData with the " +
              "same --fake-kek-keyset-file first to materialize the FAKE KEK keyset"
          }
          GenerateSyntheticData.buildFakeKmsClient(kekUri, file)
        }
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

    val resolvedMetadataUris: List<String> =
      if (metadataUris.isNotEmpty()) {
        require(outputBucket.isEmpty() && basePath.isEmpty()) {
          "--metadata-uri is mutually exclusive with --output-bucket/--base-path"
        }
        require(scheme == DEFAULT_SCHEME) {
          "--scheme is only used with --output-bucket/--base-path scans and must not be set " +
            "when --metadata-uri is given (each URI carries its own scheme)"
        }
        require(metadataPrefix == DEFAULT_METADATA_PREFIX) {
          "--metadata-prefix is only used with --output-bucket/--base-path scans and must " +
            "not be set when --metadata-uri is given"
        }
        metadataUris
      } else {
        require(outputBucket.isNotEmpty() && basePath.isNotEmpty()) {
          "Either --metadata-uri or --output-bucket/--base-path must be provided"
        }
        require(metadataPrefix.isNotEmpty()) { "--metadata-prefix must not be empty" }
        scanForMetadata(scheme, outputBucket, basePath, storagePath, metadataPrefix)
      }

    // SelectedStorageClient requires a root directory for file:/// blobs; surface a clear
    // error here instead of letting FileSystemStorageClient throw a bare NPE downstream.
    if (resolvedMetadataUris.any { it.startsWith("file:///") }) {
      requireNotNull(storagePath) {
        "--local-storage-path is required when any --metadata-uri uses file:///"
      }
    }

    val eventMessageInstance: Message =
      GenerateSyntheticData.resolveEventMessageInstance(
        eventMessageTypeUrl,
        eventMessageDescriptorSetFiles,
      )

    val result =
      verifyMetadata(
        kmsClient = kmsClient,
        kekUri = kekUri,
        metadataUris = resolvedMetadataUris,
        storagePath = storagePath,
        eventMessageInstance = eventMessageInstance,
        expectedEventTypeUrl = eventMessageTypeUrl,
      )
    lastResult = result

    logger.info("\n========== VERIFICATION SUMMARY ==========")
    logger.info("  Blobs processed: ${result.totalBlobsProcessed}")
    logger.info("  Total impressions decrypted: ${result.totalImpressions}")
    for ((eventGroupReferenceId, count) in
      result.impressionsByEventGroupReferenceId.toSortedMap()) {
      logger.info("  Event group '$eventGroupReferenceId': $count impressions")
    }
    logger.info("  Errors: ${result.errors}")
    if (result.errors == 0) {
      logger.info("  RESULT: ALL PASSED")
    } else {
      logger.severe("  RESULT: ${result.errors} FAILURES")
    }
    logger.info("==========================================\n")
  }

  /**
   * The aggregate result from the most recent [run] invocation, or `null` if [run] has not yet been
   * called. Exposed for tests that drive the command via picocli and want to assert on counts.
   */
  var lastResult: VerificationResult? = null
    private set

  /**
   * Aggregate result of a [Companion.verifySyntheticData] invocation.
   *
   * @property totalImpressions Total number of [LabeledImpression] records successfully decrypted
   *   and parsed across all metadata blobs.
   * @property totalBlobsProcessed Total number of metadata blobs that were processed without error.
   *   Each blob corresponds to a single (date, event group) shard, so multi-event-group runs report
   *   a count greater than the number of distinct dates.
   * @property errors Number of metadata blobs that failed to be processed.
   * @property impressionsByEventGroupReferenceId Number of impressions decrypted per
   *   `event_group_reference_id` recorded in `BlobDetails`. Useful for asserting that all streams
   *   in a multi-event-group run are present.
   */
  data class VerificationResult(
    val totalImpressions: Int,
    val totalBlobsProcessed: Int,
    val errors: Int,
    val impressionsByEventGroupReferenceId: Map<String, Int>,
  )

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /** Default value of the --scheme flag (local filesystem). */
    const val DEFAULT_SCHEME = "file:///"

    /** Default value of the --metadata-prefix flag (matches GenerateSyntheticData output). */
    const val DEFAULT_METADATA_PREFIX = "metadata"

    /**
     * True if this filename ends in a metadata extension the verifier knows how to parse (`.binpb`
     * for binary protobuf or `.json` for JSON).
     */
    private fun String.isSupportedMetadataExtension(): Boolean =
      endsWith(".binpb") || endsWith(".json")

    init {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }

    /**
     * Scans for metadata files under the given storage prefix.
     *
     * Constructs the base URI as [scheme][outputBucket]/[basePath] and lists all blobs whose keys
     * begin with [metadataPrefix] and end in a supported extension. Returns fully qualified URIs.
     */
    private fun scanForMetadata(
      scheme: String,
      outputBucket: String,
      basePath: String,
      storagePath: File?,
      metadataPrefix: String,
    ): List<String> {
      if (scheme == "file:///") {
        requireNotNull(storagePath) { "--local-storage-path is required when --scheme is file:///" }
        val scanDir = storagePath.resolve(outputBucket).resolve(basePath)
        logger.info("Scanning for metadata files in: $scanDir")
        check(scanDir.exists()) { "Directory does not exist: $scanDir" }

        val metadataFiles =
          scanDir
            .walkTopDown()
            .filter { it.name.startsWith(metadataPrefix) && it.name.isSupportedMetadataExtension() }
            .toList()
            .sorted()
        check(metadataFiles.isNotEmpty()) {
          "No metadata files found under: $scanDir (looking for files starting with " +
            "\"$metadataPrefix\" and ending in .binpb or .json)"
        }
        logger.info("Found ${metadataFiles.size} metadata files")

        return metadataFiles.map { file ->
          val relativePath = file.relativeTo(storagePath).path
          "file:///$relativePath"
        }
      }

      val prefix = "$basePath/"
      val baseUri = "$scheme$outputBucket"
      val blobUri = SelectedStorageClient.parseBlobUri("$baseUri/$prefix")
      val storageClient = SelectedStorageClient(blobUri)

      logger.info("Scanning for metadata files under: $baseUri/$prefix")
      val uris = runBlocking {
        storageClient
          .listBlobs(prefix)
          .toList()
          .filter {
            val name = it.blobKey.substringAfterLast("/")
            name.startsWith(metadataPrefix) && name.isSupportedMetadataExtension()
          }
          .map { "$baseUri/${it.blobKey}" }
          .sorted()
      }
      check(uris.isNotEmpty()) {
        "No metadata files found under: $baseUri/$prefix (looking for files starting with " +
          "\"$metadataPrefix\" and ending in .binpb or .json)"
      }
      logger.info("Found ${uris.size} metadata files")
      return uris
    }

    /**
     * Verifies impression blobs by reading metadata from the given URIs.
     *
     * Supports both binary protobuf and JSON metadata formats, detected by file extension (`.json`
     * for JSON, anything else for binary protobuf).
     */
    private fun verifyMetadata(
      kmsClient: KmsClient,
      kekUri: String,
      metadataUris: List<String>,
      storagePath: File?,
      eventMessageInstance: Message,
      expectedEventTypeUrl: String,
    ): VerificationResult {
      var totalImpressions = 0
      var totalBlobsProcessed = 0
      var errors = 0
      val impressionsByEventGroupReferenceId = mutableMapOf<String, Int>()

      for (metadataUri in metadataUris) {
        try {
          logger.info("\n=== Processing: $metadataUri ===")

          val metadataBlobUri = SelectedStorageClient.parseBlobUri(metadataUri)
          val metadataStorageClient = SelectedStorageClient(metadataBlobUri, storagePath)

          val metadataBytes = runBlocking {
            val blob =
              metadataStorageClient.getBlob(metadataBlobUri.key)
                ?: throw IllegalStateException("Metadata not found: ${metadataBlobUri.key}")
            blob.read().flatten()
          }

          val isJson = metadataUri.endsWith(".json")
          val blobDetails: BlobDetails
          val encryptedDek: EncryptedDek

          if (isJson) {
            val builder = BlobDetails.newBuilder()
            JsonFormat.parser().ignoringUnknownFields().merge(metadataBytes.toStringUtf8(), builder)
            blobDetails = builder.build()
            encryptedDek = blobDetails.encryptedDek
          } else {
            blobDetails = BlobDetails.parseFrom(metadataBytes)
            encryptedDek = blobDetails.encryptedDek
          }

          logger.info("  Blob URI: ${blobDetails.blobUri}")
          logger.info("  KEK URI: ${encryptedDek.kekUri}")
          logger.info("  DEK type: ${encryptedDek.typeUrl}")
          logger.info("  DEK format: ${encryptedDek.protobufFormat}")
          logger.info("  Event group ref ID: ${blobDetails.eventGroupReferenceId}")
          logger.info("  Model line: ${blobDetails.modelLine}")

          check(encryptedDek.kekUri == kekUri) {
            "KEK URI mismatch: expected $kekUri, got ${encryptedDek.kekUri}"
          }

          val impressionsBlobUri = SelectedStorageClient.parseBlobUri(blobDetails.blobUri)
          val impressionsStorageClient = SelectedStorageClient(impressionsBlobUri, storagePath)
          val mesosClient =
            EncryptedStorage.buildEncryptedMesosStorageClient(
              impressionsStorageClient,
              kmsClient,
              kekUri,
              encryptedDek,
            )

          val blobKey = impressionsBlobUri.key
          logger.info("  Decrypting from blob key: $blobKey")

          val records = runBlocking {
            val blob =
              mesosClient.getBlob(blobKey)
                ?: throw IllegalStateException("Impression blob not found: $blobKey")
            blob.read().toList()
          }
          logger.info("  Decrypted ${records.size} impression records")

          for ((index, record) in records.withIndex()) {
            val impression = LabeledImpression.parseFrom(record)
            check(impression.vid > 0) { "Invalid VID: ${impression.vid}" }
            check(impression.hasEvent()) { "Missing event in impression $index" }
            check(impression.hasEventTime()) { "Missing event time in impression $index" }
            for ((entityKeyIndex, entityKey) in impression.entityKeysList.withIndex()) {
              check(entityKey.entityType.isNotEmpty()) {
                "EntityKey[$entityKeyIndex] on impression $index has empty entity_type"
              }
              check(entityKey.entityId.isNotEmpty()) {
                "EntityKey[$entityKeyIndex] on impression $index has empty entity_id"
              }
            }

            check(impression.event.typeUrl == expectedEventTypeUrl) {
              "Event type URL mismatch on impression $index: " +
                "expected $expectedEventTypeUrl, got ${impression.event.typeUrl}"
            }
            eventMessageInstance.newBuilderForType().mergeFrom(impression.event.value).build()

            if (index < 3) {
              val entityKeysSummary =
                impression.entityKeysList.joinToString(", ") { "${it.entityType}=${it.entityId}" }
              logger.info(
                "  Record[$index]: vid=${impression.vid}, " +
                  "eventTime=${impression.eventTime}, entityKeys=[$entityKeysSummary]"
              )
            }
          }

          totalImpressions += records.size
          totalBlobsProcessed++
          impressionsByEventGroupReferenceId.merge(
            blobDetails.eventGroupReferenceId,
            records.size,
          ) { old, new ->
            old + new
          }
          logger.info("  PASS: $metadataUri - ${records.size} impressions verified")
        } catch (e: Exception) {
          errors++
          logger.severe("  FAIL: $metadataUri - ${e.message}")
          e.printStackTrace()
        }
      }

      return VerificationResult(
        totalImpressions = totalImpressions,
        totalBlobsProcessed = totalBlobsProcessed,
        errors = errors,
        impressionsByEventGroupReferenceId = impressionsByEventGroupReferenceId.toMap(),
      )
    }
  }
}

fun main(args: Array<String>) = commandLineMain(VerifySyntheticData(), args)
