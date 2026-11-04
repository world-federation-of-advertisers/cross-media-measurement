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

package org.wfanet.measurement.edpaggregator.tools

import com.google.crypto.tink.Aead
import com.google.crypto.tink.InsecureSecretKeyAccess
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.RegistryConfiguration
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.Message
import com.google.protobuf.util.JsonFormat
import com.google.protobuf.util.Timestamps
import java.io.File
import java.util.concurrent.Callable
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.system.exitProcess
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.aws.kms.AwsKmsClientFactory
import org.wfanet.measurement.common.ProtoReflection
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

/** Key management service used to decrypt the data encryption key. */
enum class KmsType {
  FAKE,
  GCP,
  AWS,
  GCP_TO_AWS,
}

/**
 * Command-line verifier for encrypted VID-labeled impression blobs and their [BlobDetails]
 * sidecars.
 *
 * Each metadata blob is verified independently. The command returns a nonzero exit code when any
 * blob cannot be read, decrypted, or validated.
 */
@Command(
  name = "verify-vid-labeled-impressions",
  description = ["Verifies VID-labeled impression blobs can be decrypted and parsed."],
  mixinStandardHelpOptions = true,
)
class VerifyVidLabeledImpressions : Callable<Int> {
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
          "Required when --kms-type=FAKE; this option is only for local testing. " +
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
          "\"metadata\". The default matches every nonempty file whose name contains " +
          "\"metadata\", as DataAvailabilitySync does. Set a different value only to add a " +
          "stricter filename-prefix filter. Metadata filenames must still contain \"metadata\". " +
          "Must not be empty. " +
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
        "Expected type URL of the event message carried by each LabeledImpression.",
        "When set, --event-message-descriptor-set must also be supplied so the payload can be parsed.",
      ],
    required = false,
    defaultValue = "",
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
        "Required when --event-message-type-url is set.",
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

  /** Options for using AWS KMS with web identity credentials. */
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
      defaultValue = "verify-vid-labeled-impressions",
    )
    var awsRoleSessionName: String = "verify-vid-labeled-impressions"

    @Option(
      names = ["--aws-region"],
      description = ["AWS region for STS and KMS."],
      required = false,
      defaultValue = "",
    )
    var awsRegion: String = ""
  }

  /** Options for using AWS KMS through GCP-to-AWS workload identity federation. */
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
      defaultValue = "verify-vid-labeled-impressions",
    )
    var roleSessionName: String = "verify-vid-labeled-impressions"

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

  override fun call(): Int = runBlocking { runSuspending() }

  private suspend fun runSuspending(): Int {
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
          require(file.exists()) { "--fake-kek-keyset-file '$file' does not exist" }
          buildFakeKmsClient(kekUri, file)
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

    val eventMessageInstance: Message? =
      if (eventMessageTypeUrl.isEmpty()) {
        require(eventMessageDescriptorSetFiles.isEmpty()) {
          "--event-message-descriptor-set requires --event-message-type-url"
        }
        null
      } else {
        resolveEventMessageInstance(eventMessageTypeUrl, eventMessageDescriptorSetFiles)
      }

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
    for ((entityKey, count) in
      result.impressionsByEntityKey.toSortedMap(
        compareBy<EntityKey> { it.entityType }.thenBy { it.entityId }
      )) {
      logger.info("  Entity '${entityKey.entityType}=${entityKey.entityId}': $count impressions")
    }
    logger.info("  Errors: ${result.errors}")
    if (result.errors == 0) {
      logger.info("  RESULT: ALL PASSED")
    } else {
      logger.severe("  RESULT: ${result.errors} FAILURES")
    }
    logger.info("==========================================\n")
    return if (result.errors == 0) 0 else 1
  }

  /**
   * The aggregate result from the most recent [call] invocation, or `null` if [call] has not yet
   * been called.
   */
  var lastResult: VerificationResult? = null
    private set

  /**
   * Aggregate result of a verification run.
   *
   * @property totalImpressions Total number of [LabeledImpression] records successfully decrypted
   *   and parsed across all metadata blobs.
   * @property totalBlobsProcessed Total number of metadata blobs that were processed without error.
   * @property errors Number of metadata blobs that failed to be processed.
   * @property impressionsByEventGroupReferenceId Number of impressions decrypted per
   *   `event_group_reference_id` recorded in `BlobDetails`. Useful for asserting that all streams
   *   in a multi-event-group run are present.
   * @property impressionsByEntityKey Number of impressions carrying each distinct entity key.
   */
  data class VerificationResult(
    val totalImpressions: Long,
    val totalBlobsProcessed: Int,
    val errors: Int,
    val impressionsByEventGroupReferenceId: Map<String, Long>,
    val impressionsByEntityKey: Map<EntityKey, Long>,
  )

  /**
   * Identifies an entity associated with a VID-labeled impression.
   *
   * @property entityType Type of entity, such as `creative` or `campaign`.
   * @property entityId Identifier within [entityType].
   */
  data class EntityKey(val entityType: String, val entityId: String)

  private enum class VerificationStage(val description: String) {
    READING_METADATA("reading the metadata sidecar"),
    PARSING_METADATA("parsing the BlobDetails sidecar"),
    VALIDATING_METADATA("validating the BlobDetails sidecar"),
    OPENING_DATA_ENCRYPTION_KEY("parsing and unwrapping the wrapped DEK"),
    READING_IMPRESSIONS("decrypting the impression blob or decoding Mesos RecordIO"),
    PARSING_IMPRESSION("parsing a bare LabeledImpression"),
    VALIDATING_IMPRESSION("validating a LabeledImpression"),
    PARSING_EVENT("parsing the event payload"),
    VALIDATING_BLOB("validating the complete impression blob"),
  }

  companion object {
    private val logger: Logger = Logger.getLogger(VerifyVidLabeledImpressions::class.java.name)

    private val EVENT_MESSAGE_EXTENSION_REGISTRY: ExtensionRegistry =
      ExtensionRegistry.newInstance()
        .also { EventAnnotationsProto.registerAllExtensions(it) }
        .unmodifiable

    private val COMPILED_PROTOBUF_TYPES: Iterable<Descriptors.FileDescriptor> =
      (ProtoReflection.WELL_KNOWN_TYPES.asSequence() + EventAnnotationsProto.getDescriptor())
        .asIterable()

    /** Default value of the --scheme flag (local filesystem). */
    const val DEFAULT_SCHEME = "file:///"

    /** Default value of the --metadata-prefix flag. */
    const val DEFAULT_METADATA_PREFIX = "metadata"

    /** Unreserved characters from RFC 3986. */
    private val URL_SAFE_STRING = Regex("^[a-zA-Z0-9._~-]+$")

    /**
     * True if this filename ends, case-insensitively, in a metadata extension the verifier knows
     * how to parse (`.binpb` for binary protobuf or `.json` for JSON).
     */
    private fun String.isSupportedMetadataExtension(): Boolean {
      val lowercaseName = lowercase()
      return lowercaseName.endsWith(".binpb") || lowercaseName.endsWith(".json")
    }

    /** True if this filename is discoverable by `DataAvailabilitySync`. */
    private fun String.isDiscoverableMetadataName(): Boolean =
      contains(DEFAULT_METADATA_PREFIX, ignoreCase = true)

    /** Applies EDPA discovery and an optional stricter caller-supplied prefix. */
    private fun String.matchesMetadataScan(metadataPrefix: String): Boolean =
      isDiscoverableMetadataName() &&
        (metadataPrefix == DEFAULT_METADATA_PREFIX || startsWith(metadataPrefix))

    init {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }

    private fun buildFakeKmsClient(kekUri: String, keysetFile: File): KmsClient {
      val keysetHandle =
        TinkProtoKeysetFormat.parseKeyset(
          keysetFile.readBytes(),
          InsecureSecretKeyAccess.get(),
          RegistryConfiguration.get(),
        )
      val aead = keysetHandle.getPrimitive(RegistryConfiguration.get(), Aead::class.java)
      return object : KmsClient {
        override fun doesSupport(keyUri: String): Boolean = keyUri == kekUri

        override fun withCredentials(credentialPath: String): KmsClient = this

        override fun withDefaultCredentials(): KmsClient = this

        override fun getAead(keyUri: String): Aead {
          require(doesSupport(keyUri)) { "Unsupported fake KMS key URI" }
          return aead
        }
      }
    }

    private fun resolveEventMessageInstance(
      eventMessageTypeUrl: String,
      descriptorSetFiles: Collection<File>,
    ): Message {
      require(descriptorSetFiles.isNotEmpty()) {
        "--event-message-descriptor-set is required when --event-message-type-url is set"
      }
      val expectedFullName = eventMessageTypeUrl.substringAfterLast('/')
      val fileDescriptorSets: List<DescriptorProtos.FileDescriptorSet> =
        descriptorSetFiles.map { file ->
          file.inputStream().use { input ->
            DescriptorProtos.FileDescriptorSet.parseFrom(input, EVENT_MESSAGE_EXTENSION_REGISTRY)
          }
        }
      val descriptors: List<Descriptors.Descriptor> =
        ProtoReflection.buildDescriptors(fileDescriptorSets, COMPILED_PROTOBUF_TYPES)
      val descriptor =
        descriptors.firstOrNull { it.fullName == expectedFullName }
          ?: error("Event message type $expectedFullName not found in supplied descriptor sets")
      return DynamicMessage.getDefaultInstance(descriptor)
    }

    /**
     * Scans for metadata files under the given storage prefix.
     *
     * Constructs the base URI as [scheme][outputBucket]/[basePath] and lists every nonempty blob
     * discoverable by `DataAvailabilitySync`. A non-default [metadataPrefix] adds a stricter prefix
     * filter. Unsupported extensions are returned so verification can report them as errors rather
     * than silently approving a directory that EDPA will reject. Returns fully qualified URIs.
     */
    private suspend fun scanForMetadata(
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
            .filter { it.isFile && it.length() > 0L && it.name.matchesMetadataScan(metadataPrefix) }
            .toList()
            .sorted()
        check(metadataFiles.isNotEmpty()) {
          "No nonempty metadata files discoverable by DataAvailabilitySync found under: $scanDir"
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
      val uris =
        storageClient
          .listBlobs(prefix)
          .toList()
          .filter {
            val name = it.blobKey.substringAfterLast("/")
            it.size > 0L && name.matchesMetadataScan(metadataPrefix)
          }
          .map { "$baseUri/${it.blobKey}" }
          .sorted()
      check(uris.isNotEmpty()) {
        "No nonempty metadata files discoverable by DataAvailabilitySync found under: " +
          "$baseUri/$prefix"
      }
      logger.info("Found ${uris.size} metadata files")
      return uris
    }

    /**
     * Verifies impression blobs by reading metadata from the given URIs.
     *
     * Supports both binary protobuf and JSON metadata formats, detected by the `.binpb` and `.json`
     * file extensions.
     */
    private suspend fun verifyMetadata(
      kmsClient: KmsClient,
      kekUri: String,
      metadataUris: List<String>,
      storagePath: File?,
      eventMessageInstance: Message?,
      expectedEventTypeUrl: String,
    ): VerificationResult {
      var totalImpressions = 0L
      var totalBlobsProcessed = 0
      var errors = 0
      val impressionsByEventGroupReferenceId = mutableMapOf<String, Long>()
      val impressionsByEntityKey = mutableMapOf<EntityKey, Long>()

      for (metadataUri in metadataUris) {
        var stage = VerificationStage.READING_METADATA
        try {
          logger.info("\n=== Processing: $metadataUri ===")

          val metadataBlobUri = SelectedStorageClient.parseBlobUri(metadataUri)
          val metadataFileName = metadataBlobUri.key.substringAfterLast("/")
          check(metadataFileName.isDiscoverableMetadataName()) {
            "Metadata filename must contain \"metadata\": $metadataFileName"
          }
          check(metadataFileName.isSupportedMetadataExtension()) {
            "Unsupported file extension for metadata: $metadataFileName"
          }
          val metadataStorageClient = SelectedStorageClient(metadataBlobUri, storagePath)

          val metadataBlob =
            metadataStorageClient.getBlob(metadataBlobUri.key)
              ?: throw IllegalStateException("Metadata not found: ${metadataBlobUri.key}")
          val metadataBytes = metadataBlob.read().flatten()

          stage = VerificationStage.PARSING_METADATA
          val isJson = metadataFileName.lowercase().endsWith(".json")
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

          stage = VerificationStage.VALIDATING_METADATA
          check(blobDetails.blobUri.isNotEmpty()) { "BlobDetails.blob_uri is empty" }
          check(blobDetails.hasEncryptedDek()) { "BlobDetails.encrypted_dek is missing" }
          check(ModelLineKey.fromName(blobDetails.modelLine) != null) {
            "Invalid model line resource name: ${blobDetails.modelLine}"
          }
          check(blobDetails.interval.hasStartTime() && blobDetails.interval.hasEndTime()) {
            "BlobDetails.interval must have start_time and end_time"
          }
          Timestamps.checkValid(blobDetails.interval.startTime)
          Timestamps.checkValid(blobDetails.interval.endTime)
          check(
            Timestamps.compare(blobDetails.interval.startTime, blobDetails.interval.endTime) < 0
          ) {
            "BlobDetails.interval start_time must be before end_time"
          }
          check(blobDetails.eventGroupReferenceId.isNotEmpty() || blobDetails.entityKeysCount > 0) {
            "BlobDetails must contain event_group_reference_id or entity_keys"
          }
          val expectedEntityKeys =
            blobDetails.entityKeysList
              .flatMap { group ->
                check(group.entityType.isNotEmpty()) { "BlobDetails contains an empty entity_type" }
                check(URL_SAFE_STRING.matches(group.entityType)) {
                  "BlobDetails contains a non-URL-safe entity_type"
                }
                check(group.entityIdsCount > 0) {
                  "BlobDetails contains an entity key group with no entity_ids"
                }
                group.entityIdsList.map { entityId ->
                  check(entityId.isNotEmpty()) { "BlobDetails contains an empty entity_id" }
                  check(URL_SAFE_STRING.matches(entityId)) {
                    "BlobDetails contains a non-URL-safe entity_id"
                  }
                  EntityKey(group.entityType, entityId)
                }
              }
              .toSet()

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
          check(metadataBlobUri.scheme == impressionsBlobUri.scheme) {
            "Metadata and impression blob must use the same storage scheme"
          }
          check(metadataBlobUri.bucket == impressionsBlobUri.bucket) {
            "Metadata and impression blob must be in the same bucket"
          }
          val impressionsStorageClient = SelectedStorageClient(impressionsBlobUri, storagePath)
          stage = VerificationStage.OPENING_DATA_ENCRYPTION_KEY
          val mesosClient =
            EncryptedStorage.buildEncryptedMesosStorageClient(
              impressionsStorageClient,
              kmsClient,
              kekUri,
              encryptedDek,
            )

          val blobKey = impressionsBlobUri.key
          logger.info("  Decrypting from blob key: $blobKey")

          stage = VerificationStage.READING_IMPRESSIONS
          val impressionsBlob =
            mesosClient.getBlob(blobKey)
              ?: throw IllegalStateException("Impression blob not found: $blobKey")
          var blobImpressionCount = 0L
          val blobImpressionsByEntityKey = mutableMapOf<EntityKey, Long>()

          impressionsBlob.read().collect { record ->
            val index = blobImpressionCount
            stage = VerificationStage.PARSING_IMPRESSION
            val impression = LabeledImpression.parseFrom(record)
            stage = VerificationStage.VALIDATING_IMPRESSION
            check(impression.vid > 0) { "Invalid VID: ${impression.vid}" }
            check(impression.hasEvent()) { "Missing event in impression $index" }
            check(impression.hasEventTime()) { "Missing event time in impression $index" }
            Timestamps.checkValid(impression.eventTime)
            check(
              Timestamps.compare(impression.eventTime, blobDetails.interval.startTime) >= 0 &&
                Timestamps.compare(impression.eventTime, blobDetails.interval.endTime) < 0
            ) {
              "Event time on impression $index is outside BlobDetails.interval"
            }
            check(impression.event.typeUrl.isNotEmpty()) {
              "Missing event type URL in impression $index"
            }
            if (expectedEntityKeys.isNotEmpty()) {
              check(impression.entityKeysCount > 0) {
                "Impression $index must have at least one entity key"
              }
            }
            val impressionEntityKeys = mutableSetOf<EntityKey>()
            for ((entityKeyIndex, entityKey) in impression.entityKeysList.withIndex()) {
              check(entityKey.entityType.isNotEmpty()) {
                "EntityKey[$entityKeyIndex] on impression $index has empty entity_type"
              }
              check(entityKey.entityId.isNotEmpty()) {
                "EntityKey[$entityKeyIndex] on impression $index has empty entity_id"
              }
              check(URL_SAFE_STRING.matches(entityKey.entityType)) {
                "EntityKey[$entityKeyIndex] on impression $index has non-URL-safe entity_type"
              }
              check(URL_SAFE_STRING.matches(entityKey.entityId)) {
                "EntityKey[$entityKeyIndex] on impression $index has non-URL-safe entity_id"
              }
              impressionEntityKeys += EntityKey(entityKey.entityType, entityKey.entityId)
            }
            for (entityKey in impressionEntityKeys) {
              blobImpressionsByEntityKey.merge(entityKey, 1L, Long::plus)
            }

            if (expectedEventTypeUrl.isNotEmpty()) {
              check(impression.event.typeUrl == expectedEventTypeUrl) {
                "Event type URL mismatch on impression $index: " +
                  "expected $expectedEventTypeUrl, got ${impression.event.typeUrl}"
              }
              stage = VerificationStage.PARSING_EVENT
              checkNotNull(eventMessageInstance)
                .newBuilderForType()
                .mergeFrom(impression.event.value)
                .build()
            }

            if (index < 3) {
              val entityKeysSummary =
                impression.entityKeysList.joinToString(", ") { "${it.entityType}=${it.entityId}" }
              logger.info(
                "  Record[$index]: vid=${impression.vid}, " +
                  "eventTime=${impression.eventTime}, entityKeys=[$entityKeysSummary]"
              )
            }
            blobImpressionCount++
            stage = VerificationStage.READING_IMPRESSIONS
          }
          logger.info("  Decrypted $blobImpressionCount impression records")
          stage = VerificationStage.VALIDATING_BLOB
          check(blobImpressionCount > 0L) { "Impression blob is empty" }

          if (expectedEntityKeys.isNotEmpty()) {
            check(blobImpressionsByEntityKey.keys == expectedEntityKeys) {
              "BlobDetails.entity_keys does not match the entity keys in the impression blob"
            }
          }

          totalImpressions += blobImpressionCount
          totalBlobsProcessed++
          if (blobDetails.eventGroupReferenceId.isNotEmpty()) {
            impressionsByEventGroupReferenceId.merge(
              blobDetails.eventGroupReferenceId,
              blobImpressionCount,
              Long::plus,
            )
          }
          for ((entityKey, count) in blobImpressionsByEntityKey) {
            impressionsByEntityKey.merge(entityKey, count, Long::plus)
          }
          logger.info("  PASS: $metadataUri - $blobImpressionCount impressions verified")
        } catch (e: CancellationException) {
          throw e
        } catch (e: Exception) {
          errors++
          logger.log(
            Level.SEVERE,
            "  FAIL: $metadataUri while ${stage.description}: ${e.message}",
            e,
          )
        }
      }

      return VerificationResult(
        totalImpressions = totalImpressions,
        totalBlobsProcessed = totalBlobsProcessed,
        errors = errors,
        impressionsByEventGroupReferenceId = impressionsByEventGroupReferenceId.toMap(),
        impressionsByEntityKey = impressionsByEntityKey.toMap(),
      )
    }
  }
}

fun main(args: Array<String>) {
  val exitCode = CommandLine(VerifyVidLabeledImpressions()).execute(*args)
  if (exitCode != 0) {
    exitProcess(exitCode)
  }
}
