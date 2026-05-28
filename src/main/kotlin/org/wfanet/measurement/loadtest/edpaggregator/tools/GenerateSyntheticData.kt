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

import com.google.crypto.tink.Aead
import com.google.crypto.tink.InsecureSecretKeyAccess
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.Message
import com.google.protobuf.TypeRegistry
import java.io.File
import java.nio.file.Paths
import java.time.LocalDate
import java.time.ZoneId
import java.util.SortedMap
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.aws.kms.AwsKmsClientFactory
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.tink.AwsWebIdentityCredentials
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.loadtest.dataprovider.EntityKey
import org.wfanet.measurement.loadtest.dataprovider.EntityKeyedLabeledEventDateShard
import org.wfanet.measurement.loadtest.dataprovider.EntityKeysWithLabeledEvents
import org.wfanet.measurement.loadtest.dataprovider.LabeledEventDateShard
import org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration
import org.wfanet.measurement.loadtest.edpaggregator.testing.ImpressionsWriter
import picocli.CommandLine.Command
import picocli.CommandLine.Option

enum class KmsType {
  FAKE,
  GCP,
  AWS,
  GCP_TO_AWS,
}

@Command(
  name = "generate-synthetic-data",
  description = ["Generates synthetic data for Panel Match."],
)
class GenerateSyntheticData : Runnable {
  @Option(
    names = ["--kms-type"],
    description = ["Type of kms: \${COMPLETION-CANDIDATES}"],
    required = true,
  )
  lateinit var kmsType: KmsType
    private set

  @Option(
    names = ["--local-storage-path"],
    description = ["Optional. Path to local storage used when schema is file:///"],
    required = false,
  )
  private var storagePath: File? = null

  @Option(
    names = ["--model-line"],
    description = ["The full model line resource name for this campaign."],
    required = true,
  )
  lateinit var modelLine: String
    private set

  @Option(
    names = ["--kek-uri"],
    description = ["The KMS kek uri."],
    required = true,
    defaultValue = DEFAULT_KEK_URI,
  )
  lateinit var kekUri: String
    private set

  @Option(
    names = ["--fake-kek-keyset-file"],
    description =
      [
        "Optional. Path to a Tink keyset file that backs the FAKE KEK identified by --kek-uri. " +
          "Only valid when --kms-type=FAKE. When set, on first run the generated fake KEK " +
          "keyset is written here so that VerifySyntheticData (in a separate process) can load " +
          "the same keyset and decrypt the impressions. When the file already exists it is reused."
      ],
    required = false,
  )
  private var fakeKekKeysetFile: File? = null

  @Option(
    names = ["--output-bucket"],
    description = ["The bucket where to write the metadata and impressions."],
    required = true,
  )
  lateinit var outputBucket: String
    private set

  @Option(
    names = ["--schema"],
    description =
      [
        "The schema to write to. Supported options are gs:// and file:///. Used by a SelectedStorageClient to build the proper storage client to write output to."
      ],
    required = true,
    defaultValue = "file:///",
  )
  lateinit var schema: String
    private set

  @Option(
    names = ["--zone-id"],
    description = ["The Zone ID by which to generate the events"],
    required = true,
    defaultValue = "UTC",
  )
  lateinit var zoneId: String
    private set

  @Option(
    names = ["--event-message-type-url"],
    description =
      [
        "Type URL (e.g. type.googleapis.com/<full_name>) of the event message type that the " +
          "generated impressions carry. Defaults to TestEvent.",
        "When set to a type other than TestEvent, --event-message-descriptor-set must be supplied.",
      ],
    required = false,
    defaultValue = DEFAULT_EVENT_MESSAGE_TYPE_URL,
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

  @Option(
    names = ["--impression-metadata-base-path"],
    description = ["Base path where to store the Impressions files"],
    required = false,
  )
  var impressionMetadataBasePath: String? = null
    private set

  @Option(
    names = ["--flat-output-base-path"],
    description =
      [
        "Optional. When set, outputs files directly under <base-path>/<date>/ without model-line and event-group-reference-id path segments."
      ],
    required = false,
  )
  var flatOutputBasePath: String? = null
    private set

  @Option(
    names = ["--config-file"],
    description =
      [
        "Path to a ImpressionTestDataConfig textproto file that defines the event group specs " +
          "to generate for the EDP specified by --edp-name."
      ],
    required = true,
  )
  private lateinit var configFile: File

  @Option(
    names = ["--edp-name"],
    description =
      [
        "EDP short name (e.g. edp7, edpa_meta). Selects which event groups from the " +
          "--config-file to generate."
      ],
    required = false,
  )
  var edpName: String? = null
    private set

  @Option(
    names = ["--aws-role-arn"],
    description =
      ["AWS IAM role ARN for STS AssumeRoleWithWebIdentity. Required when --kms-type=AWS."],
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
    description = ["AWS STS role session name. Required when --kms-type=AWS."],
    required = false,
    defaultValue = "generate-synthetic-data",
  )
  lateinit var awsRoleSessionName: String
    private set

  @Option(
    names = ["--aws-region"],
    description = ["AWS region for STS and KMS. Required when --kms-type=AWS."],
    required = false,
    defaultValue = "",
  )
  lateinit var awsRegion: String
    private set

  @kotlin.io.path.ExperimentalPathApi
  override fun run() {
    require(flatOutputBasePath == null || impressionMetadataBasePath == null) {
      "Cannot specify both --impression-metadata-base-path and --flat-output-base-path; set exactly one or neither"
    }
    require(kmsType == KmsType.FAKE || fakeKekKeysetFile == null) {
      "--fake-kek-keyset-file is only valid when --kms-type=FAKE"
    }

    val config: ImpressionTestDataConfig =
      parseTextProto(configFile, ImpressionTestDataConfig.getDefaultInstance())
    val eventGroupSpecs: List<ResolvedEventGroupSpec> = buildSpecsFromConfig(config, edpName)

    if (flatOutputBasePath != null) {
      val outputKeys = eventGroupSpecs.map { it.outputKey }
      require(outputKeys.toSet().size == outputKeys.size) {
        "output_key values must be unique when using --flat-output-base-path: $outputKeys"
      }
    }
    val eventGroupReferenceIds = eventGroupSpecs.map { it.eventGroupReferenceId }
    require(eventGroupReferenceIds.toSet().size == eventGroupReferenceIds.size) {
      "event-group-reference-id values must be unique: $eventGroupReferenceIds"
    }

    val resolvedPopulationSpecPath = config.populationSpecResourcePath

    val eventMessageInstance: Message =
      resolveEventMessageInstance(eventMessageTypeUrl, eventMessageDescriptorSetFiles)

    val populationSpec: PopulationSpec =
      parseTextProto(
        resolveSpecFile(resolvedPopulationSpecPath),
        PopulationSpec.getDefaultInstance(),
        buildEventMessageTypeRegistry(eventMessageInstance),
      )
    val kmsClient: KmsClient =
      when (kmsType) {
        KmsType.FAKE -> buildFakeKmsClient(kekUri, fakeKekKeysetFile)
        KmsType.GCP -> GcpKmsClient().withDefaultCredentials()
        KmsType.AWS -> {
          require(awsRoleArn.isNotEmpty()) { "--aws-role-arn is required when --kms-type=AWS" }
          require(awsWebIdentityTokenFile.isNotEmpty()) {
            "--aws-web-identity-token-file is required when --kms-type=AWS"
          }
          require(awsRegion.isNotEmpty()) { "--aws-region is required when --kms-type=AWS" }
          AwsKmsClientFactory()
            .getKmsClient(
              AwsWebIdentityCredentials(
                roleArn = awsRoleArn,
                webIdentityTokenFilePath = awsWebIdentityTokenFile,
                roleSessionName = awsRoleSessionName,
                region = awsRegion,
              )
            )
        }
        KmsType.GCP_TO_AWS ->
          throw UnsupportedOperationException(
            "GCP_TO_AWS is not yet supported in GenerateSyntheticData. Use VerifySyntheticData."
          )
      }
    val modelLineName = ModelLineKey.fromName(modelLine)?.modelLineId

    runBlocking {
      for (spec in eventGroupSpecs) {
        val perSubSpecShards: List<Sequence<LabeledEventDateShard<Message>>> =
          spec.subSpecs.map { subSpec ->
            val syntheticEventGroupSpec: SyntheticEventGroupSpec =
              parseTextProto(
                resolveSpecFile(subSpec.dataSpecResourcePath),
                SyntheticEventGroupSpec.getDefaultInstance(),
              )
            SyntheticDataGeneration.generateEvents(
              messageInstance = eventMessageInstance,
              populationSpec = populationSpec,
              syntheticEventGroupSpec = syntheticEventGroupSpec,
              zoneId = ZoneId.of(zoneId),
            )
          }
        val coalescedShards: Sequence<EntityKeyedLabeledEventDateShard<Message>> =
          coalesceByDate(perSubSpecShards, spec.subSpecs.map { listOf(it.entityKey) })
        val eventGroupPath =
          "model-line/$modelLineName/event-group-reference-id/${spec.eventGroupReferenceId}"
        val impressionWriter =
          ImpressionsWriter(
            spec.eventGroupReferenceId,
            eventGroupPath,
            kekUri,
            kmsClient,
            outputBucket,
            outputBucket,
            storagePath,
            schema,
            spec.outputKey,
          )
        impressionWriter.writeLabeledImpressionData(
          coalescedShards,
          modelLine,
          impressionsBasePath = impressionMetadataBasePath,
          flatOutputBasePath = flatOutputBasePath,
        )
      }
    }
  }

  /**
   * Coalesces per-sub-spec date shard sequences into a single per-event-group sequence. Each output
   * shard contains one [EntityKeysWithLabeledEvents] group per sub-spec that emitted events for
   * that date, so a single impressions blob can hold impressions with different entity keys.
   */
  private fun coalesceByDate(
    perSubSpecShards: List<Sequence<LabeledEventDateShard<Message>>>,
    perSubSpecEntityKeys: List<List<EntityKey>>,
  ): Sequence<EntityKeyedLabeledEventDateShard<Message>> {
    require(perSubSpecShards.size == perSubSpecEntityKeys.size)
    // Materialize per-sub-spec, per-date groups so we can join across sub-specs by date.
    val groupsByDate: SortedMap<LocalDate, MutableList<EntityKeysWithLabeledEvents<Message>>> =
      sortedMapOf()
    for ((index, shards) in perSubSpecShards.withIndex()) {
      val entityKeys = perSubSpecEntityKeys[index]
      for (shard in shards) {
        val materializedEvents = shard.labeledEvents.toList()
        if (materializedEvents.isEmpty()) continue
        groupsByDate
          .getOrPut(shard.localDate) { mutableListOf() }
          .add(EntityKeysWithLabeledEvents(entityKeys, materializedEvents.asSequence()))
      }
    }
    return groupsByDate.asSequence().map { (date, groups) ->
      EntityKeyedLabeledEventDateShard(date, groups.asSequence())
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val DEFAULT_KEK_URI = FakeKmsClient.KEY_URI_PREFIX + "key1"

    /** Type URL of the default event message ([TestEvent]). */
    const val DEFAULT_EVENT_MESSAGE_TYPE_URL: String =
      ProtoReflection.DEFAULT_TYPE_URL_PREFIX +
        "/wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"

    // This is the relative location from which population and data spec textprotos are read.
    private val TEST_DATA_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "proto",
        "wfa",
        "measurement",
        "loadtest",
        "dataprovider",
      )
    private val TEST_DATA_RUNTIME_PATH = getRuntimePath(TEST_DATA_PATH)!!

    private fun resolveSpecFile(path: String): File {
      val specPath = Paths.get(path)
      return if (specPath.isAbsolute) specPath.toFile()
      else TEST_DATA_RUNTIME_PATH.resolve(path).toFile()
    }

    private val EVENT_MESSAGE_EXTENSION_REGISTRY: ExtensionRegistry =
      ExtensionRegistry.newInstance()
        .also { EventAnnotationsProto.registerAllExtensions(it) }
        .unmodifiable

    /**
     * [Descriptors.FileDescriptor]s of protobuf types known at compile time that may be loaded from
     * a [DescriptorProtos.FileDescriptorSet] without being included in the set's contents.
     */
    private val COMPILED_PROTOBUF_TYPES: Iterable<Descriptors.FileDescriptor> =
      (ProtoReflection.WELL_KNOWN_TYPES.asSequence() +
          EventAnnotationsProto.getDescriptor() +
          TestEvent.getDescriptor().file)
        .asIterable()

    init {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }

    /**
     * Resolves the event message [Message] instance for the supplied [eventMessageTypeUrl].
     *
     * When [eventMessageTypeUrl] matches [DEFAULT_EVENT_MESSAGE_TYPE_URL], returns the compiled
     * [TestEvent] default instance directly. Otherwise builds a [DynamicMessage] from the supplied
     * [descriptorSetFiles].
     */
    fun resolveEventMessageInstance(
      eventMessageTypeUrl: String,
      descriptorSetFiles: Collection<File>,
    ): Message {
      if (eventMessageTypeUrl == DEFAULT_EVENT_MESSAGE_TYPE_URL) {
        return TestEvent.getDefaultInstance()
      }
      require(descriptorSetFiles.isNotEmpty()) {
        "--event-message-descriptor-set is required when --event-message-type-url is not " +
          "$DEFAULT_EVENT_MESSAGE_TYPE_URL"
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
      val descriptor: Descriptors.Descriptor =
        descriptors.firstOrNull { it.fullName == expectedFullName }
          ?: error(
            "Event message type $expectedFullName not found in supplied descriptor sets " +
              "(${descriptorSetFiles.joinToString(", ")})"
          )
      return DynamicMessage.getDefaultInstance(descriptor)
    }

    /**
     * Builds a [TypeRegistry] containing the descriptor of [eventMessageInstance] plus the
     * descriptors of all of its message-type template fields, so that textproto parsing can resolve
     * `Any`-typed population attributes via the `[type.googleapis.com/<full_name>] { ... }` syntax.
     */
    fun buildEventMessageTypeRegistry(eventMessageInstance: Message): TypeRegistry {
      val descriptors: Set<Descriptors.Descriptor> = buildSet {
        add(eventMessageInstance.descriptorForType)
        for (field in eventMessageInstance.descriptorForType.fields) {
          if (field.type == Descriptors.FieldDescriptor.Type.MESSAGE) {
            add(field.messageType)
          }
        }
      }
      return TypeRegistry.newBuilder().add(descriptors).build()
    }

    /**
     * Builds a [FakeKmsClient] registered to serve [kekUri] from a fake KEK keyset persisted at
     * [fakeKekKeysetFile].
     *
     * `FakeKmsClient` is in-memory only, so to round-trip envelope-encrypted data across processes
     * the underlying fake KEK keyset must itself be persisted somewhere. Behavior:
     * * `fakeKekKeysetFile == null` — generate a fresh fake KEK keyset in memory (process-local;
     *   cannot be decrypted by a later process).
     * * `fakeKekKeysetFile` exists — load and reuse the fake KEK keyset from disk.
     * * `fakeKekKeysetFile` does not exist — generate a fresh fake KEK keyset and write it to disk
     *   so it can be reloaded later.
     *
     * The keyset is serialized in cleartext via [TinkProtoKeysetFormat], which is appropriate
     * because [FakeKmsClient] itself provides no protection — this is a testing-only KMS.
     */
    fun buildFakeKmsClient(kekUri: String, fakeKekKeysetFile: File?): FakeKmsClient {
      val fakeKekKeysetHandle: KeysetHandle =
        if (fakeKekKeysetFile != null && fakeKekKeysetFile.exists()) {
          TinkProtoKeysetFormat.parseKeyset(
            fakeKekKeysetFile.readBytes(),
            InsecureSecretKeyAccess.get(),
          )
        } else {
          val newHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
          if (fakeKekKeysetFile != null) {
            fakeKekKeysetFile.parentFile?.mkdirs()
            fakeKekKeysetFile.writeBytes(
              TinkProtoKeysetFormat.serializeKeyset(newHandle, InsecureSecretKeyAccess.get())
            )
          }
          newHandle
        }
      return FakeKmsClient().apply {
        setAead(kekUri, fakeKekKeysetHandle.getPrimitive(Aead::class.java))
      }
    }

    fun buildSpecsFromConfig(
      config: ImpressionTestDataConfig,
      edpName: String? = null,
    ): List<ResolvedEventGroupSpec> {
      val edpEventGroups =
        if (edpName != null) {
          val filtered = config.eventGroupsList.filter { it.edpName == edpName }
          require(filtered.isNotEmpty()) { "No event groups found for EDP '$edpName' in config" }
          filtered
        } else {
          config.eventGroupsList
        }
      return edpEventGroups.flatMap { eg ->
        if (eg.entityKeySpecsList.isEmpty()) {
          listOf(
            ResolvedEventGroupSpec(
              eventGroupReferenceId = eg.eventGroupReferenceId,
              outputKey = eg.outputKey,
              subSpecs =
                listOf(
                  ResolvedSubSpec(
                    dataSpecResourcePath = eg.dataSpecResourcePath,
                    entityKey = EntityKey("campaign", eg.eventGroupReferenceId),
                  )
                ),
            )
          )
        } else {
          listOf(
            ResolvedEventGroupSpec(
              eventGroupReferenceId = eg.eventGroupReferenceId,
              outputKey = eg.outputKey,
              subSpecs =
                eg.entityKeySpecsList.map { eks ->
                  ResolvedSubSpec(
                    dataSpecResourcePath = eks.dataSpecResourcePath,
                    entityKey = EntityKey(eks.entityType, eks.entityId),
                  )
                },
            )
          )
        }
      }
    }
  }
}

data class ResolvedEventGroupSpec(
  val eventGroupReferenceId: String,
  val outputKey: String,
  val subSpecs: List<ResolvedSubSpec>,
)

data class ResolvedSubSpec(val dataSpecResourcePath: String, val entityKey: EntityKey)

fun main(args: Array<String>) = commandLineMain(GenerateSyntheticData(), args)
