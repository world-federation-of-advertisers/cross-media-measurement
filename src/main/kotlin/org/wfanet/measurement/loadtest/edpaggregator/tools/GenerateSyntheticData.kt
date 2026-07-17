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
import java.time.ZoneId
import java.util.logging.Logger
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.runBlocking
import org.measurement.integration.k8s.testing.ImpressionTestDataConfig
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.aws.kms.AwsKmsClientFactory
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.tink.AwsWebIdentityCredentials
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.integration.common.ImpressionTestDataConfigs
import org.wfanet.measurement.loadtest.dataprovider.EntityKey
import org.wfanet.measurement.loadtest.dataprovider.EntityKeyedLabeledEventDateShard
import org.wfanet.measurement.loadtest.dataprovider.EntityKeysWithLabeledEvents
import org.wfanet.measurement.loadtest.dataprovider.LabeledEventDateShard
import org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration
import org.wfanet.measurement.loadtest.edpaggregator.testing.ImpressionsWriter
import org.wfanet.measurement.storage.SelectedStorageClient
import picocli.CommandLine.ArgGroup
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
  class EdpKmsConfig {
    @Option(
      names = ["--edp-name"],
      description =
        [
          "EDP short name (e.g. edp7, edpa_meta). Selects which event groups from the " +
            "--config-file to generate."
        ],
      required = true,
    )
    lateinit var edpName: String
      private set

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
          "Optional. Path to a Tink keyset file that backs the FAKE KEK identified by --kek-uri. " +
            "Only valid when --kms-type=FAKE. When set, on first run the generated fake KEK " +
            "keyset is written here so that VerifySyntheticData (in a separate process) can load " +
            "the same keyset and decrypt the impressions. When the file already exists it is reused."
        ],
      required = false,
    )
    var fakeKekKeysetFile: File? = null
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
  }

  @ArgGroup(exclusive = false, multiplicity = "1..*") lateinit var edpKmsConfigs: List<EdpKmsConfig>

  @Option(
    names = ["--local-storage-path"],
    description = ["Optional. Path to local storage used when scheme is file:///"],
    required = false,
  )
  private var storagePath: File? = null

  @Option(
    names = ["--output-bucket"],
    description = ["The bucket where to write the metadata and impressions."],
    required = true,
  )
  lateinit var outputBucket: String
    private set

  @Option(
    names = ["--scheme"],
    description =
      [
        "Storage URI scheme to write to. Supported values are gs:// and file:///. Used by a SelectedStorageClient to build the proper storage client to write output to."
      ],
    required = true,
    defaultValue = "file:///",
  )
  lateinit var scheme: String
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
    names = ["--config-file"],
    description =
      [
        "Path to a ImpressionTestDataConfig textproto file that defines the event group specs " +
          "to generate."
      ],
    required = true,
  )
  private lateinit var configFile: File

  @Option(
    names = ["--create-done-blobs"],
    description = ["Write an empty done blob in each date directory after generation."],
    required = false,
    defaultValue = "false",
  )
  var createDoneBlobs: Boolean = false
    private set

  @Option(
    names = ["--model-line"],
    description = ["Full ModelLine resource name."],
    required = true,
  )
  lateinit var modelLine: String
    private set

  @kotlin.io.path.ExperimentalPathApi
  override fun run() {
    val edpKmsConfigsByName: Map<String, EdpKmsConfig> = edpKmsConfigs.associateBy { it.edpName }

    for (edpKmsConfig in edpKmsConfigs) {
      require(edpKmsConfig.kmsType == KmsType.FAKE || edpKmsConfig.fakeKekKeysetFile == null) {
        "--fake-kek-keyset-file is only valid when --kms-type=FAKE (edp: ${edpKmsConfig.edpName})"
      }
    }

    val config: ImpressionTestDataConfig =
      parseTextProto(configFile, ImpressionTestDataConfig.getDefaultInstance())

    val edpNames = edpKmsConfigsByName.keys
    val eventGroupSpecs: List<ResolvedEventGroupSpec> = buildSpecsFromConfig(config, edpNames)

    val eventGroupReferenceIds = eventGroupSpecs.map { it.eventGroupReferenceId }
    require(eventGroupReferenceIds.toSet().size == eventGroupReferenceIds.size) {
      "event-group-reference-id values must be unique: $eventGroupReferenceIds"
    }

    val resolvedPopulationSpecPath = config.populationSpecResourcePath

    val eventMessageInstance: Message =
      resolveEventMessageInstance(eventMessageTypeUrl, eventMessageDescriptorSetFiles)

    val populationSpec: PopulationSpec =
      parseTextProto(
        ImpressionTestDataConfigs.resolveSpecPath(resolvedPopulationSpecPath),
        PopulationSpec.getDefaultInstance(),
        buildEventMessageTypeRegistry(eventMessageInstance),
      )

    val writtenDatePaths = mutableSetOf<String>()

    runBlocking {
      for (spec in eventGroupSpecs) {
        val edpKmsConfig = edpKmsConfigsByName.getValue(spec.edpName)
        val kmsClient: KmsClient = buildKmsClient(edpKmsConfig)

        val perSubSpecShards: List<Sequence<LabeledEventDateShard<Message>>> =
          spec.subSpecs.map { subSpec ->
            val syntheticEventGroupSpec: SyntheticEventGroupSpec =
              parseTextProto(
                ImpressionTestDataConfigs.resolveSpecPath(subSpec.dataSpecResourcePath),
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
        val trackingShards =
          coalescedShards.map { (date, groups) ->
            writtenDatePaths.add("${spec.outputBasePath}/$date")
            EntityKeyedLabeledEventDateShard(date, groups)
          }
        val impressionWriter =
          ImpressionsWriter(
            spec.eventGroupReferenceId,
            "",
            edpKmsConfig.kekUri,
            kmsClient,
            outputBucket,
            outputBucket,
            storagePath,
            scheme,
            spec.outputKey,
          )
        impressionWriter.writeLabeledImpressionData(
          trackingShards,
          modelLine,
          flatOutputBasePath = spec.outputBasePath,
        )
      }

      if (createDoneBlobs) {
        for (datePath in writtenDatePaths) {
          val doneKey = "$datePath/done"
          val doneUri = "$scheme$outputBucket/$doneKey"
          val storageClient = SelectedStorageClient(doneUri, storagePath)
          logger.info("Writing done blob: $doneKey")
          storageClient.writeBlob(doneKey, emptyFlow())
        }
      }
    }
  }

  /**
   * Coalesces per-sub-spec date shard sequences into a single per-event-group sequence using a
   * streaming k-way merge. Each output shard contains one [EntityKeysWithLabeledEvents] group per
   * sub-spec that emitted events for that date. Holds at most one shard per sub-spec in memory at a
   * time, avoiding the O(total_events) materialization of the previous implementation.
   */
  private fun coalesceByDate(
    perSubSpecShards: List<Sequence<LabeledEventDateShard<Message>>>,
    perSubSpecEntityKeys: List<List<EntityKey>>,
  ): Sequence<EntityKeyedLabeledEventDateShard<Message>> = sequence {
    require(perSubSpecShards.size == perSubSpecEntityKeys.size)

    val iterators = perSubSpecShards.map { it.iterator() }
    val buffers = arrayOfNulls<LabeledEventDateShard<Message>>(iterators.size)
    for (i in iterators.indices) {
      if (iterators[i].hasNext()) buffers[i] = iterators[i].next()
    }

    while (buffers.any { it != null }) {
      val minDate = buffers.filterNotNull().minOf { it.localDate }

      val groups = mutableListOf<EntityKeysWithLabeledEvents<Message>>()
      for (i in buffers.indices) {
        val shard = buffers[i] ?: continue
        if (shard.localDate == minDate) {
          groups.add(EntityKeysWithLabeledEvents(perSubSpecEntityKeys[i], shard.labeledEvents))
        }
      }

      yield(EntityKeyedLabeledEventDateShard(minDate, groups.asSequence()))

      for (i in buffers.indices) {
        if (buffers[i]?.localDate == minDate) {
          buffers[i] = if (iterators[i].hasNext()) iterators[i].next() else null
        }
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val DEFAULT_KEK_URI = FakeKmsClient.KEY_URI_PREFIX + "key1"

    /** Type URL of the default event message ([TestEvent]). */
    const val DEFAULT_EVENT_MESSAGE_TYPE_URL: String =
      ProtoReflection.DEFAULT_TYPE_URL_PREFIX +
        "/wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"

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
     * * `fakeKekKeysetFile == null` -- generate a fresh fake KEK keyset in memory (process-local;
     *   cannot be decrypted by a later process).
     * * `fakeKekKeysetFile` exists -- load and reuse the fake KEK keyset from disk.
     * * `fakeKekKeysetFile` does not exist -- generate a fresh fake KEK keyset and write it to disk
     *   so it can be reloaded later.
     *
     * The keyset is serialized in cleartext via [TinkProtoKeysetFormat], which is appropriate
     * because [FakeKmsClient] itself provides no protection -- this is a testing-only KMS.
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

    fun buildKmsClient(edpKmsConfig: EdpKmsConfig): KmsClient {
      return when (edpKmsConfig.kmsType) {
        KmsType.FAKE -> buildFakeKmsClient(edpKmsConfig.kekUri, edpKmsConfig.fakeKekKeysetFile)
        KmsType.GCP -> GcpKmsClient().withDefaultCredentials()
        KmsType.AWS -> {
          require(edpKmsConfig.awsRoleArn.isNotEmpty()) {
            "--aws-role-arn is required when --kms-type=AWS (edp: ${edpKmsConfig.edpName})"
          }
          require(edpKmsConfig.awsWebIdentityTokenFile.isNotEmpty()) {
            "--aws-web-identity-token-file is required when --kms-type=AWS (edp: ${edpKmsConfig.edpName})"
          }
          require(edpKmsConfig.awsRegion.isNotEmpty()) {
            "--aws-region is required when --kms-type=AWS (edp: ${edpKmsConfig.edpName})"
          }
          AwsKmsClientFactory()
            .getKmsClient(
              AwsWebIdentityCredentials(
                roleArn = edpKmsConfig.awsRoleArn,
                webIdentityTokenFilePath = edpKmsConfig.awsWebIdentityTokenFile,
                roleSessionName = edpKmsConfig.awsRoleSessionName,
                region = edpKmsConfig.awsRegion,
              )
            )
        }
        KmsType.GCP_TO_AWS ->
          throw UnsupportedOperationException(
            "GCP_TO_AWS is not yet supported in GenerateSyntheticData. Use VerifySyntheticData."
          )
      }
    }

    fun buildSpecsFromConfig(
      config: ImpressionTestDataConfig,
      edpNames: Set<String>,
    ): List<ResolvedEventGroupSpec> {
      val edpEventGroups = config.eventGroupsList.filter { it.edpName in edpNames }
      require(edpEventGroups.isNotEmpty()) { "No event groups found for EDPs $edpNames in config" }
      return edpEventGroups.flatMap { eg ->
        if (eg.entityKeySpecsList.isEmpty()) {
          listOf(
            ResolvedEventGroupSpec(
              eventGroupReferenceId = eg.eventGroupReferenceId,
              outputKey = eg.outputKey,
              outputBasePath = eg.outputBasePath,
              edpName = eg.edpName,
              subSpecs =
                listOf(
                  ResolvedEntityKeySpec(
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
              outputBasePath = eg.outputBasePath,
              edpName = eg.edpName,
              subSpecs =
                eg.entityKeySpecsList.map { eks ->
                  ResolvedEntityKeySpec(
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
  val outputBasePath: String,
  val edpName: String,
  val subSpecs: List<ResolvedEntityKeySpec>,
)

data class ResolvedEntityKeySpec(val dataSpecResourcePath: String, val entityKey: EntityKey)

fun main(args: Array<String>) = commandLineMain(GenerateSyntheticData(), args)
