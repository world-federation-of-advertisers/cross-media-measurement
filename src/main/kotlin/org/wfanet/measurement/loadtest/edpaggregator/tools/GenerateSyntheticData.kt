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
import java.io.File
import java.nio.file.Paths
import java.time.LocalDate
import java.time.ZoneId
import java.util.SortedMap
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.aws.kms.AwsKmsClientFactory
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
    names = ["--population-spec-resource-path"],
    description = ["The path to the resource of the population-spec. Must be textproto format."],
    required = true,
  )
  lateinit var populationSpecResourcePath: String
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

  @ArgGroup(
    exclusive = false,
    multiplicity = "1..*",
    heading =
      "Per-event-group flags. Repeat the group to generate multiple event groups in a single " +
        "invocation; each event group has its own data-spec textproto, event group reference id, " +
        "and EntityKeys.%n",
  )
  private lateinit var eventGroupSpecFlags: List<EventGroupSpecFlags>

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
    require(flatOutputBasePath == null || eventGroupSpecFlags.size == 1) {
      "--flat-output-base-path is incompatible with multiple event group specs (output paths would collide)"
    }
    val eventGroupReferenceIds = eventGroupSpecFlags.map { it.eventGroupReferenceId }
    require(eventGroupReferenceIds.toSet().size == eventGroupReferenceIds.size) {
      "--event-group-reference-id values must be unique across event group specs: $eventGroupReferenceIds"
    }
    require(kmsType == KmsType.FAKE || fakeKekKeysetFile == null) {
      "--fake-kek-keyset-file is only valid when --kms-type=FAKE"
    }

    val syntheticPopulationSpec: SyntheticPopulationSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve(populationSpecResourcePath).toFile(),
        SyntheticPopulationSpec.getDefaultInstance(),
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
      for (specFlags in eventGroupSpecFlags) {
        // TODO(world-federation-of-advertisers/cross-media-measurement#2360): Consider supporting
        // other event types.
        val perSubSpecShards: List<Sequence<LabeledEventDateShard<TestEvent>>> =
          specFlags.subSpecFlags.map { subSpec ->
            val syntheticEventGroupSpec: SyntheticEventGroupSpec =
              parseTextProto(
                TEST_DATA_RUNTIME_PATH.resolve(subSpec.dataSpecResourcePath).toFile(),
                SyntheticEventGroupSpec.getDefaultInstance(),
              )
            SyntheticDataGeneration.generateEvents(
              messageInstance = TestEvent.getDefaultInstance(),
              populationSpec = syntheticPopulationSpec,
              syntheticEventGroupSpec = syntheticEventGroupSpec,
              zoneId = ZoneId.of(zoneId),
            )
          }
        val perSubSpecEntityKeys: List<List<EntityKey>> =
          specFlags.subSpecFlags.map { subSpec ->
            subSpec.entityKeyFlags.map { EntityKey(it.entityType, it.entityId) }
          }
        val coalescedShards: Sequence<EntityKeyedLabeledEventDateShard<TestEvent>> =
          coalesceByDate(perSubSpecShards, perSubSpecEntityKeys)
        val eventGroupPath =
          "model-line/$modelLineName/event-group-reference-id/${specFlags.eventGroupReferenceId}"
        val impressionWriter =
          ImpressionsWriter(
            specFlags.eventGroupReferenceId,
            eventGroupPath,
            kekUri,
            kmsClient,
            outputBucket,
            outputBucket,
            storagePath,
            schema,
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
    perSubSpecShards: List<Sequence<LabeledEventDateShard<TestEvent>>>,
    perSubSpecEntityKeys: List<List<EntityKey>>,
  ): Sequence<EntityKeyedLabeledEventDateShard<TestEvent>> {
    require(perSubSpecShards.size == perSubSpecEntityKeys.size)
    // Materialize per-sub-spec, per-date groups so we can join across sub-specs by date.
    val groupsByDate: SortedMap<LocalDate, MutableList<EntityKeysWithLabeledEvents<TestEvent>>> =
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

    init {
      AeadConfig.register()
      StreamingAeadConfig.register()
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
  }
}

/**
 * Flags describing a single synthetic event group to generate. An event group is identified by its
 * [eventGroupReferenceId] and produces one impressions blob per date. Each blob is populated by one
 * or more [EntityKeysSubSpecFlags]: each sub-spec contributes its own data-spec textproto and its
 * own EntityKeys, allowing different impressions in the same blob to carry different EntityKeys.
 */
private class EventGroupSpecFlags {
  @Option(
    names = ["--event-group-reference-id"],
    description = ["The EDP-generated event group reference id for this synthetic event group."],
    required = true,
  )
  lateinit var eventGroupReferenceId: String
    private set

  @ArgGroup(
    exclusive = false,
    multiplicity = "1..*",
    heading =
      "Sub-spec flags for this event group. Each sub-spec contributes its own data-spec textproto " +
        "and EntityKeys; impressions from all sub-specs land in the same per-date impressions " +
        "blob, with each impression stamped with its sub-spec's EntityKeys.%n",
  )
  lateinit var subSpecFlags: List<EntityKeysSubSpecFlags>
    private set
}

/**
 * One sub-spec contribution to an event group's per-date impressions blob: a data-spec textproto
 * and a list of [LabeledImpression.EntityKey]s that should be stamped on every impression generated
 * from this sub-spec.
 */
private class EntityKeysSubSpecFlags {
  @Option(
    names = ["--data-spec-resource-path"],
    description =
      ["The path to the data-spec resource for this sub-spec. Must be textproto format."],
    required = true,
  )
  lateinit var dataSpecResourcePath: String
    private set

  @ArgGroup(
    exclusive = false,
    multiplicity = "1..*",
    heading =
      "EntityKey to attach to every LabeledImpression contributed by this sub-spec. " +
        "At least one must be specified; pass the pair multiple times for multiple EntityKeys.%n",
  )
  lateinit var entityKeyFlags: List<EntityKeyFlags>
    private set
}

/**
 * Flags describing a single `LabeledImpression.EntityKey` to stamp on every generated impression.
 */
private class EntityKeyFlags {
  @Option(
    names = ["--entity-key-type"],
    description =
      [
        "Type of the entity in the DataProvider's system. Must be URL-safe. " +
          "Pair with --entity-key-id; specify both flags together repeatedly to attach multiple EntityKeys."
      ],
    required = true,
  )
  lateinit var entityType: String
    private set

  @Option(
    names = ["--entity-key-id"],
    description = ["ID of the entity in the DataProvider's system. Must be URL-safe."],
    required = true,
  )
  lateinit var entityId: String
    private set
}

fun main(args: Array<String>) = commandLineMain(GenerateSyntheticData(), args)
