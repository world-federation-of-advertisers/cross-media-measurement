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
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import java.io.File
import java.nio.file.Paths
import java.time.ZoneId
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
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpression
import org.wfanet.measurement.edpaggregator.v1alpha.LabeledImpressionKt.entityKey
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
    names = ["--event-group-reference-id"],
    description = ["The EDP generated event group reference id for this campaign."],
    required = true,
  )
  lateinit var eventGroupReferenceId: String
    private set

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
    names = ["--data-spec-resource-path"],
    description = ["The path to the resource of the data-spec. Must be textproto format."],
    required = true,
  )
  lateinit var dataSpecResourcePath: String
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
    names = ["--entity-key"],
    description =
      [
        "Optional. An EntityKey to attach to every generated LabeledImpression, in the form " +
          "'entity_type=entity_id'. May be repeated to attach multiple EntityKeys. " +
          "Both 'entity_type' and 'entity_id' must be URL-safe."
      ],
    required = false,
  )
  private var entityKeySpecs: List<String> = emptyList()

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
    val syntheticPopulationSpec: SyntheticPopulationSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve(populationSpecResourcePath).toFile(),
        SyntheticPopulationSpec.getDefaultInstance(),
      )
    val syntheticEventGroupSpec: SyntheticEventGroupSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve(dataSpecResourcePath).toFile(),
        SyntheticEventGroupSpec.getDefaultInstance(),
      )
    // TODO(world-federation-of-advertisers/cross-media-measurement#2360): Consider supporting other
    // event types.
    val events: Sequence<LabeledEventDateShard<TestEvent>> =
      SyntheticDataGeneration.generateEvents(
        messageInstance = TestEvent.getDefaultInstance(),
        populationSpec = syntheticPopulationSpec,
        syntheticEventGroupSpec = syntheticEventGroupSpec,
        zoneId = ZoneId.of(zoneId),
      )
    val kmsClient: KmsClient = run {
      when (kmsType) {
        KmsType.FAKE -> {
          val client = FakeKmsClient()
          val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
          client.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
          client
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
            AwsWebIdentityCredentials(
              roleArn = awsRoleArn,
              webIdentityTokenFilePath = awsWebIdentityTokenFile,
              roleSessionName = awsRoleSessionName,
              region = awsRegion,
            )
          AwsKmsClientFactory().getKmsClient(awsConfig)
        }
        KmsType.GCP_TO_AWS -> {
          throw UnsupportedOperationException(
            "GCP_TO_AWS is not yet supported in GenerateSyntheticData. Use VerifySyntheticData."
          )
        }
      }
    }
    val modelLineName = ModelLineKey.fromName(modelLine)?.modelLineId
    val eventGroupPath = "model-line/$modelLineName/event-group-reference-id/$eventGroupReferenceId"
    val parsedEntityKeys: List<LabeledImpression.EntityKey> = entityKeySpecs.map(::parseEntityKey)
    runBlocking {
      val impressionWriter =
        ImpressionsWriter(
          eventGroupReferenceId,
          eventGroupPath,
          kekUri,
          kmsClient,
          outputBucket,
          outputBucket,
          storagePath,
          schema,
          parsedEntityKeys,
        )
      require(flatOutputBasePath == null || impressionMetadataBasePath == null) {
        "Cannot specify both --impression-metadata-base-path and --flat-output-base-path; set exactly one or neither"
      }
      impressionWriter.writeLabeledImpressionData(
        events,
        modelLine,
        impressionsBasePath = impressionMetadataBasePath,
        flatOutputBasePath = flatOutputBasePath,
      )
    }
  }

  /**
   * Parses an `entity_type=entity_id` CLI value into a [LabeledImpression.EntityKey].
   *
   * Splits on the first `=` so that, e.g., `creative=creative-123` yields
   * `entityType="creative"`, `entityId="creative-123"`. Both halves must be non-empty.
   */
  private fun parseEntityKey(spec: String): LabeledImpression.EntityKey {
    val separatorIndex = spec.indexOf('=')
    require(separatorIndex > 0 && separatorIndex < spec.length - 1) {
      "Invalid --entity-key value '$spec'; expected 'entity_type=entity_id' with both sides non-empty"
    }
    val type = spec.substring(0, separatorIndex)
    val id = spec.substring(separatorIndex + 1)
    return entityKey {
      entityType = type
      entityId = id
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
  }

  init {
    AeadConfig.register()
    StreamingAeadConfig.register()
  }
}

fun main(args: Array<String>) = commandLineMain(GenerateSyntheticData(), args)
