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
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.loadtest.dataprovider.LabeledEventDateShard
import org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration
import org.wfanet.measurement.loadtest.edpaggregator.testing.ImpressionsWriter
import picocli.CommandLine.Command
import picocli.CommandLine.Option

enum class KmsType {
  FAKE,
  GCP,
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
    description = ["The model line for this campaign."],
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
  lateinit var impressionMetadataBasePath: String
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
      }
    }
    val modelLineName = ModelLineKey.fromName(modelLine)?.modelLineId
    val eventGroupPath = "model-line/$modelLineName/event-group-reference-id/$eventGroupReferenceId"
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
        )
      impressionWriter.writeLabeledImpressionData(events, modelLine, impressionMetadataBasePath)
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
