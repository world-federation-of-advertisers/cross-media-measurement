/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.loadtest.edpaggregator.tools

import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.TypeRegistry
import java.io.File
import java.nio.file.Files
import java.time.LocalDate
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.loadtest.dataprovider.EntityKey
import org.wfanet.measurement.loadtest.dataprovider.EntityKeyedLabeledEventDateShard
import org.wfanet.measurement.loadtest.dataprovider.EntityKeysWithLabeledEvents
import org.wfanet.measurement.loadtest.dataprovider.SyntheticDataGeneration
import org.wfanet.measurement.loadtest.edpaggregator.testing.RawImpressionColumns
import org.wfanet.measurement.loadtest.edpaggregator.testing.RawImpressionsWriter
import org.wfanet.measurement.storage.ParquetValue
import org.wfanet.measurement.storage.parquetValue
import picocli.CommandLine.Command
import picocli.CommandLine.Option

/**
 * BENCHMARK-ONLY (throwaway) generator of PME-encrypted raw-impression Parquet for one VID
 * sub-range on one date, uploaded to Cloud Storage. Mirrors the cloud-test SeedRawImpressionsRule
 * write path (RawImpressionsWriter + testEventColumns + person entity key) so the deployed VID
 * Labeling pipeline consumes it unchanged. Run many instances in parallel (disjoint VID ranges,
 * distinct --blob-key-id) to produce many ~300MB files under one date folder.
 */
@Command(
  name = "generate-benchmark-raw-impressions",
  description = ["Generates one raw-impression Parquet shard for a VID sub-range."],
)
class GenerateBenchmarkRawImpressions : Runnable {
  @Option(names = ["--population-spec-file"], required = true) lateinit var populationSpecFile: File
  @Option(names = ["--data-spec-file"], required = true) lateinit var dataSpecFile: File
  @Option(names = ["--blob-key-id"], required = true) lateinit var blobKeyId: String
  @Option(names = ["--output-bucket"], required = true) lateinit var outputBucket: String
  @Option(names = ["--output-prefix"], required = false, defaultValue = "edp/edp7")
  lateinit var outputPrefix: String
  @Option(names = ["--kek-uri"], required = true) lateinit var kekUri: String
  @Option(names = ["--date"], required = true, defaultValue = "2021-03-21") lateinit var date: String

  override fun run() {
    val typeRegistry = TypeRegistry.newBuilder().add(Person.getDescriptor()).build()
    val populationSpec =
      parseTextProto(populationSpecFile, PopulationSpec.getDefaultInstance(), typeRegistry)
    val dataSpec = parseTextProto(dataSpecFile, SyntheticEventGroupSpec.getDefaultInstance())
    val targetDate = LocalDate.parse(date)
    val kmsClient: KmsClient = GcpKmsClient().withDefaultCredentials()
    val storage: Storage = StorageOptions.getDefaultInstance().service
    val localRoot: File = Files.createTempDirectory("bench-raw").toFile()
    val rootPath = Path(localRoot.toURI())

    val writer =
      RawImpressionsWriter(
        blobKeyId = blobKeyId,
        kekUri = kekUri,
        kmsClient = kmsClient,
        storageConfiguration = Configuration(),
        rootPath = rootPath,
        requiredEntityKeyColumns =
          mapOf(RawImpressionColumns.ENTITY_TYPE_PERSON to RawImpressionColumns.PERSON_ID),
      )

    val shards: Sequence<EntityKeyedLabeledEventDateShard<TestEvent>> =
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          dataSpec,
        )
        .filter { it.localDate == targetDate }
        .map { shard ->
          EntityKeyedLabeledEventDateShard(
            localDate = shard.localDate,
            entityKeysWithLabeledEvents =
              shard.labeledEvents.map { event ->
                EntityKeysWithLabeledEvents(
                  entityKeys =
                    listOf(
                      EntityKey(RawImpressionColumns.ENTITY_TYPE_PERSON, "person-" + event.vid)
                    ),
                  labeledEvents = sequenceOf(event),
                )
              },
          )
        }

    val blobKeys: List<String> = runBlocking {
      writer.writeRawImpressions(shards, outputPrefix) { event -> testEventColumns(event) }
    }
    for (blobKey in blobKeys) {
      val local = File(localRoot, blobKey)
      storage.create(
        BlobInfo.newBuilder(BlobId.of(outputBucket, blobKey)).build(),
        local.readBytes(),
      )
      logger.info("uploaded gs://" + outputBucket + "/" + blobKey + " (" + local.length() + " bytes)")
    }
  }

  companion object {
    private val logger = Logger.getLogger(GenerateBenchmarkRawImpressions::class.java.name)

    init {
      AeadConfig.register()
      StreamingAeadConfig.register()
    }

    fun testEventColumns(event: TestEvent): Map<String, ParquetValue> =
      mapOf(
        RawImpressionColumns.PERSON_GENDER to parquetValue { stringValue = event.person.gender.name },
        RawImpressionColumns.PERSON_AGE_GROUP to
          parquetValue { stringValue = event.person.ageGroup.name },
      )
  }
}

fun main(args: Array<String>) = commandLineMain(GenerateBenchmarkRawImpressions(), args)
