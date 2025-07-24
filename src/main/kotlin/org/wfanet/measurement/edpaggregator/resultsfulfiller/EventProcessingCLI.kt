/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.aead.AeadConfig
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.type.Interval
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.Timestamp
import com.google.protobuf.TypeRegistry
import java.io.File
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.util.logging.Logger
import kotlin.io.path.Path
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.edpaggregator.StorageConfig
import picocli.CommandLine

/**
 * Command-line interface for the event processing pipeline.
 * Handles argument parsing and delegates to the pipeline orchestrator.
 */
@CommandLine.Command(
  name = "ResultsFulfillerPipeline",
  description = ["Event processing pipeline with synthetic data generation, filtering and measurement"],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
class EventProcessingCLI(
  private val orchestrator: EventProcessingOrchestrator = EventProcessingOrchestrator()
) : Runnable {

  private fun loadFileDescriptorSets(
    files: Iterable<File>
  ): List<DescriptorProtos.FileDescriptorSet> {
    return files.map { file ->
      file.inputStream().use { input ->
        DescriptorProtos.FileDescriptorSet.parseFrom(input, EXTENSION_REGISTRY)
      }
    }
  }

  private fun buildTypeRegistry(): TypeRegistry {
    return TypeRegistry.newBuilder()
      .apply {
        add(COMPILED_PROTOBUF_TYPES.flatMap { it.messageTypes })
        if (eventTemplateDescriptorSetFiles.isNotEmpty()) {
          add(
            ProtoReflection.buildDescriptors(
              loadFileDescriptorSets(eventTemplateDescriptorSetFiles),
              COMPILED_PROTOBUF_TYPES,
            )
          )
        }
      }
      .build()
  }

  companion object {
    private val logger = Logger.getLogger(EventProcessingCLI::class.java.name)

    /**
     * [Descriptors.FileDescriptor]s of protobuf types known at compile-time that may be loaded from
     * a [DescriptorProtos.FileDescriptorSet].
     */
    private val COMPILED_PROTOBUF_TYPES: Iterable<Descriptors.FileDescriptor> =
      (ProtoReflection.WELL_KNOWN_TYPES.asSequence() + TestEvent.getDescriptor().file)
        .asIterable()

    private val EXTENSION_REGISTRY =
      ExtensionRegistry.newInstance()
        .also { EventAnnotationsProto.registerAllExtensions(it) }
        .unmodifiable

    @JvmStatic
    fun main(args: Array<String>) = commandLineMain(EventProcessingCLI(), args)
  }

  @CommandLine.Option(
    names = ["--start-date"],
    description = ["Start date for event range (YYYY-MM-DD)"],
    defaultValue = "2025-01-01"
  )
  private lateinit var startDateStr: String

  @CommandLine.Option(
    names = ["--end-date"],
    description = ["End date for event range (YYYY-MM-DD)"],
    defaultValue = "2025-01-02"
  )
  private lateinit var endDateStr: String

  @CommandLine.Option(
    names = ["--batch-size"],
    description = ["Event batch size for processing"],
    defaultValue = "10000"
  )
  private var batchSize: Int = 10000

  @CommandLine.Option(
    names = ["--channel-capacity"],
    description = ["Channel capacity for backpressure control"],
    defaultValue = "100"
  )
  private var channelCapacity: Int = 100

  @CommandLine.Option(
    names = ["--event-source-type"],
    description = ["Type of event source: SYNTHETIC or STORAGE"],
    defaultValue = "SYNTHETIC"
  )
  private lateinit var eventSourceTypeStr: String

  @CommandLine.Option(
    names = ["--population-spec-resource-path"],
    description = ["The path to the resource of the population-spec. Must be textproto format. Required for SYNTHETIC source."],
    required = false
  )
  private var populationSpecResourcePath: String? = null

  @CommandLine.Option(
    names = ["--data-spec-resource-path"],
    description = ["The path to the resource of the data-spec. Must be textproto format. Required for SYNTHETIC source."],
    required = false
  )
  private var dataSpecResourcePath: String? = null

  @CommandLine.Option(
    names = ["--event-group-reference-ids"],
    description = ["Comma-separated list of event group reference IDs. Required for STORAGE source."],
    defaultValue = ""
  )
  private var eventGroupReferenceIdsStr: String = ""

  @CommandLine.Option(
    names = ["--impressions-storage-root"],
    description = ["Root directory for impressions storage. Required for STORAGE source."],
    defaultValue = ""
  )
  private var impressionsStorageRoot: String = ""


  @CommandLine.Option(
    names = ["--impression-dek-storage-root"],
    description = ["Root directory for impression DEK storage. Required for STORAGE source."],
    defaultValue = ""
  )
  private var impressionDekStorageRoot: String = ""

  @CommandLine.Option(
    names = ["--labeled-impressions-dek-prefix"],
    description = ["Prefix for labeled impressions DEK. Required for STORAGE source."],
    defaultValue = ""
  )
  private var labeledImpressionsDekPrefix: String = ""

  @CommandLine.Option(
    names = ["--zone-id"],
    description = ["The Zone ID by which to generate the events"],
    defaultValue = "UTC"
  )
  private var zoneId: String = "UTC"

  @CommandLine.Option(
    names = ["--use-parallel-pipeline"],
    description = ["Use parallel batched pipeline instead of single pipeline"],
    defaultValue = "false"
  )
  private var useParallelPipeline: Boolean = false

  @CommandLine.Option(
    names = ["--parallel-batch-size"],
    description = ["Batch size for parallel pipeline"],
    defaultValue = "256"
  )
  private var parallelBatchSize: Int = 100000

  @CommandLine.Option(
    names = ["--parallel-workers"],
    description = ["Number of worker threads for parallel pipeline (0 = use all CPU cores)"],
    defaultValue = "0"
  )
  private var parallelWorkers: Int = 0

  @CommandLine.Option(
    names = ["--thread-pool-size"],
    description = ["Custom thread pool size (0 = 4x CPU cores)"],
    defaultValue = "0"
  )
  private var threadPoolSize: Int = 0

  @CommandLine.Option(
    names = ["--collection-start-time"],
    description = ["Collection interval start time (ISO-8601 format, e.g., 2025-01-01T00:00:00Z)"],
    defaultValue = ""
  )
  private var collectionStartTime: String = ""

  @CommandLine.Option(
    names = ["--collection-end-time"],
    description = ["Collection interval end time (ISO-8601 format, e.g., 2025-01-02T00:00:00Z)"],
    defaultValue = ""
  )
  private var collectionEndTime: String = ""

  @CommandLine.Option(
    names = ["--disable-logging"],
    description = ["Disable batch progress logging"],
    defaultValue = "false"
  )
  private var disableLogging: Boolean = false

  @CommandLine.Option(
    names = ["--event-template-metadata-type"],
    description = [
      "Serialized FileDescriptorSet for EventTemplate metadata types.",
      "This can be specified multiple times."
    ],
    required = false
  )
  private var eventTemplateDescriptorSetFiles: List<File> = emptyList()

  override fun run() = runBlocking {
    logger.info("Starting Event Processing Pipeline")
    println("Event Processing Pipeline")
    println("Structure: Parallel Event Readers -> Batching -> Fan Out -> CEL Filtering -> Frequency Vector Sinks")
    println()

    try {
      initializeTink()

      val config = buildConfiguration()
      val typeRegistry = buildTypeRegistry()
      orchestrator.run(config, typeRegistry)
    } catch (e: Exception) {
      logger.severe("Pipeline execution failed: ${e.message}")
      throw e
    }
  }

  private fun initializeTink() {
    logger.info("Initializing Tink crypto")
    AeadConfig.register()
    StreamingAeadConfig.register()
    logger.info("Tink initialization completed successfully")
    println("Tink initialization complete")
  }

  /**
   * Builds the pipeline configuration from command line arguments.
   * Made public for testing purposes.
   */
  fun buildConfiguration(): PipelineConfiguration {
    val startDate = parseDate(startDateStr, "start date")
    val endDate = parseDate(endDateStr, "end date")
    val collectionInterval = parseCollectionInterval()
    val zoneIdParsed = ZoneId.of(zoneId)

    val eventSourceType = try {
      EventSourceType.valueOf(eventSourceTypeStr)
    } catch (e: IllegalArgumentException) {
      throw IllegalArgumentException("Invalid event source type: $eventSourceTypeStr. Must be SYNTHETIC or STORAGE")
    }

    return when (eventSourceType) {
      EventSourceType.SYNTHETIC -> {
        requireNotNull(populationSpecResourcePath) { "--population-spec-resource-path is required for SYNTHETIC source" }
        requireNotNull(dataSpecResourcePath) { "--data-spec-resource-path is required for SYNTHETIC source" }

        val populationSpec: SyntheticPopulationSpec = parseTextProto(
          Path(populationSpecResourcePath!!).toFile(),
          SyntheticPopulationSpec.getDefaultInstance()
        )

        logger.info("Population spec loaded: $populationSpec")
        println("Population spec: $populationSpec")

        val eventGroupSpec: SyntheticEventGroupSpec = parseTextProto(
          Path(dataSpecResourcePath!!).toFile(),
          SyntheticEventGroupSpec.getDefaultInstance()
        )

        PipelineConfiguration(
          startDate = startDate,
          endDate = endDate,
          batchSize = batchSize,
          channelCapacity = channelCapacity,
          eventSourceType = eventSourceType,
          populationSpec = populationSpec,
          eventGroupSpec = eventGroupSpec,
          zoneId = zoneIdParsed,
          useParallelPipeline = useParallelPipeline,
          parallelBatchSize = parallelBatchSize,
          parallelWorkers = if (parallelWorkers == 0) Runtime.getRuntime().availableProcessors() else parallelWorkers,
          threadPoolSize = if (threadPoolSize == 0) Runtime.getRuntime().availableProcessors() * 8 else threadPoolSize,
          collectionInterval = collectionInterval,
          disableLogging = disableLogging
        )
      }

      EventSourceType.STORAGE -> {
        require(eventGroupReferenceIdsStr.isNotEmpty()) { "--event-group-reference-ids is required for STORAGE source" }
        require(impressionsStorageRoot.isNotEmpty()) { "--impressions-storage-root is required for STORAGE source" }
        require(impressionDekStorageRoot.isNotEmpty()) { "--impression-dek-storage-root is required for STORAGE source" }
        require(labeledImpressionsDekPrefix.isNotEmpty()) { "--labeled-impressions-dek-prefix is required for STORAGE source" }
        requireNotNull(populationSpecResourcePath) { "--population-spec-resource-path is required for STORAGE source" }

        val populationSpec: SyntheticPopulationSpec = parseTextProto(
          Path(populationSpecResourcePath!!).toFile(),
          SyntheticPopulationSpec.getDefaultInstance()
        )

        val eventGroupReferenceIds = eventGroupReferenceIdsStr.split(",").map { it.trim() }.filter { it.isNotEmpty() }

        // Create StorageConfig instances
        val impressionsStorageConfig = StorageConfig(
          rootDirectory = File(impressionsStorageRoot),
          projectId = null // Can be null for local storage
        )

        val impressionDekStorageConfig = StorageConfig(
          rootDirectory = File(impressionDekStorageRoot),
          projectId = null
        )

        // Create TypeRegistry with TestEvent
        val typeRegistry = TypeRegistry.newBuilder()
          .add(TestEvent.getDescriptor())
          .build()

        // For now, we'll use null KmsClient (no encryption)
        val eventReader = EventReader(
          kmsClient = null,
          impressionsStorageConfig = impressionsStorageConfig,
          impressionDekStorageConfig = impressionDekStorageConfig,
          labeledImpressionsDekPrefix = labeledImpressionsDekPrefix,
          typeRegistry = typeRegistry
        )

        PipelineConfiguration(
          startDate = startDate,
          endDate = endDate,
          batchSize = batchSize,
          channelCapacity = channelCapacity,
          eventSourceType = eventSourceType,
          populationSpec = populationSpec,
          eventReader = eventReader,
          eventGroupReferenceIds = eventGroupReferenceIds,
          zoneId = zoneIdParsed,
          useParallelPipeline = useParallelPipeline,
          parallelBatchSize = parallelBatchSize,
          parallelWorkers = if (parallelWorkers == 0) Runtime.getRuntime().availableProcessors() else parallelWorkers,
          threadPoolSize = if (threadPoolSize == 0) Runtime.getRuntime().availableProcessors() * 8 else threadPoolSize,
          collectionInterval = collectionInterval,
          disableLogging = disableLogging
        )
      }
    }
  }

  private fun parseDate(dateStr: String, description: String): LocalDate {
    return try {
      LocalDate.parse(dateStr)
    } catch (e: Exception) {
      logger.severe("Failed to parse $description '$dateStr': ${e.message}")
      throw IllegalArgumentException("Invalid $description: $dateStr", e)
    }
  }

  private fun parseCollectionInterval(): Interval? {
    return if (collectionStartTime.isNotEmpty() && collectionEndTime.isNotEmpty()) {
      try {
        val startInstant = Instant.parse(collectionStartTime)
        val endInstant = Instant.parse(collectionEndTime)

        Interval.newBuilder()
          .setStartTime(Timestamp.newBuilder()
            .setSeconds(startInstant.epochSecond)
            .setNanos(startInstant.nano))
          .setEndTime(Timestamp.newBuilder()
            .setSeconds(endInstant.epochSecond)
            .setNanos(endInstant.nano))
          .build()
      } catch (e: Exception) {
        logger.severe("Failed to parse collection interval: ${e.message}")
        throw IllegalArgumentException("Invalid collection interval", e)
      }
    } else {
      null
    }
  }
}
