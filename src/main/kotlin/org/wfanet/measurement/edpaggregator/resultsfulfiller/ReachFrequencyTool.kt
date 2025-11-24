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
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import com.google.crypto.tink.streamingaead.StreamingAeadConfig
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.Message
import com.google.type.interval
import io.opentelemetry.api.common.Attributes
import java.io.File
import java.time.Instant
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import kotlin.math.roundToInt
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.PublicKeyHandle
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.computation.HistogramComputations
import org.wfanet.measurement.computation.ReachAndFrequencyComputations
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.FrequencyVectorBuilder
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.ParallelInMemoryVidIndexMap
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.size
import org.wfanet.measurement.edpaggregator.resultsfulfiller.EventProcessingOrchestrator
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ParallelBatchedPipeline
import org.wfanet.measurement.edpaggregator.resultsfulfiller.FilterSpec
import org.wfanet.measurement.edpaggregator.resultsfulfiller.StripedByteFrequencyVector
import org.wfanet.measurement.edpaggregator.resultsfulfiller.EventBatch
import org.wfanet.measurement.edpaggregator.resultsfulfiller.EventReaderResult
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ProgressTracker
import org.wfanet.measurement.edpaggregator.resultsfulfiller.StorageEventReader
import org.wfanet.measurement.edpaggregator.resultsfulfiller.EventSource
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ResultsFulfillerMetrics
import org.wfanet.measurement.edpaggregator.resultsfulfiller.BlobDetailsLoader
import picocli.CommandLine.Command
import picocli.CommandLine.ITypeConverter
import picocli.CommandLine.Option

/**
 * Offline tool for computing reach and frequency from impression blobs without contacting the CMMS.
 *
 * This builds the same event-processing pipeline used by [ResultsFulfiller], but instead of
 * fulfilling requisitions it emits reach and frequency summaries for user-supplied filters.
 *
 * All inputs are provided via flags:
 * - Impression metadata (BlobDetails) locations
 * - CEL filters and collection intervals
 * - Event group reference IDs
 * - Population spec file and measurement parameters (sampling interval + max frequency)
 * - Descriptor set for the event template
 *
 * No RPCs are made; storage is read directly using the supplied URIs.
 */
@Command(
  name = "reach-frequency",
  description = ["Compute reach and frequency for supplied filters from impression blobs"],
)
class ReachFrequencyCommand : Runnable {
  @Option(
    names = ["--metadata-uri"],
    description = ["BlobDetails metadata URI. May be supplied multiple times."],
    required = true,
  )
  private var metadataUris: List<String> = emptyList()

  @Option(
    names = ["--population-spec-file"],
    description = ["Path to a PopulationSpec textproto file."],
    required = true,
  )
  private lateinit var populationSpecFile: File

  @Option(
    names = ["--descriptor-set-file"],
    description = ["Path to a FileDescriptorSet containing the event template type."],
    required = true,
  )
  private lateinit var descriptorSetFile: File

  @Option(
    names = ["--event-message-type"],
    description = ["Fully-qualified protobuf message name for the event template."],
    required = true,
  )
  private lateinit var eventMessageType: String

  @Option(
    names = ["--project-id"],
    description = ["GCP project id for gs:// URIs."],
    required = false,
  )
  private var projectId: String? = null

  @Option(
    names = ["--storage-root"],
    description = ["Local root directory when using file:/// URIs."],
    required = false,
  )
  private var storageRoot: File? = null

  @Option(
    names = ["--kms-type"],
    description = ["KMS provider for decrypting blobs: \${COMPLETION-CANDIDATES}"],
    required = false,
    defaultValue = "GCP",
  )
  private lateinit var kmsType: KmsType

  enum class KmsType {
    GCP,
    NONE,
  }

  @Option(
    names = ["--batch-size"],
    description = ["Event batch size for both reading and processing."],
    defaultValue = "256",
  )
  private var batchSize: Int = 256

  @Option(
    names = ["--worker-channel-capacity"],
    description = ["Per-worker channel capacity for the pipeline (in batches)."],
    defaultValue = "128",
  )
  private var workerChannelCapacity: Int = 128

  @Option(
    names = ["--workers"],
    description = ["Number of parallel workers for the pipeline."],
    defaultValue = "4",
  )
  private var workers: Int = 4

  @Option(
    names = ["--thread-pool-size"],
    description = ["Size of the dispatcher thread pool used by the pipeline."],
    defaultValue = "4",
  )
  private var threadPoolSize: Int = 4

  @Option(
    names = ["--max-frequency"],
    description = ["Maximum frequency (ReachAndFrequency.maximum_frequency)."],
    defaultValue = "10",
  )
  private var maxFrequency: Int = 10

  @Option(
    names = ["--vid-sampling-start"],
    description = ["VidSamplingInterval.start in [0.0,1.0]."],
    defaultValue = "0.0",
  )
  private var vidSamplingStart: Double = 0.0

  @Option(
    names = ["--vid-sampling-width"],
    description = ["VidSamplingInterval.width in (0.0,1.0]."],
    defaultValue = "1.0",
  )
  private var vidSamplingWidth: Double = 1.0

  @Option(
    names = ["--filter-name"],
    description = ["Name for a filter; may be supplied multiple times."],
    required = true,
    split = ",",
  )
  private var filterNames: List<String> = emptyList()

  @Option(
    names = ["--cel-filter"],
    description = ["CEL filter expression per filter name (aligned with --filter-name order)."],
    split = ",",
    defaultValue = "",
  )
  private var celFilters: List<String> = emptyList()

  @Option(
    names = ["--collection-start"],
    description = ["ISO-8601 start time per filter (aligned with --filter-name order)."],
    converter = InstantConverter::class,
    split = ",",
    required = true,
  )
  private var collectionStarts: List<Instant> = emptyList()

  @Option(
    names = ["--collection-end"],
    description = ["ISO-8601 end time per filter (aligned with --filter-name order)."],
    converter = InstantConverter::class,
    split = ",",
    required = true,
  )
  private var collectionEnds: List<Instant> = emptyList()

  @Option(
    names = ["--event-group-reference-ids"],
    description = ["Comma-separated event group reference IDs per filter (aligned by order)."],
    split = ";",
    required = true,
  )
  private var eventGroupReferenceIds: List<String> = emptyList()

  override fun run() = runBlocking {
    AeadConfig.register()
    StreamingAeadConfig.register()

    validateFilterInputs()

    val kmsClient = buildKmsClient()
    val storageConfig = StorageConfig(rootDirectory = storageRoot, projectId = projectId)
    val populationSpec = parsePopulationSpec()
    val measurementSpec = buildMeasurementSpec()
    val eventDescriptor = loadEventDescriptor()
    val blobDetails = loadBlobDetails(storageConfig)
    val filterSpecs = buildFilterSpecs()

    val vidIndexMap =
      ResultsFulfillerMetrics.vidIndexBuildDuration.measured {
        ParallelInMemoryVidIndexMap.build(populationSpec)
      }

    val orchestrator = EventProcessingOrchestrator<Message>(NOOP_PRIVATE_KEY_HANDLE)
    val sinkByFilterSpec =
      orchestrator.createSinksFromFilterSpecs(
        filterSpecs.values,
        vidIndexMap,
        populationSpec,
        eventDescriptor,
      )

    val pipeline =
      ParallelBatchedPipeline<Message>(
        batchSize = batchSize,
        workers = workers,
        workerChannelCapacity = workerChannelCapacity,
      )
    val eventSource =
      BlobDetailsEventSource(
        blobDetails = blobDetails,
        kmsClient = kmsClient,
        impressionsStorageConfig = storageConfig,
        descriptor = eventDescriptor,
        batchSize = batchSize,
      )

    val executor =
      ForkJoinPool(
        threadPoolSize,
        { pool ->
          val thread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool)
          thread.name = "ReachFrequencyPool-${thread.poolIndex}"
          thread.isDaemon = false
          thread
        },
        null,
        true,
      )

    try {
      withContext(executor.asCoroutineDispatcher()) {
        pipeline.processEventBatches(eventSource, sinkByFilterSpec.values.toList())
      }
    } finally {
      executor.shutdown()
      executor.awaitTermination(10, TimeUnit.SECONDS)
    }

    val results =
      filterSpecs.mapValues { (name, spec) ->
        val frequencyVector = sinkByFilterSpec.getValue(spec).getFrequencyVector()
        computeReachAndFrequency(frequencyVector, populationSpec, measurementSpec)
          .also { logSummary(name, spec, it) }
      }

    printSummary(results, filterSpecs)
  }

  private fun validateFilterInputs() {
    require(
      filterNames.size == collectionStarts.size &&
        collectionStarts.size == collectionEnds.size &&
        collectionEnds.size == eventGroupReferenceIds.size
    ) {
      "Provide matching counts for --filter-name, --collection-start, --collection-end, " +
        "and --event-group-reference-ids"
    }
    require(filterNames.isNotEmpty()) { "At least one filter must be provided" }
    if (celFilters.isNotEmpty()) {
      require(celFilters.size == filterNames.size || celFilters.size == 1) {
        "--cel-filter must be supplied once or once per filter"
      }
    }
  }

  private fun buildFilterSpecs(): Map<String, FilterSpec> {
    val expressions =
      if (celFilters.isEmpty()) {
        List(filterNames.size) { "" }
      } else if (celFilters.size == 1 && filterNames.size > 1) {
        List(filterNames.size) { celFilters.first() }
      } else {
        celFilters
      }
    return filterNames.indices.associate { index ->
      val start = collectionStarts[index]
      val end = collectionEnds[index]
      val interval =
        com.google.type.interval {
          startTime = start.toProtoTime()
          endTime = end.toProtoTime()
        }
      val groups = eventGroupReferenceIds[index].split(",").filter { it.isNotBlank() }
      filterNames[index] to
        FilterSpec(
          celExpression = expressions[index],
          collectionInterval = interval,
          eventGroupReferenceIds = groups,
        )
    }
  }

  private fun parsePopulationSpec(): PopulationSpec {
    return populationSpecFile.inputStream().reader().use {
      parseTextProto(it, PopulationSpec.getDefaultInstance())
    }
  }

  private fun buildMeasurementSpec(): MeasurementSpec {
    require(maxFrequency > 0) { "--max-frequency must be > 0" }
    require(vidSamplingWidth > 0.0 && vidSamplingWidth <= 1.0) {
      "--vid-sampling-width must be in (0, 1]"
    }
    require(vidSamplingStart >= 0.0 && vidSamplingStart <= 1.0) {
      "--vid-sampling-start must be in [0, 1]"
    }
    return measurementSpec {
      reachAndFrequency =
        MeasurementSpecKt.reachAndFrequency {
          maximumFrequency = maxFrequency
          reachPrivacyParams = differentialPrivacyParams {
            epsilon = 0.0
            delta = 0.0
          }
          frequencyPrivacyParams = differentialPrivacyParams {
            epsilon = 0.0
            delta = 0.0
          }
        }
      vidSamplingInterval = MeasurementSpecKt.vidSamplingInterval {
        start = vidSamplingStart.toFloat()
        width = vidSamplingWidth.toFloat()
      }
    }
  }

  private fun loadEventDescriptor(): Descriptors.Descriptor {
    val fileDescriptorSet =
      descriptorSetFile.inputStream().use { DescriptorProtos.FileDescriptorSet.parseFrom(it) }
    val descriptors =
      ProtoReflection.buildDescriptors(
        descriptorSets = listOf(fileDescriptorSet),
        compiledFileDescriptors = ProtoReflection.WELL_KNOWN_TYPES,
      )
    return descriptors.firstOrNull { it.fullName == eventMessageType }
      ?: error("Event message type $eventMessageType not found in descriptor set")
  }

  private suspend fun loadBlobDetails(storageConfig: StorageConfig): List<BlobDetails> {
    return metadataUris.map { BlobDetailsLoader.load(it, storageConfig) }
  }

  private fun buildKmsClient(): KmsClient? {
    return when (kmsType) {
      KmsType.NONE -> null
      KmsType.GCP -> GcpKmsClient().withDefaultCredentials()
    }
  }

  private fun computeReachAndFrequency(
    frequencyVector: StripedByteFrequencyVector,
    populationSpec: PopulationSpec,
    measurementSpec: MeasurementSpec,
  ): ReachFrequencyResult {
    val builder =
      FrequencyVectorBuilder(
        populationSpec = populationSpec,
        measurementSpec = measurementSpec,
        frequencyDataBytes = frequencyVector.getByteArray(),
        strict = false,
      )
    val maxFrequency = measurementSpec.reachAndFrequency.maximumFrequency
    val histogram =
      HistogramComputations.buildHistogram(
        frequencyVector = builder.frequencyDataArray,
        maxFrequency = maxFrequency,
      )
    val scaledReach =
      ReachAndFrequencyComputations.computeReach(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = measurementSpec.vidSamplingInterval.width,
        vectorSize = populationSpec.size.toInt(),
        dpParams = null,
        kAnonymityParams = null,
      )
    val frequencyDistribution =
      ReachAndFrequencyComputations.computeFrequencyDistribution(
        rawHistogram = histogram,
        maxFrequency = maxFrequency,
        dpParams = null,
        kAnonymityParams = null,
        vidSamplingIntervalWidth = measurementSpec.vidSamplingInterval.width,
      )
    val sampleReach = histogram.sum()
    val totalImpressions =
      builder.frequencyDataArray.fold(0L) { acc, value -> acc + value.toLong() }

    return ReachFrequencyResult(
      scaledReach = scaledReach,
      sampleReach = sampleReach,
      totalImpressions = totalImpressions,
      frequencyDistribution =
        frequencyDistribution.mapKeys { (frequency, _) -> frequency.toInt() }.toSortedMap(),
    )
  }

  private fun logSummary(
    name: String,
    filterSpec: FilterSpec,
    result: ReachFrequencyResult,
  ) {
    Tracing.trace("reach_frequency_result", Attributes.empty()) {
      logger.info(
        "Filter '$name' reach=${result.scaledReach} impressions=${result.totalImpressions} " +
          "eventGroups=${filterSpec.eventGroupReferenceIds.joinToString()} " +
          "interval=${filterSpec.collectionInterval.startTime}.." +
          filterSpec.collectionInterval.endTime,
      )
    }
  }

  private fun printSummary(
    results: Map<String, ReachFrequencyResult>,
    filterSpecs: Map<String, FilterSpec>,
  ) {
    println("Computed reach and frequency for ${results.size} filter(s)")
    results.forEach { (name, result) ->
      val filterSpec = filterSpecs.getValue(name)
      println("- $name")
      println("  Event groups: ${filterSpec.eventGroupReferenceIds.joinToString()}")
      println(
        "  Interval: ${filterSpec.collectionInterval.startTime} -> ${filterSpec.collectionInterval.endTime}"
      )
      println(
        "  Reach (scaled by sampling): ${result.scaledReach} " +
          "(sample reach=${result.sampleReach})",
      )
      println("  Total impressions (sample): ${result.totalImpressions}")
      println("  Frequency distribution:")
      result.frequencyDistribution.forEach { (freq, percentage) ->
        val pct = (percentage * 10000).roundToInt() / 100.0
        println("    $freq: $pct%")
      }
    }
  }

  companion object {
    private val logger = Logger.getLogger(ReachFrequencyCommand::class.java.name)

    private val NOOP_PRIVATE_KEY_HANDLE =
      object : PrivateKeyHandle {
        override val publicKey: PublicKeyHandle =
          object : PublicKeyHandle {
            override fun hybridEncrypt(
              plaintext: com.google.protobuf.ByteString,
              contextInfo: com.google.protobuf.ByteString?,
            ): com.google.protobuf.ByteString {
              throw UnsupportedOperationException("Encryption not supported in offline tool")
            }
          }

        override fun hybridDecrypt(
          ciphertext: com.google.protobuf.ByteString,
          contextInfo: com.google.protobuf.ByteString?,
        ): com.google.protobuf.ByteString {
          throw UnsupportedOperationException("Decryption not supported in offline tool")
        }
      }
  }
}

private class InstantConverter : ITypeConverter<Instant> {
  override fun convert(value: String): Instant = Instant.parse(value)
}

private data class ReachFrequencyResult(
  val scaledReach: Long,
  val sampleReach: Long,
  val totalImpressions: Long,
  val frequencyDistribution: Map<Int, Double>,
)

/**
 * Event source that reads events directly from provided [BlobDetails], without calling any APIs to
 * discover metadata.
 */
private class BlobDetailsEventSource(
  private val blobDetails: List<BlobDetails>,
  private val kmsClient: KmsClient?,
  private val impressionsStorageConfig: StorageConfig,
  private val descriptor: Descriptors.Descriptor,
  private val batchSize: Int,
) : EventSource<Message> {

  override fun generateEventBatches(): Flow<EventBatch<Message>> {
    return channelFlow {
      val eventReaders =
        blobDetails.map {
          StorageEventReader(
            it,
            kmsClient,
            impressionsStorageConfig,
            descriptor,
            batchSize,
          )
        }

      ProgressTracker(eventReaders.size).use { progressTracker ->
        logger.info(
          "Processing ${eventReaders.size} EventReaders across ${blobDetails.size} blob(s)"
        )
        coroutineScope {
          eventReaders.forEach { eventReader ->
            launch {
              val result = processEventReader(eventReader) { send(it) }
              progressTracker.updateProgress(result.eventCount, result.batchCount)
            }
          }
        }
      }
    }
  }

  private suspend fun processEventReader(
    eventReader: StorageEventReader,
    sendEventBatch: suspend (EventBatch<Message>) -> Unit,
  ): EventReaderResult {
    var batchCount = 0
    var eventCount = 0
    val blobDetails = eventReader.getBlobDetails()

    eventReader.readEvents().collect { events ->
      val eventBatch =
        EventBatch(
          events = events,
          minTime = events.minOf { it.timestamp },
          maxTime = events.maxOf { it.timestamp },
          eventGroupReferenceId = blobDetails.eventGroupReferenceId,
        )
      sendEventBatch(eventBatch)
      batchCount++
      eventCount += events.size
    }
    return EventReaderResult(batchCount, eventCount)
  }

  companion object {
    private val logger = Logger.getLogger(BlobDetailsEventSource::class.java.name)
  }
}

fun main(args: Array<String>) = commandLineMain(ReachFrequencyCommand(), args)
