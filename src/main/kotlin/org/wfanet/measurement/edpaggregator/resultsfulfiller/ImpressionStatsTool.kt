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
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.TypeRegistry
import com.google.protobuf.Timestamp
import com.google.type.Interval
import java.io.File
import java.time.Instant
import java.util.LinkedHashSet
import java.util.logging.Logger
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.forEach
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventAnnotationsProto
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.storage.SelectedStorageClient
import picocli.CommandLine.Command
import picocli.CommandLine.Option

@Command(
  name = "impression-stats",
  description = ["Counts records and distinct VIDs from impression blobs described by BlobDetails"],
)
class ImpressionStatsCommand : Runnable {
  @Option(
    names = ["--metadata-uri"],
    description = ["BlobDetails metadata URI. May be supplied multiple times."],
    required = false,
  )
  private var metadataUris: List<String> = emptyList()

  @Option(
    names = ["--metadata-pattern"],
    description =
      [
        "Glob pattern applied to metadata file basenames (default: '*'). May be supplied multiple times."
      ],
    required = false,
  )
  private var metadataPatterns: List<String> = emptyList()

  @Option(
    names = ["--metadata-prefix"],
    description = ["Prefix to discover BlobDetails metadata (e.g. gs://bucket/path)."],
    required = false,
  )
  private var metadataPrefixes: List<String> = emptyList()

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

  @Option(
    names = ["--event-message-descriptor-set"],
    description =
      ["Serialized FileDescriptorSet containing the event message definition. May be repeated."],
    required = true,
  )
  private lateinit var eventMessageDescriptorSetFiles: List<File>

  @Option(
    names = ["--event-message-type-url"],
    description = ["Type URL for the event message packed into LabeledImpression.event"],
    required = true,
  )
  private lateinit var eventMessageTypeUrl: String

  @Option(
    names = ["--cel-filter"],
    description = ["CEL expression to filter events. Leave empty to disable filtering."],
    required = false,
  )
  private var celFilters: List<String> = emptyList()

  @Option(
    names = ["--collection-start"],
    description = ["Optional ISO-8601 start time (inclusive) for filtering (e.g. 2024-01-01T00:00:00Z)."],
    required = false,
  )
  private var collectionStartTime: String? = null

  @Option(
    names = ["--collection-end"],
    description = ["Optional ISO-8601 end time (exclusive) for filtering (e.g. 2024-02-01T00:00:00Z)."],
    required = false,
  )
  private var collectionEndTime: String? = null

  enum class KmsType {
    GCP,
    NONE,
  }

  override fun run() = runBlocking {
    AeadConfig.register()
    StreamingAeadConfig.register()

    val storageConfig = StorageConfig(rootDirectory = storageRoot, projectId = projectId)
    val resolvedMetadataUris = resolveMetadataUris(storageConfig)
    val eventDescriptor = loadEventMessageDescriptor()
    val statsCalculator =
      ImpressionStatsCalculator(
        storageConfig = storageConfig,
        kmsClient = buildKmsClient(),
        eventDescriptor = eventDescriptor,
        celExpressions = celFilters,
        collectionIntervalOverride = parseCollectionIntervalOverride(),
      )
    val stats = statsCalculator.compute(resolvedMetadataUris)
    printSummary(stats)
  }

  private fun buildKmsClient(): KmsClient? {
    return when (kmsType) {
      KmsType.NONE -> null
      KmsType.GCP -> GcpKmsClient().withDefaultCredentials()
    }
  }

  private suspend fun resolveMetadataUris(storageConfig: StorageConfig): List<String> {
    val uris = LinkedHashSet<String>()
    uris.addAll(metadataUris)

    val patternsToUse = if (metadataPatterns.isEmpty()) listOf("*") else metadataPatterns
    for (prefix in metadataPrefixes) {
      uris.addAll(discoverMetadataUris(prefix, patternsToUse, storageConfig))
    }

    if (uris.isEmpty()) {
      throw IllegalArgumentException("Provide at least one --metadata-uri or --metadata-prefix")
    }

    return uris.toList()
  }

  private suspend fun discoverMetadataUris(
    prefix: String,
    patterns: List<String>,
    storageConfig: StorageConfig,
  ): List<String> {
    val blobUri = SelectedStorageClient.parseBlobUri(prefix)
    val listPrefix = blobUri.key.ifEmpty { null }
    val regexes = patterns.map { Regex(it) }
    val storageClient =
      SelectedStorageClient(blobUri, storageConfig.rootDirectory, storageConfig.projectId)

    val discovered =
      storageClient
        .listBlobs(listPrefix)
        .filter { blob -> regexes.any { it.matches(blob.blobKey) } }
        .map { "${blobUri.scheme}://${blobUri.bucket}/${it.blobKey}" }
        .toList()
        .sorted()

    logger.info("Discovered ${discovered.size} metadata blobs under $prefix matching $patterns")
    return discovered
  }

  private fun printSummary(stats: ImpressionStats) {
    stats.filterStats.forEach { filterStat ->
      val label = if (filterStat.celExpression.isBlank()) "<no CEL filter>" else filterStat.celExpression
      println("Filter: $label")
      println("  Processed ${filterStat.blobStats.size} metadata blobs")
      filterStat.blobStats.forEach { blob ->
        println(
          "  - ${blob.metadataUri} -> ${blob.blobUri}: records=${blob.recordCount}, newDistinctVids=${blob.newDistinctVids}"
        )
      }
      println("  Total records: ${filterStat.totalRecords}")
      println("  Distinct VIDs: ${filterStat.distinctVids}")
      println()
    }
  }

  companion object {
    private val logger = Logger.getLogger(ImpressionStatsCommand::class.java.name)

    private val KNOWN_TYPES =
      ProtoReflection.WELL_KNOWN_TYPES + EventAnnotationsProto.getDescriptor()

    private val EXTENSION_REGISTRY =
      ExtensionRegistry.newInstance()
        .also { EventAnnotationsProto.registerAllExtensions(it) }
        .unmodifiable

    private const val MAX_TIMESTAMP_SECONDS = 253402300799L // 9999-12-31T23:59:59Z
  }

  private fun loadEventMessageDescriptor(): Descriptors.Descriptor {
    val descriptorSets: List<DescriptorProtos.FileDescriptorSet> =
      eventMessageDescriptorSetFiles.map { file ->
        file.inputStream().use { input ->
          DescriptorProtos.FileDescriptorSet.parseFrom(input, EXTENSION_REGISTRY)
        }
      }

    val typeRegistry: TypeRegistry =
      TypeRegistry.newBuilder()
        .add(ProtoReflection.buildDescriptors(descriptorSets, KNOWN_TYPES))
        .build()

    return checkNotNull(typeRegistry.getDescriptorForTypeUrl(eventMessageTypeUrl)) {
      "Event message type not found in descriptor sets"
    }
  }

  private fun parseCollectionIntervalOverride(): Interval? {
    if (collectionStartTime.isNullOrBlank() && collectionEndTime.isNullOrBlank()) {
      return null
    }

    val startInstant = collectionStartTime?.let(Instant::parse) ?: Instant.EPOCH
    val endInstant =
      collectionEndTime?.let(Instant::parse) ?: Instant.ofEpochSecond(MAX_TIMESTAMP_SECONDS)

    require(startInstant.isBefore(endInstant)) {
      "Provided collection interval start must be before end"
    }

    return Interval.newBuilder()
      .setStartTime(
        Timestamp.newBuilder()
          .setSeconds(startInstant.epochSecond)
          .setNanos(startInstant.nano)
          .build()
      )
      .setEndTime(
        Timestamp.newBuilder()
          .setSeconds(endInstant.epochSecond)
          .setNanos(endInstant.nano)
          .build()
      )
      .build()
  }
}

fun main(args: Array<String>) = commandLineMain(ImpressionStatsCommand(), args)
