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

package org.wfanet.measurement.tools

import com.google.crypto.tink.KmsClient
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient
import com.google.protobuf.ByteString
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.TextFormat
import com.google.protobuf.util.JsonFormat
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import java.time.LocalDate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.MappedEventGroup
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.resultsfulfiller.StorageEventReader
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient
import com.google.cloud.storage.StorageOptions
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters
import java.io.File
import java.time.Instant
import kotlin.system.exitProcess

/**
 * EventGroup Manager Tool
 *
 * Manages EventGroup protobuf records in mesos record format (length-delimited).
 * Supports both local files and Google Cloud Storage (gs://) paths.
 */
object StorageHelper {
  /**
   * Create a MesosRecordIoStorageClient for the given path.
   * Supports both local files and GCS (gs://) paths.
   */
  fun createStorageClient(path: String): MesosRecordIoStorageClient {
    val blobUri = SelectedStorageClient.parseBlobUri(path)
    val baseClient = SelectedStorageClient(
      blobUri = blobUri,
      rootDirectory = if (!path.startsWith("gs://")) File(".") else null,
      projectId = null // Uses default credentials
    )
    return MesosRecordIoStorageClient(baseClient)
  }
}

private fun normalizeToBlobUri(path: String): String {
  val httpsPrefix = "https://storage.googleapis.com/"
  val httpPrefix = "http://storage.googleapis.com/"
  return when {
    path.startsWith(httpsPrefix) -> "gs://${path.removePrefix(httpsPrefix)}"
    path.startsWith(httpPrefix) -> "gs://${path.removePrefix(httpPrefix)}"
    else -> path
  }
}

private fun parseBlobDetailsBytes(data: ByteString): BlobDetails {
  val utf8 = data.toStringUtf8().trimStart()
  if (utf8.startsWith("{")) {
    val builder = BlobDetails.newBuilder()
    JsonFormat.parser().merge(utf8, builder)
    return builder.build()
  }

  return try {
    BlobDetails.parseFrom(data)
  } catch (_: InvalidProtocolBufferException) {
    // Fallback to JSON parsing if binary parsing fails.
    val builder = BlobDetails.newBuilder()
    JsonFormat.parser().merge(utf8, builder)
    builder.build()
  }
}

@Command(
  name = "event_group_manager",
  description = ["Manages EventGroup protobuf records in mesos record format. Supports local files and GCS (gs://) paths."],
  mixinStandardHelpOptions = true,
  descriptionHeading = "%nDescription:%n",
  headerHeading = "",
  optionListHeading = "%nOptions:%n",
  commandListHeading = "%nCommands:%n",
  footerHeading = "%nExamples:%n",
  footer = [
    "bazel run //tools:EventGroupManager -- print /tmp/event_groups",
    "bazel run //tools:EventGroupManager -- count gs://my-bucket/path/to/event-groups",
    "bazel run //tools:EventGroupManager -- append /tmp/event_groups --json '{\"eventGroupReferenceId\":\"eg-1\",\"measurementConsumer\":\"measurementConsumers/123\",\"mediaTypes\":[\"OTHER\"],\"eventGroupMetadata\":{\"adMetadata\":{\"campaignMetadata\":{\"brand\":\"brand\",\"campaign\":\"campaign\"}}},\"dataAvailabilityInterval\":{\"startTime\":{\"seconds\":1}}}'",
    "bazel run //tools:EventGroupManager -- append-dummy /tmp/event_groups -n 3 --reference-prefix dummy-eg --measurement-consumer measurementConsumers/test",
    "bazel run //tools:EventGroupManager -- sample /tmp/event_groups",
    "bazel run //tools:EventGroupManager -- filter /tmp/event_groups --keep-first 10",
    "bazel run //tools:EventGroupManager -- remove /tmp/event_groups --reference-id eg-123",
    "bazel run //tools:EventGroupManager -- list-impressions --bucket my-bucket --date 2024-10-01",
    "bazel run //tools:EventGroupManager -- mark-done --start-date 2024-10-01 --end-date 2024-10-07 --model-line model-a"
  ],
  subcommands = [
    CommandLine.HelpCommand::class,
    PrintCommand::class,
    PrintMapCommand::class,
    CountCommand::class,
    AppendCommand::class,
    AppendDummyCommand::class,
    AppendMapCommand::class,
    SampleCommand::class,
    FilterCommand::class,
    RemoveCommand::class,
    ListImpressionsCommand::class,
    ReadImpressionMetadataCommand::class,
    CheckImpressionsCommand::class,
    CheckImpressionsRangeCommand::class,
    MarkDoneCommand::class,
  ],
)
class EventGroupManager : Runnable {
  override fun run() {
    // No-op. See subcommands.
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(EventGroupManager(), args)
  }
}

@Command(name = "print", description = ["Print all records from a file (local or gs://)"])
class PrintCommand : Runnable {
  @Parameters(index = "0", description = ["Path to the record file (supports gs://)"])
  private lateinit var file: String

  @Option(names = ["-f", "--format"], description = ["Output format: text or json"], defaultValue = "text")
  private var format: String = "text"

  override fun run() = runBlocking {
    val storageClient = StorageHelper.createStorageClient(file)
    val blobUri = SelectedStorageClient.parseBlobUri(file)

    val blob = storageClient.getBlob(blobUri.key)
      ?: run {
        System.err.println("File not found: $file")
        exitProcess(1)
      }

    val records = blob.read().map { bytes -> EventGroup.parseFrom(bytes) }.toList()

    val jsonPrinter = JsonFormat.printer()
      .preservingProtoFieldNames()
      .includingDefaultValueFields()

    var count = 0
    for (record in records) {
      count++
      println("--- Record $count ---")

      when (format) {
        "json" -> println(jsonPrinter.print(record))
        else -> println(TextFormat.printer().printToString(record))
      }
      println()
    }

    if (count == 0) {
      println("No records found in file.")
    } else {
      println("Total records: $count")
    }
  }
}

@Command(name = "print-map", description = ["Print MappedEventGroup records from a file (local or gs://)"])
class PrintMapCommand : Runnable {
  @Parameters(index = "0", description = ["Path to the MappedEventGroup record file (supports gs://)"])
  private lateinit var file: String

  @Option(names = ["-f", "--format"], description = ["Output format: text or json"], defaultValue = "text")
  private var format: String = "text"

  override fun run() = runBlocking {
    val storageClient = StorageHelper.createStorageClient(file)
    val blobUri = SelectedStorageClient.parseBlobUri(file)

    val blob = storageClient.getBlob(blobUri.key)
      ?: run {
        System.err.println("File not found: $file")
        exitProcess(1)
      }

    val records = blob.read().map { bytes -> MappedEventGroup.parseFrom(bytes) }.toList()

    val jsonPrinter = JsonFormat.printer()
      .preservingProtoFieldNames()
      .includingDefaultValueFields()

    var count = 0
    for (record in records) {
      count++
      println("--- Mapped Record $count ---")

      when (format) {
        "json" -> println(jsonPrinter.print(record))
        else -> println(TextFormat.printer().printToString(record))
      }
      println()
    }

    if (count == 0) {
      println("No records found in file.")
    } else {
      println("Total mapped records: $count")
    }
  }
}

@Command(name = "count", description = ["Count records in a file (local or gs://)"])
class CountCommand : Runnable {
  @Parameters(index = "0", description = ["Path to the record file (supports gs://)"])
  private lateinit var file: String

  override fun run() = runBlocking {
    val storageClient = StorageHelper.createStorageClient(file)
    val blobUri = SelectedStorageClient.parseBlobUri(file)

    val blob = storageClient.getBlob(blobUri.key)
      ?: run {
        System.err.println("File not found: $file")
        exitProcess(1)
      }

    val count = blob.read().toList().size

    println("Total records: $count")
  }
}

@Command(name = "append", description = ["Append a new record to a file (local or gs://)"])
class AppendCommand : Runnable {
  @Parameters(index = "0", description = ["Path to the record file (supports gs://)"])
  private lateinit var file: String

  @Option(names = ["--json"], description = ["EventGroup as JSON string"])
  private var jsonData: String? = null

  @Option(names = ["--textproto"], description = ["EventGroup as text proto string"])
  private var textprotoData: String? = null

  override fun run() = runBlocking {
    val builder = EventGroup.newBuilder()

    when {
      jsonData != null -> JsonFormat.parser().merge(jsonData, builder)
      textprotoData != null -> TextFormat.Parser.newBuilder().build().merge(textprotoData, builder)
      else -> {
        System.err.println("Error: Must provide either --json or --textproto")
        exitProcess(1)
      }
    }

    val record = builder.build()

    // Validate required fields
    if (record.eventGroupReferenceId.isEmpty()) {
      System.err.println("Warning: 'event_group_reference_id' field is required but not set")
    }
    if (record.measurementConsumer.isEmpty()) {
      System.err.println("Warning: 'measurement_consumer' field is required but not set")
    }

    val storageClient = StorageHelper.createStorageClient(file)
    val blobUri = SelectedStorageClient.parseBlobUri(file)

    // Read existing records
    val existingRecords = storageClient.getBlob(blobUri.key)?.let { blob ->
      blob.read().map { bytes -> EventGroup.parseFrom(bytes) }.toList()
    } ?: emptyList()

    // Append new record and write back
    val updatedRecords = existingRecords + record
    storageClient.writeBlob(blobUri.key, updatedRecords.map { it.toByteString() }.asFlow())

    println("Record appended successfully to $file")
  }
}

@Command(name = "append-dummy", description = ["Append dummy EventGroup records to a file (local or gs://)"])
class AppendDummyCommand : Runnable {
  @Parameters(index = "0", description = ["Path to the record file (supports gs://)"])
  private lateinit var file: String

  @Option(names = ["-n", "--count"], description = ["Number of dummy records to append"], defaultValue = "1")
  private var count: Int = 1

  @Option(names = ["--measurement-consumer"], description = ["Measurement consumer resource name to use"], defaultValue = "measurementConsumers/dummy")
  private var measurementConsumer: String = "measurementConsumers/dummy"

  @Option(names = ["--reference-prefix"], description = ["Prefix for generated event_group_reference_id values"], defaultValue = "dummy-event-group")
  private var referencePrefix: String = "dummy-event-group"

  @Option(names = ["--start-index"], description = ["Starting index for generated reference IDs"], defaultValue = "1")
  private var startIndex: Int = 1

  override fun run() = runBlocking {
    if (count <= 0) {
      System.err.println("Error: count must be positive")
      exitProcess(1)
    }

    val storageClient = StorageHelper.createStorageClient(file)
    val blobUri = SelectedStorageClient.parseBlobUri(file)

    val existingRecords = storageClient.getBlob(blobUri.key)?.let { blob ->
      blob.read().map { bytes -> EventGroup.parseFrom(bytes) }.toList()
    } ?: emptyList()

    val baseSeconds = Instant.now().epochSecond
    val dummyRecords = (0 until count).map { index ->
      val suffix = startIndex + index
      EventGroup.newBuilder().apply {
        eventGroupReferenceId = "$referencePrefix-$suffix"
        measurementConsumer = measurementConsumer
        addMediaTypes(EventGroup.MediaType.OTHER)
        eventGroupMetadataBuilder.apply {
          adMetadataBuilder.apply {
            campaignMetadataBuilder.apply {
              brand = "dummy-brand"
              campaign = "dummy-campaign-$suffix"
            }
          }
        }
        dataAvailabilityIntervalBuilder.apply {
          startTimeBuilder.seconds = baseSeconds
          endTimeBuilder.seconds = baseSeconds + 86_400
        }
      }.build()
    }

    val updatedRecords = existingRecords + dummyRecords
    storageClient.writeBlob(blobUri.key, updatedRecords.map { it.toByteString() }.asFlow())

    println("Appended ${dummyRecords.size} dummy record(s) to $file")
  }
}

@Command(name = "append-map", description = ["Append a MappedEventGroup record to a file (local or gs://)"])
class AppendMapCommand : Runnable {
  @Parameters(index = "0", description = ["Path to the MappedEventGroup record file (supports gs://)"])
  private lateinit var file: String

  @Option(names = ["--json"], description = ["MappedEventGroup as JSON string"])
  private var jsonData: String? = null

  @Option(names = ["--textproto"], description = ["MappedEventGroup as text proto string"])
  private var textprotoData: String? = null

  override fun run() = runBlocking {
    val builder = MappedEventGroup.newBuilder()

    when {
      jsonData != null -> JsonFormat.parser().merge(jsonData, builder)
      textprotoData != null -> TextFormat.Parser.newBuilder().build().merge(textprotoData, builder)
      else -> {
        System.err.println("Error: Must provide either --json or --textproto")
        exitProcess(1)
      }
    }

    val record = builder.build()

    // Validate required fields
    if (record.eventGroupReferenceId.isEmpty()) {
      System.err.println("Warning: 'event_group_reference_id' field is required but not set")
    }
    if (record.eventGroupResource.isEmpty()) {
      System.err.println("Warning: 'event_group_resource' field is required but not set")
    }

    val storageClient = StorageHelper.createStorageClient(file)
    val blobUri = SelectedStorageClient.parseBlobUri(file)

    // Read existing records
    val existingRecords = storageClient.getBlob(blobUri.key)?.let { blob ->
      blob.read().map { bytes -> MappedEventGroup.parseFrom(bytes) }.toList()
    } ?: emptyList()

    // Append new record and write back
    val updatedRecords = existingRecords + record
    storageClient.writeBlob(blobUri.key, updatedRecords.map { it.toByteString() }.asFlow())

    println("Mapped record appended successfully to $file")
  }
}

@Command(name = "sample", description = ["Create and append a sample record (local or gs://)"])
class SampleCommand : Runnable {
  @Parameters(index = "0", description = ["Path to the record file (supports gs://)"])
  private lateinit var file: String

  override fun run() = runBlocking {
    val record = EventGroup.newBuilder().apply {
      eventGroupReferenceId = "sample-campaign-001"
      measurementConsumer = "measurementConsumers/789"

      // Add media types
      addMediaTypes(EventGroup.MediaType.VIDEO)

      // Add event group metadata
      eventGroupMetadataBuilder.apply {
        adMetadataBuilder.apply {
          campaignMetadataBuilder.apply {
            brand = "Example Brand"
            campaign = "Example Campaign"
          }
        }
      }

      // Add data availability interval
      dataAvailabilityIntervalBuilder.apply {
        startTimeBuilder.seconds = 1700000000
      }
    }.build()

    val storageClient = StorageHelper.createStorageClient(file)
    val blobUri = SelectedStorageClient.parseBlobUri(file)

    // Read existing records
    val existingRecords = storageClient.getBlob(blobUri.key)?.let { blob ->
      blob.read().map { bytes -> EventGroup.parseFrom(bytes) }.toList()
    } ?: emptyList()

    // Append new record and write back
    val updatedRecords = existingRecords + record
    storageClient.writeBlob(blobUri.key, updatedRecords.map { it.toByteString() }.asFlow())

    println("Sample record appended to $file")
    println("\nRecord content:")
    println(TextFormat.printer().printToString(record))
  }
}

@Command(name = "filter", description = ["Filter records in a file by criteria and overwrite (local or gs://)"])
class FilterCommand : Runnable {
  @Parameters(index = "0", description = ["Path to the record file (supports gs://)"])
  private lateinit var file: String

  @Option(names = ["--keep-first"], description = ["Keep only the first N records"])
  private var keepFirst: Int? = null

  @Option(names = ["--keep-last"], description = ["Keep only the last N records"])
  private var keepLast: Int? = null

  @Option(names = ["--reference-id"], description = ["Keep only records with this event_group_reference_id"])
  private var referenceId: String? = null

  @Option(names = ["--measurement-consumer"], description = ["Keep only records with this measurement_consumer"])
  private var measurementConsumer: String? = null

  @Option(names = ["--dry-run"], description = ["Show what would be kept without writing"])
  private var dryRun: Boolean = false

  override fun run() = runBlocking {
    val storageClient = StorageHelper.createStorageClient(file)
    val blobUri = SelectedStorageClient.parseBlobUri(file)

    val blob = storageClient.getBlob(blobUri.key)
      ?: run {
        System.err.println("File not found: $file")
        exitProcess(1)
      }

    // Read all records
    val allRecords = blob.read().map { bytes -> EventGroup.parseFrom(bytes) }.toList()
    println("Found ${allRecords.size} total records")

    // Apply filters
    var filteredRecords = allRecords

    if (keepFirst != null) {
      filteredRecords = filteredRecords.take(keepFirst!!)
      println("Keeping first $keepFirst record(s)")
    }

    if (keepLast != null) {
      filteredRecords = filteredRecords.takeLast(keepLast!!)
      println("Keeping last $keepLast record(s)")
    }

    if (referenceId != null) {
      filteredRecords = filteredRecords.filter { it.eventGroupReferenceId == referenceId }
      println("Filtering by reference_id: $referenceId")
    }

    if (measurementConsumer != null) {
      filteredRecords = filteredRecords.filter { it.measurementConsumer == measurementConsumer }
      println("Filtering by measurement_consumer: $measurementConsumer")
    }

    // Check if any filters were applied
    if (keepFirst == null && keepLast == null && referenceId == null && measurementConsumer == null) {
      System.err.println("Error: Must specify at least one filter option (--keep-first, --keep-last, --reference-id, or --measurement-consumer)")
      exitProcess(1)
    }

    println("\nRecords after filtering: ${filteredRecords.size}")

    // Print what will be kept
    println("\nRecords that will be kept:")
    filteredRecords.forEachIndexed { index, record ->
      println("  ${index + 1}. ${record.eventGroupReferenceId} (consumer: ${record.measurementConsumer})")
    }

    if (dryRun) {
      println("\n[DRY RUN] Would have written ${filteredRecords.size} record(s) to $file")
      return@runBlocking
    }

    // Write filtered records back
    if (filteredRecords.isEmpty()) {
      System.err.println("\nWarning: No records match the filter criteria. File would be empty.")
      System.err.println("Aborting to prevent data loss. Use --dry-run to preview.")
      exitProcess(1)
    }

    storageClient.writeBlob(blobUri.key, filteredRecords.map { it.toByteString() }.asFlow())
    println("\nFile overwritten successfully with ${filteredRecords.size} record(s)")
  }
}

@Command(name = "remove", description = ["Remove records from a file by criteria and overwrite (local or gs://)"])
class RemoveCommand : Runnable {
  @Parameters(index = "0", description = ["Path to the record file (supports gs://)"])
  private lateinit var file: String

  @Option(names = ["--all"], description = ["Remove all records (creates empty file)"])
  private var removeAll: Boolean = false

  @Option(names = ["--first"], description = ["Remove the first N records"])
  private var removeFirst: Int? = null

  @Option(names = ["--last"], description = ["Remove the last N records"])
  private var removeLast: Int? = null

  @Option(names = ["--index"], description = ["Remove record at specific index (1-based)"])
  private var removeIndex: Int? = null

  @Option(names = ["--reference-id"], description = ["Remove records with this event_group_reference_id"])
  private var referenceId: String? = null

  @Option(names = ["--measurement-consumer"], description = ["Remove records with this measurement_consumer"])
  private var measurementConsumer: String? = null

  @Option(names = ["--dry-run"], description = ["Show what would be removed without writing"])
  private var dryRun: Boolean = false

  override fun run() = runBlocking {
    val storageClient = StorageHelper.createStorageClient(file)
    val blobUri = SelectedStorageClient.parseBlobUri(file)

    val blob = storageClient.getBlob(blobUri.key)
      ?: run {
        System.err.println("File not found: $file")
        exitProcess(1)
      }

    // Read all records
    val allRecords = blob.read().map { bytes -> EventGroup.parseFrom(bytes) }.toList()
    println("Found ${allRecords.size} total records")

    // Check if any removal criteria were specified
    if (!removeAll && removeFirst == null && removeLast == null && removeIndex == null &&
        referenceId == null && measurementConsumer == null) {
      System.err.println("Error: Must specify at least one removal option (--all, --first, --last, --index, --reference-id, or --measurement-consumer)")
      exitProcess(1)
    }

    // Track which records to remove
    val indicesToRemove = mutableSetOf<Int>()

    // Apply removal criteria
    if (removeAll) {
      indicesToRemove.addAll(allRecords.indices)
      println("Removing all records")
    }

    if (removeFirst != null) {
      val count = removeFirst!!.coerceAtMost(allRecords.size)
      indicesToRemove.addAll(0 until count)
      println("Removing first $count record(s)")
    }

    if (removeLast != null) {
      val count = removeLast!!.coerceAtMost(allRecords.size)
      val startIndex = (allRecords.size - count).coerceAtLeast(0)
      indicesToRemove.addAll(startIndex until allRecords.size)
      println("Removing last $count record(s)")
    }

    if (removeIndex != null) {
      val index = removeIndex!! - 1 // Convert to 0-based
      if (index in allRecords.indices) {
        indicesToRemove.add(index)
        println("Removing record at index $removeIndex")
      } else {
        System.err.println("Warning: Index $removeIndex is out of bounds (valid range: 1-${allRecords.size})")
      }
    }

    if (referenceId != null) {
      allRecords.forEachIndexed { index, record ->
        if (record.eventGroupReferenceId == referenceId) {
          indicesToRemove.add(index)
        }
      }
      println("Removing records with reference_id: $referenceId")
    }

    if (measurementConsumer != null) {
      allRecords.forEachIndexed { index, record ->
        if (record.measurementConsumer == measurementConsumer) {
          indicesToRemove.add(index)
        }
      }
      println("Removing records with measurement_consumer: $measurementConsumer")
    }

    // Create filtered list (keeping records NOT in the removal set)
    val remainingRecords = allRecords.filterIndexed { index, _ -> index !in indicesToRemove }

    println("\nRecords to be removed: ${indicesToRemove.size}")
    println("Records that will remain: ${remainingRecords.size}")

    // Print records that will be removed
    if (indicesToRemove.isNotEmpty()) {
      println("\nRecords that will be removed:")
      indicesToRemove.sorted().forEach { index ->
        val record = allRecords[index]
        println("  ${index + 1}. ${record.eventGroupReferenceId} (consumer: ${record.measurementConsumer})")
      }
    }

    if (dryRun) {
      println("\n[DRY RUN] Would have written ${remainingRecords.size} record(s) to $file")
      return@runBlocking
    }

    // Write remaining records back
    if (remainingRecords.isEmpty() && !removeAll) {
      System.err.println("\nWarning: All records would be removed. File would be empty.")
      System.err.println("Aborting to prevent data loss. Use --all to explicitly remove all records, or --dry-run to preview.")
      exitProcess(1)
    }

    storageClient.writeBlob(blobUri.key, remainingRecords.map { it.toByteString() }.asFlow())

    if (remainingRecords.isEmpty()) {
      println("\nFile overwritten successfully. All ${indicesToRemove.size} record(s) removed. File is now empty.")
    } else {
      println("\nFile overwritten successfully. Removed ${indicesToRemove.size} record(s), ${remainingRecords.size} record(s) remain")
    }
  }
}

@Command(name = "read-impression-metadata", description = ["Read and print BlobDetails (impression metadata) from a single binary protobuf file (local or gs://)"])
class ReadImpressionMetadataCommand : Runnable {
  @Parameters(index = "0", description = ["Path to the BlobDetails metadata file (supports gs://)"])
  private lateinit var file: String

  @Option(names = ["-f", "--format"], description = ["Output format: text or json"], defaultValue = "text")
  private var format: String = "text"

  override fun run() = runBlocking {
    val blobUri = SelectedStorageClient.parseBlobUri(file)
    val baseClient = SelectedStorageClient(
      blobUri = blobUri,
      rootDirectory = if (!file.startsWith("gs://")) File(".") else null,
      projectId = null // Uses default credentials
    )

    val blob = baseClient.getBlob(blobUri.key)
      ?: run {
        System.err.println("File not found: $file")
        exitProcess(1)
      }

    // Read entire file as single binary protobuf
    val bytes = blob.read().toList()
    if (bytes.isEmpty()) {
      println("File is empty")
      return@runBlocking
    }

    // Concatenate all chunks into single ByteString
    val data = bytes.fold(ByteString.EMPTY) { acc, chunk -> acc.concat(chunk) }
    val blobDetails = BlobDetails.parseFrom(data)

    val jsonPrinter = JsonFormat.printer()
      .preservingProtoFieldNames()
      .includingDefaultValueFields()

    println("--- BlobDetails ---")
    when (format) {
      "json" -> println(jsonPrinter.print(blobDetails))
      else -> println(TextFormat.printer().printToString(blobDetails))
    }
  }
}

@Command(
  name = "check-impressions",
  description = [
    "Read impression data referenced by a BlobDetails metadata file, decrypt it if needed, and fail if any vid is zero."
  ],
)
class CheckImpressionsCommand : Runnable {
  @Parameters(index = "0", description = ["Path to the BlobDetails metadata file (supports gs://)"])
  private lateinit var metadataFile: String

  @Option(
    names = ["--impressions-root"],
    description = ["Local root directory for resolving file:/// impression blobs (ignored for gs://)"],
  )
  private var impressionsRoot: File? = null

  @Option(names = ["--project-id"], description = ["GCP project ID to use for storage"], defaultValue = "")
  private var projectId: String = ""

  @Option(
    names = ["--kms"],
    description = ["KMS mode: gcp (default) or none for unencrypted impression blobs"],
    defaultValue = "gcp",
  )
  private var kmsMode: String = "gcp"

  @Option(
    names = ["--max-zero-samples"],
    description = ["Number of zero-vid sample entries to print"],
    defaultValue = "5",
  )
  private var maxZeroSamples: Int = 5

  override fun run() = runBlocking {
    val blobDetails = loadBlobDetails(metadataFile, projectId.ifEmpty { null })

    val kmsClient: KmsClient? =
      when (kmsMode.lowercase()) {
        "none" -> null
        "gcp", "auto" -> GcpKmsClient().withDefaultCredentials()
        else -> {
          System.err.println("Unsupported --kms value '$kmsMode'. Use 'gcp' or 'none'.")
          exitProcess(1)
        }
      }

    val rootDirForImpressions =
      if (blobDetails.blobUri.startsWith("gs://")) {
        null
      } else {
        impressionsRoot ?: File(".")
      }

    val storageConfig =
      StorageConfig(
        rootDirectory = rootDirForImpressions,
        projectId = projectId.ifEmpty { null },
      )

    val reader =
      StorageEventReader(
        blobDetails = blobDetails,
        kmsClient = kmsClient,
        impressionsStorageConfig = storageConfig,
        descriptor = TestEvent.getDescriptor(),
      )

    var totalRecords = 0L
    var zeroVidCount = 0L
    val zeroVidSamples = mutableListOf<String>()

    try {
      reader.readEvents().collect { batch ->
        batch.forEach { event ->
          totalRecords++
          if (event.vid == 0L) {
            zeroVidCount++
            if (zeroVidSamples.size < maxZeroSamples) {
              zeroVidSamples.add("timestamp=${event.timestamp}")
            }
          }
        }
      }
    } catch (e: Exception) {
      System.err.println("Failed to read impressions: ${e.message}")
      e.printStackTrace()
      exitProcess(1)
    }

    println("Checked $totalRecords impressions from ${blobDetails.blobUri}")
    if (zeroVidCount > 0) {
      println("Found $zeroVidCount impression(s) with vid == 0")
      if (zeroVidSamples.isNotEmpty()) {
        println("Sample zero-vid entries:")
        zeroVidSamples.forEach { println("  $it") }
      }
      exitProcess(2)
    } else {
      println("No impressions with vid == 0 detected.")
    }
  }

  private suspend fun loadBlobDetails(path: String, projectId: String?): BlobDetails {
    val normalizedPath = normalizeToBlobUri(path)
    val blobUri = SelectedStorageClient.parseBlobUri(normalizedPath)
    val baseClient =
      SelectedStorageClient(
        blobUri = blobUri,
        rootDirectory = if (!normalizedPath.startsWith("gs://")) File(".") else null,
        projectId = projectId,
      )

    val blob = baseClient.getBlob(blobUri.key)
      ?: run {
        System.err.println("File not found: $normalizedPath")
        exitProcess(1)
      }

    val bytes = blob.read().toList()
    if (bytes.isEmpty()) {
      System.err.println("Metadata file is empty: $normalizedPath")
      exitProcess(1)
    }

    val data = bytes.fold(ByteString.EMPTY) { acc, chunk -> acc.concat(chunk) }
    return parseBlobDetailsBytes(data)
  }
}

@Command(
  name = "check-impressions-range",
  description = [
    "Check impressions across a date range using a metadata path template."
  ],
)
class CheckImpressionsRangeCommand : Runnable {
  @Option(names = ["--start-date"], required = true, description = ["Start date (YYYY-MM-DD)"])
  private lateinit var startDate: String

  @Option(names = ["--end-date"], required = true, description = ["End date (YYYY-MM-DD)"])
  private lateinit var endDate: String

  @Option(
    names = ["--bucket"],
    description = ["GCS bucket name"],
    defaultValue = "secure-computation-storage-dev-bucket",
  )
  private var bucket: String = "secure-computation-storage-dev-bucket"

  @Option(
    names = ["--base-path"],
    description = ["Base path in bucket before {date}"],
    defaultValue = "edp/edpa_meta",
  )
  private var basePath: String = "edp/edpa_meta"

  @Option(
    names = ["--metadata-template"],
    description = [
      "Template for metadata path with {date} and {campaignId} placeholders if desired. ",
      "Example: gs://bucket/edp/edpa_meta/{date}/metadata_campaign_{campaignId}.json"
    ],
    defaultValue = "",
  )
  private var metadataTemplate: String = ""

  @Option(names = ["--campaign-id"], description = ["Campaign ID to substitute into default template"], defaultValue = "")
  private var campaignId: String = ""

  @Option(
    names = ["--metadata-file-template"],
    description = [
      "Filename template placed after the date when not using --metadata-template. Supports {campaignId} and {date}.",
      "Example: metadata_campaign_{campaignId}.json"
    ],
    defaultValue = "metadata_campaign_{campaignId}.json",
  )
  private var metadataFileTemplate: String = "metadata_campaign_{campaignId}.json"

  @Option(
    names = ["--impressions-root"],
    description = ["Local root directory for resolving file:/// impression blobs (ignored for gs://)"],
  )
  private var impressionsRoot: File? = null

  @Option(names = ["--project-id"], description = ["GCP project ID to use for storage"], defaultValue = "")
  private var projectId: String = ""

  @Option(
    names = ["--kms"],
    description = ["KMS mode: gcp (default) or none for unencrypted impression blobs"],
    defaultValue = "gcp",
  )
  private var kmsMode: String = "gcp"

  @Option(
    names = ["--max-zero-samples"],
    description = ["Number of zero-vid sample entries to print per date"],
    defaultValue = "5",
  )
  private var maxZeroSamples: Int = 5

  override fun run() = runBlocking {
    val start = LocalDate.parse(startDate)
    val end = LocalDate.parse(endDate)
    require(!start.isAfter(end)) { "start-date must be on or before end-date" }

    val kmsClient: KmsClient? =
      when (kmsMode.lowercase()) {
        "none" -> null
        "gcp", "auto" -> GcpKmsClient().withDefaultCredentials()
        else -> {
          System.err.println("Unsupported --kms value '$kmsMode'. Use 'gcp' or 'none'.")
          exitProcess(1)
        }
      }

    var overallRecords = 0L
    var overallZero = 0L
    var current = start

    while (!current.isAfter(end)) {
      val metadataPath =
        metadataTemplate.takeIf { it.isNotBlank() }?.let { tmpl ->
          tmpl
            .replace("{date}", current.toString())
            .replace("{campaignId}", campaignId)
        }
          ?: run {
            val renderedFile =
              metadataFileTemplate
                .replace("{date}", current.toString())
                .replace("{campaignId}", campaignId)

            if (renderedFile.contains("{campaignId}") && campaignId.isEmpty()) {
              System.err.println("campaign-id is required when the filename template includes {campaignId}.")
              exitProcess(1)
            }

            "gs://$bucket/$basePath/$current/$renderedFile"
          }

      val (totalRecords, zeroCount, samples) =
        checkSingleMetadata(
          metadataPath = metadataPath,
          kmsClient = kmsClient,
          rootDirectory = impressionsRoot,
          projectId = projectId.ifEmpty { null },
          maxZeroSamples = maxZeroSamples,
        )

      overallRecords += totalRecords
      overallZero += zeroCount

      println("[$current] Checked $totalRecords impressions from $metadataPath")
      if (zeroCount > 0) {
        println("[$current] Found $zeroCount impression(s) with vid == 0")
        if (samples.isNotEmpty()) {
          println("[$current] Sample zero-vid entries:")
          samples.forEach { println("  $it") }
        }
      } else {
        println("[$current] No impressions with vid == 0 detected.")
      }

      current = current.plusDays(1)
    }

    println("---- Summary ----")
    println("Dates: $startDate to $endDate")
    println("Total impressions checked: $overallRecords")
    if (overallZero > 0) {
      println("Total zero-vid impressions: $overallZero")
      exitProcess(2)
    } else {
      println("No zero-vid impressions detected across the range.")
    }
  }

  private suspend fun checkSingleMetadata(
    metadataPath: String,
    kmsClient: KmsClient?,
    rootDirectory: File?,
    projectId: String?,
    maxZeroSamples: Int,
  ): Triple<Long, Long, List<String>> {
    val blobDetails = loadBlobDetails(metadataPath, projectId, rootDirectory)

    val storageConfig =
      StorageConfig(
        rootDirectory =
          if (blobDetails.blobUri.startsWith("gs://")) {
            null
          } else {
            rootDirectory ?: File(".")
          },
        projectId = projectId,
      )

    val reader =
      StorageEventReader(
        blobDetails = blobDetails,
        kmsClient = kmsClient,
        impressionsStorageConfig = storageConfig,
        descriptor = TestEvent.getDescriptor(),
      )

    var totalRecords = 0L
    var zeroVidCount = 0L
    val zeroVidSamples = mutableListOf<String>()

    reader.readEvents().collect { batch ->
      batch.forEach { event ->
        totalRecords++
        if (event.vid == 0L) {
          zeroVidCount++
          if (zeroVidSamples.size < maxZeroSamples) {
            zeroVidSamples.add("timestamp=${event.timestamp}")
          }
        }
      }
    }

    return Triple(totalRecords, zeroVidCount, zeroVidSamples)
  }

  private suspend fun loadBlobDetails(
    path: String,
    projectId: String?,
    rootDirectory: File?,
  ): BlobDetails {
    val normalizedPath = normalizeToBlobUri(path)
    val blobUri = SelectedStorageClient.parseBlobUri(normalizedPath)
    val baseClient =
      SelectedStorageClient(
        blobUri = blobUri,
        rootDirectory =
          rootDirectory
            ?: if (!normalizedPath.startsWith("gs://")) {
              File(".")
            } else {
              null
            },
        projectId = projectId,
      )

    val blob = baseClient.getBlob(blobUri.key)
      ?: run {
        System.err.println("Metadata file not found: $normalizedPath")
        exitProcess(1)
      }

    val bytes = blob.read().toList()
    if (bytes.isEmpty()) {
      System.err.println("Metadata file is empty: $normalizedPath")
      exitProcess(1)
    }

    val data = bytes.fold(ByteString.EMPTY) { acc, chunk -> acc.concat(chunk) }
    return parseBlobDetailsBytes(data)
  }
}

@Command(name = "list-impressions", description = ["List impression data files in GCS bucket"])
class ListImpressionsCommand : Runnable {
  @Option(names = ["--bucket"], description = ["GCS bucket name"], defaultValue = "secure-computation-storage-dev-bucket")
  private var bucket: String = "secure-computation-storage-dev-bucket"

  @Option(names = ["--base-path"], description = ["Base path in bucket"], defaultValue = "ds")
  private var basePath: String = "ds"

  @Option(names = ["--date"], description = ["Filter by specific date (YYYY-MM-DD)"])
  private var date: String? = null

  @Option(names = ["--event-group-reference-id"], description = ["Filter by event group reference ID"])
  private var eventGroupReferenceId: String? = null

  override fun run() = runBlocking {
    val storage = StorageOptions.getDefaultInstance().service
    val gcsClient = GcsStorageClient(storage, bucket)

    // List all date folders
    val datePrefix = "$basePath/"
    val dateFolders = storage.list(bucket,
      com.google.cloud.storage.Storage.BlobListOption.prefix(datePrefix),
      com.google.cloud.storage.Storage.BlobListOption.currentDirectory()
    ).iterateAll().mapNotNull { blob ->
      val name = blob.name.removePrefix(datePrefix).removeSuffix("/")
      if (name.matches(Regex("\\d{4}-\\d{2}-\\d{2}"))) name else null
    }.filter { dateFolder ->
      date == null || dateFolder == date
    }.sorted()

    println("Found ${dateFolders.size} date folder(s)")
    println()

    for (dateFolder in dateFolders) {
      // List event group reference ID folders for this date
      val eventGroupPrefix = "$basePath/$dateFolder/event-group-reference-id/"
      val eventGroupFolders = storage.list(bucket,
        com.google.cloud.storage.Storage.BlobListOption.prefix(eventGroupPrefix),
        com.google.cloud.storage.Storage.BlobListOption.currentDirectory()
      ).iterateAll().mapNotNull { blob ->
        val name = blob.name.removePrefix(eventGroupPrefix).removeSuffix("/")
        if (name.isNotEmpty()) name else null
      }.filter { egRefId ->
        eventGroupReferenceId == null || egRefId.contains(eventGroupReferenceId!!)
      }.sorted()

      if (eventGroupFolders.isEmpty()) continue

      println("Date: $dateFolder")
      for (egFolder in eventGroupFolders) {
        val impressionPath = "$eventGroupPrefix$egFolder/impressions"
        val metadataPath = "$eventGroupPrefix$egFolder/metadata"

        val impressionBlob = storage.get(bucket, impressionPath)
        val metadataBlob = storage.get(bucket, metadataPath)

        val impressionSize = impressionBlob?.size ?: 0
        val metadataSize = metadataBlob?.size ?: 0

        println("  Event Group: $egFolder")
        println("    Impressions: gs://$bucket/$impressionPath (${formatSize(impressionSize)})")
        println("    Metadata:    gs://$bucket/$metadataPath (${formatSize(metadataSize)})")
      }
      println()
    }
  }

  private fun formatSize(bytes: Long): String {
    return when {
      bytes < 1024 -> "$bytes B"
      bytes < 1024 * 1024 -> "${bytes / 1024} KB"
      bytes < 1024 * 1024 * 1024 -> "${bytes / (1024 * 1024)} MB"
      else -> "${bytes / (1024 * 1024 * 1024)} GB"
    }
  }
}

@Command(name = "mark-done", description = ["Create or update 'done' marker files in model-line folders for a date range"])
class MarkDoneCommand : Runnable {
  @Option(names = ["--bucket"], description = ["GCS bucket name"], defaultValue = "secure-computation-storage-dev-bucket")
  private var bucket: String = "secure-computation-storage-dev-bucket"

  @Option(names = ["--base-path"], description = ["Base path in bucket"], defaultValue = "ds")
  private var basePath: String = "ds"

  @Option(names = ["--start-date"], description = ["Start date (YYYY-MM-DD)"], required = true)
  private lateinit var startDate: String

  @Option(names = ["--end-date"], description = ["End date (YYYY-MM-DD)"], required = true)
  private lateinit var endDate: String

  @Option(names = ["--model-line"], description = ["Model line name"], defaultValue = "some-model-line")
  private var modelLine: String = "some-model-line"

  @Option(names = ["--dry-run"], description = ["Show what would be created without writing"])
  private var dryRun: Boolean = false

  override fun run() = runBlocking {
    val storage = StorageOptions.getDefaultInstance().service

    // Parse dates and generate date range
    val start = java.time.LocalDate.parse(startDate)
    val end = java.time.LocalDate.parse(endDate)

    if (start.isAfter(end)) {
      System.err.println("Error: start-date must be before or equal to end-date")
      exitProcess(1)
    }

    val dates = mutableListOf<String>()
    var currentDate = start
    while (!currentDate.isAfter(end)) {
      dates.add(currentDate.toString())
      currentDate = currentDate.plusDays(1)
    }

    println("Date range: $startDate to $endDate (${dates.size} days)")
    println("Model line: $modelLine")
    println()

    var createdCount = 0

    for (date in dates) {
      // List event group folders in model-line for this date
      val eventGroupPrefix = "$basePath/$date/model-line/$modelLine/event-group-reference-id/"

      val eventGroupFolders = storage.list(bucket,
        com.google.cloud.storage.Storage.BlobListOption.prefix(eventGroupPrefix),
        com.google.cloud.storage.Storage.BlobListOption.currentDirectory()
      ).iterateAll().mapNotNull { blob ->
        val name = blob.name.removePrefix(eventGroupPrefix).removeSuffix("/")
        if (name.isNotEmpty()) name else null
      }.sorted()

      if (eventGroupFolders.isEmpty()) {
        println("[$date] No event group folders found")
        continue
      }

      println("[$date] Found ${eventGroupFolders.size} event group folder(s)")

      for (egFolder in eventGroupFolders) {
        val donePath = "$eventGroupPrefix$egFolder/done"

        if (dryRun) {
          println("  $egFolder: would upload gs://$bucket/$donePath")
          createdCount++
        } else {
          // Create/overwrite 0-byte blob
          val blobInfo = com.google.cloud.storage.BlobInfo.newBuilder(bucket, donePath)
            .setContentType("application/octet-stream")
            .build()
          storage.create(blobInfo, ByteArray(0))
          println("  $egFolder: uploaded gs://$bucket/$donePath")
          createdCount++
        }
      }
    }

    println()
    if (dryRun) {
      println("[DRY RUN] Would upload $createdCount done file(s)")
    } else {
      println("Uploaded $createdCount done file(s)")
    }
  }
}
