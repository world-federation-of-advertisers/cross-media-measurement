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

import com.google.protobuf.ByteString
import com.google.protobuf.TextFormat
import com.google.protobuf.util.JsonFormat
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.MappedEventGroup
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

@Command(
  name = "event_group_manager",
  description = ["Manages EventGroup protobuf records in mesos record format. Supports local files and GCS (gs://) paths."],
  subcommands = [
    CommandLine.HelpCommand::class,
    PrintCommand::class,
    PrintMapCommand::class,
    CountCommand::class,
    AppendCommand::class,
    AppendMapCommand::class,
    SampleCommand::class,
    FilterCommand::class,
    ListImpressionsCommand::class,
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

@Command(name = "mark-done", description = ["Create 'done' marker files in model-line folders for a date range"])
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
    var skippedCount = 0

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

        // Check if done file already exists
        val existingBlob = storage.get(bucket, donePath)
        if (existingBlob != null) {
          println("  $egFolder: done file already exists (skipping)")
          skippedCount++
          continue
        }

        if (dryRun) {
          println("  $egFolder: would create gs://$bucket/$donePath")
          createdCount++
        } else {
          // Create 0-byte blob
          val blobInfo = com.google.cloud.storage.BlobInfo.newBuilder(bucket, donePath)
            .setContentType("application/octet-stream")
            .build()
          storage.create(blobInfo, ByteArray(0))
          println("  $egFolder: created gs://$bucket/$donePath")
          createdCount++
        }
      }
    }

    println()
    if (dryRun) {
      println("[DRY RUN] Would create $createdCount done file(s), skip $skippedCount existing file(s)")
    } else {
      println("Created $createdCount done file(s), skipped $skippedCount existing file(s)")
    }
  }
}

