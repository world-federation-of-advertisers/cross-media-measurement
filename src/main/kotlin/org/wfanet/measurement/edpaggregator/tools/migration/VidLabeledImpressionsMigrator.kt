/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.tools.migration

import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.crypto.tink.KmsClient
import com.google.protobuf.util.JsonFormat
import java.time.LocalDate
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.common.crypto.tink.StreamingAeadStorageClient
import org.wfanet.measurement.edpaggregator.EncryptedStorage
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek.ProtobufFormat
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.BlobUri
import org.wfanet.measurement.storage.ConditionalOperationStorageClient
import org.wfanet.measurement.storage.SelectedStorageClient

/** Migrates encrypted VID-labeled impression blobs between model-line namespaces in GCS. */
class VidLabeledImpressionsMigrator(
  private val storage: Storage,
  kmsClientFactory: () -> KmsClient,
  private val log: (String) -> Unit = { message -> logger.info(message) },
) {
  private val kmsClient: KmsClient by lazy(kmsClientFactory)

  /** Parameters for one inclusive date-range migration. */
  data class Request(
    val startDate: LocalDate,
    val endDate: LocalDate,
    val sourceModelLine: String,
    val destinationModelLine: String,
    val sourceDatePrefix: String,
    val destinationBlobPrefix: String,
    val modelLinesAreCompatible: Boolean,
    val dryRun: Boolean = false,
  )

  /** Outcome for one requested date. */
  enum class DateStatus {
    COPIED,
    PLANNED,
    SKIPPED_SOURCE_MISSING,
    SKIPPED_SOURCE_INCOMPLETE,
    SKIPPED_DESTINATION_NOT_EMPTY,
    FAILED,
  }

  /** Result for one requested date. */
  data class DateResult(
    val date: LocalDate,
    val status: DateStatus,
    val dataBlobs: Int = 0,
    val metadataBlobs: Int = 0,
    val doneMarkers: Int = 0,
    val bytes: Long = 0L,
    val message: String = "",
  )

  /** Aggregate result printed after every requested date has been considered. */
  data class Summary(val results: List<DateResult>, val dryRun: Boolean) {
    val requestedDates: Int
      get() = results.size

    val copiedDates: Int
      get() = results.count { it.status == DateStatus.COPIED }

    val plannedDates: Int
      get() = results.count { it.status == DateStatus.PLANNED }

    val skippedSourceMissingDates: Int
      get() = results.count { it.status == DateStatus.SKIPPED_SOURCE_MISSING }

    val skippedSourceIncompleteDates: Int
      get() = results.count { it.status == DateStatus.SKIPPED_SOURCE_INCOMPLETE }

    val skippedDestinationDates: Int
      get() = results.count { it.status == DateStatus.SKIPPED_DESTINATION_NOT_EMPTY }

    val failedDates: Int
      get() = results.count { it.status == DateStatus.FAILED }

    val copiedDataBlobs: Int
      get() = results.filter { it.status != DateStatus.PLANNED }.sumOf { it.dataBlobs }

    val plannedDataBlobs: Int
      get() = results.filter { it.status == DateStatus.PLANNED }.sumOf { it.dataBlobs }

    val writtenMetadataBlobs: Int
      get() = results.filter { it.status != DateStatus.PLANNED }.sumOf { it.metadataBlobs }

    val plannedMetadataBlobs: Int
      get() = results.filter { it.status == DateStatus.PLANNED }.sumOf { it.metadataBlobs }

    val writtenDoneMarkers: Int
      get() = results.sumOf { it.doneMarkers }

    val copiedBytes: Long
      get() = results.filter { it.status != DateStatus.PLANNED }.sumOf { it.bytes }

    val plannedBytes: Long
      get() = results.filter { it.status == DateStatus.PLANNED }.sumOf { it.bytes }

    /** Returns the final operator-facing summary. */
    fun toDisplayString(): String = buildString {
      appendLine("Migration summary:")
      appendLine("  mode: ${if (dryRun) "dry-run" else "write"}")
      appendLine("  requested dates: $requestedDates")
      appendLine("  copied dates: $copiedDates")
      appendLine("  planned dates: $plannedDates")
      appendLine("  skipped missing source dates: $skippedSourceMissingDates")
      appendLine("  skipped incomplete source dates: $skippedSourceIncompleteDates")
      appendLine("  skipped non-empty destination dates: $skippedDestinationDates")
      appendLine("  failed dates: $failedDates")
      appendLine("  copied data blobs: $copiedDataBlobs")
      appendLine("  planned data blobs: $plannedDataBlobs")
      appendLine("  written metadata blobs: $writtenMetadataBlobs")
      appendLine("  planned metadata blobs: $plannedMetadataBlobs")
      appendLine("  written done markers: $writtenDoneMarkers")
      appendLine("  copied encrypted bytes: $copiedBytes")
      append("  planned encrypted bytes: $plannedBytes")
    }
  }

  /** Migrates each date in [Request.startDate] through [Request.endDate], inclusive. */
  suspend fun migrate(request: Request): Summary {
    validate(request)
    val sourcePrefix = parseGcsPrefix(request.sourceDatePrefix, "sourceDatePrefix")
    val destinationPrefix = parseGcsPrefix(request.destinationBlobPrefix, "destinationBlobPrefix")
    require(
      sourcePrefix.bucket != destinationPrefix.bucket || sourcePrefix.key != destinationPrefix.key
    ) {
      "sourceDatePrefix and destinationBlobPrefix must differ"
    }

    val results = mutableListOf<DateResult>()
    var date = request.startDate
    while (!date.isAfter(request.endDate)) {
      results += migrateDate(date, request, sourcePrefix, destinationPrefix)
      date = date.plusDays(1)
    }
    return Summary(results, request.dryRun)
  }

  private fun validate(request: Request) {
    require(!request.startDate.isAfter(request.endDate)) {
      "startDate must be on or before endDate"
    }
    require(request.modelLinesAreCompatible) {
      "modelLinesAreCompatible acknowledgement is required"
    }
    requireNotNull(ModelLineKey.fromName(request.sourceModelLine)) {
      "sourceModelLine is not a valid ModelLine resource name"
    }
    requireNotNull(ModelLineKey.fromName(request.destinationModelLine)) {
      "destinationModelLine is not a valid ModelLine resource name"
    }
    require(request.sourceModelLine != request.destinationModelLine) {
      "sourceModelLine and destinationModelLine must differ"
    }
  }

  private suspend fun migrateDate(
    date: LocalDate,
    request: Request,
    sourceRoot: BlobUri,
    destinationRoot: BlobUri,
  ): DateResult {
    val sourceDateKey = "${sourceRoot.key.trimEnd('/')}/$date/"
    val destinationDoneUri =
      parseGcsPrefix(
        LabeledImpressionsBlobKeys.forDoneUri(
          request.destinationBlobPrefix,
          request.destinationModelLine,
          date,
        ),
        "destination done URI",
        allowObjectName = true,
      )
    val destinationDateKey = destinationDoneUri.key.substringBeforeLast('/') + "/"

    return try {
      if (hasAnyObject(destinationRoot.bucket, destinationDateKey)) {
        return DateResult(date, DateStatus.SKIPPED_DESTINATION_NOT_EMPTY).also {
          log(
            "$date: skipped because gs://${destinationRoot.bucket}/$destinationDateKey is not empty"
          )
        }
      }

      val sourceObjects = listObjects(sourceRoot.bucket, sourceDateKey)
      if (sourceObjects.isEmpty()) {
        return DateResult(date, DateStatus.SKIPPED_SOURCE_MISSING).also {
          log("$date: skipped because gs://${sourceRoot.bucket}/$sourceDateKey does not exist")
        }
      }
      if (sourceObjects.none { it.name == "${sourceDateKey}done" }) {
        return DateResult(date, DateStatus.SKIPPED_SOURCE_INCOMPLETE).also {
          log("$date: skipped because the source date has no done marker")
        }
      }

      val plan = buildPlan(date, request, sourceObjects, destinationDoneUri)
      if (request.dryRun) {
        DateResult(
            date = date,
            status = DateStatus.PLANNED,
            dataBlobs = plan.entries.size,
            metadataBlobs = plan.entries.size,
            bytes = plan.entries.sumOf { it.sourceData.size },
          )
          .also { log("$date: planned ${plan.entries.size} impression blob(s)") }
      } else {
        execute(plan)
      }
    } catch (e: Exception) {
      logger.log(Level.WARNING, "Migration failed for $date", e)
      DateResult(date, DateStatus.FAILED, message = e.message.orEmpty()).also {
        log("$date: failed: ${e.message}")
      }
    }
  }

  private suspend fun buildPlan(
    date: LocalDate,
    request: Request,
    sourceObjects: List<Blob>,
    destinationDoneUri: BlobUri,
  ): DatePlan {
    val metadataObjects = sourceObjects.filter(::isMetadataObject)
    require(metadataObjects.isNotEmpty()) { "source date contains no metadata sidecars" }

    val entries =
      metadataObjects.map { metadataObject ->
        val details = parseBlobDetails(metadataObject)
        validateBlobDetails(details, metadataObject.name)
        require(details.modelLine == request.sourceModelLine) {
          "${metadataObject.name} has model_line ${details.modelLine}, expected " +
            request.sourceModelLine
        }
        val sourceDataUri = parseGcsPrefix(details.blobUri, "BlobDetails.blob_uri", true)
        val sourceData =
          requireNotNull(storage.get(BlobId.of(sourceDataUri.bucket, sourceDataUri.key))) {
            "referenced impression blob does not exist: ${details.blobUri}"
          }
        val targetDataUri =
          parseGcsPrefix(
            LabeledImpressionsBlobKeys.forInputUri(
              request.destinationBlobPrefix,
              details.blobUri,
              request.destinationModelLine,
              date,
            ),
            "destination data URI",
            allowObjectName = true,
          )
        check(targetDataUri.bucket == destinationDoneUri.bucket) {
          "destination data and done URIs must use the same bucket"
        }
        val targetData = BlobId.of(targetDataUri.bucket, targetDataUri.key)
        val targetMetadata = BlobId.of(targetDataUri.bucket, "${targetDataUri.key}.metadata.binpb")
        val sourceEncryptedStorage = encryptedStorage(sourceDataUri.bucket, details)
        require(sourceEncryptedStorage is StreamingAeadStorageClient) {
          "${metadataObject.name} uses a non-streaming DEK, which is not supported"
        }
        PlannedEntry(
          sourceData = sourceData,
          sourceDetails = details,
          sourceEncryptedStorage = sourceEncryptedStorage,
          destinationEncryptedStorage = encryptedStorage(targetData.bucket, details),
          targetData = targetData,
          targetMetadata = targetMetadata,
          targetMetadataBytes =
            details
              .copy {
                blobUri = "gs://${targetData.bucket}/${targetData.name}"
                modelLine = request.destinationModelLine
              }
              .toByteArray(),
        )
      }

    require(entries.map { it.targetData }.distinct().size == entries.size) {
      "multiple metadata sidecars reference the same source impression blob"
    }
    return DatePlan(date, entries, BlobId.of(destinationDoneUri.bucket, destinationDoneUri.key))
  }

  private suspend fun execute(plan: DatePlan): DateResult {
    var copiedDataBlobs = 0
    var writtenMetadataBlobs = 0
    var copiedBytes = 0L
    try {
      for (entry in plan.entries) {
        reencrypt(entry)
        copiedDataBlobs++
        copiedBytes += entry.sourceData.size
        storage.create(
          BlobInfo.newBuilder(entry.targetMetadata)
            .setContentType(BINARY_PROTO_CONTENT_TYPE)
            .build(),
          entry.targetMetadataBytes,
          Storage.BlobTargetOption.doesNotExist(),
        )
        writtenMetadataBlobs++
      }
      storage.create(
        BlobInfo.newBuilder(plan.doneMarker).setContentType(BINARY_CONTENT_TYPE).build(),
        byteArrayOf(),
        Storage.BlobTargetOption.doesNotExist(),
      )
      return DateResult(
          date = plan.date,
          status = DateStatus.COPIED,
          dataBlobs = copiedDataBlobs,
          metadataBlobs = writtenMetadataBlobs,
          doneMarkers = 1,
          bytes = copiedBytes,
        )
        .also { log("${plan.date}: copied $copiedDataBlobs impression blob(s) and wrote done") }
    } catch (e: Exception) {
      logger.log(Level.WARNING, "Copy failed after writing part of ${plan.date}", e)
      return DateResult(
          date = plan.date,
          status = DateStatus.FAILED,
          dataBlobs = copiedDataBlobs,
          metadataBlobs = writtenMetadataBlobs,
          bytes = copiedBytes,
          message = e.message.orEmpty(),
        )
        .also {
          log(
            "${plan.date}: failed after copying $copiedDataBlobs data blob(s) and writing " +
              "$writtenMetadataBlobs metadata blob(s); no done marker was written"
          )
        }
    }
  }

  private suspend fun reencrypt(entry: PlannedEntry) {
    val sourceBlob =
      requireNotNull(entry.sourceEncryptedStorage.getBlob(entry.sourceData.name)) {
        "referenced impression blob disappeared during migration: ${entry.sourceDetails.blobUri}"
      }
    entry.destinationEncryptedStorage.writeBlobIfNotFound(entry.targetData.name, sourceBlob.read())
  }

  private fun encryptedStorage(
    bucket: String,
    details: BlobDetails,
  ): ConditionalOperationStorageClient =
    EncryptedStorage.buildEncryptedStorageClient(
      storageClient = GcsStorageClient(storage, bucket),
      kmsClient = kmsClient,
      kekUri = details.encryptedDek.kekUri,
      encryptedDek = details.encryptedDek,
    )

  private fun validateBlobDetails(details: BlobDetails, metadataName: String) {
    require(details.hasEncryptedDek()) { "$metadataName has no encrypted_dek" }
    require(details.encryptedDek.kekUri.startsWith(GCP_KMS_URI_PREFIX)) {
      "$metadataName uses unsupported KEK URI ${details.encryptedDek.kekUri}; only GCP KMS is supported"
    }
    require(details.encryptedDek.ciphertext.size() > 0) {
      "$metadataName has an empty encrypted_dek.ciphertext"
    }
    require(details.encryptedDek.typeUrl.isNotEmpty()) {
      "$metadataName has an empty encrypted_dek.type_url"
    }
    require(
      (details.encryptedDek.typeUrl == TYPE_URL_TINK_KEYSET &&
        details.encryptedDek.protobufFormat == ProtobufFormat.BINARY) ||
        (details.encryptedDek.typeUrl == TYPE_URL_ENCRYPTION_KEY &&
          details.encryptedDek.protobufFormat == ProtobufFormat.JSON)
    ) {
      "$metadataName has unsupported encrypted_dek type_url=${details.encryptedDek.typeUrl} " +
        "and protobuf_format=${details.encryptedDek.protobufFormat}"
    }
    require(details.interval.hasStartTime() && details.interval.hasEndTime()) {
      "$metadataName has an interval without start_time or end_time"
    }
    require(details.eventGroupReferenceId.isNotEmpty() || details.entityKeysList.isNotEmpty()) {
      "$metadataName has neither event_group_reference_id nor entity_keys"
    }
    details.entityKeysList.forEachIndexed { groupIndex, group ->
      require(group.entityType.isNotEmpty()) {
        "$metadataName has an empty entity_keys[$groupIndex].entity_type"
      }
      require(group.entityIdsList.isNotEmpty() && group.entityIdsList.none { it.isEmpty() }) {
        "$metadataName has invalid entity_keys[$groupIndex].entity_ids"
      }
    }
  }

  private fun parseBlobDetails(metadataObject: Blob): BlobDetails {
    val bytes = metadataObject.getContent()
    return when {
      metadataObject.name.lowercase().endsWith(BINARY_PROTO_SUFFIX) -> BlobDetails.parseFrom(bytes)
      metadataObject.name.lowercase().endsWith(JSON_SUFFIX) ->
        BlobDetails.newBuilder()
          .also {
            JsonFormat.parser().ignoringUnknownFields().merge(bytes.toString(Charsets.UTF_8), it)
          }
          .build()
      else -> error("unsupported metadata extension: ${metadataObject.name}")
    }
  }

  private fun isMetadataObject(blob: Blob): Boolean {
    if (blob.size == 0L) return false
    val fileName = blob.name.substringAfterLast('/').lowercase()
    return METADATA_FILE_NAME in fileName &&
      (fileName.endsWith(BINARY_PROTO_SUFFIX) || fileName.endsWith(JSON_SUFFIX))
  }

  private fun hasAnyObject(bucket: String, prefix: String): Boolean =
    storage
      .list(bucket, Storage.BlobListOption.prefix(prefix), Storage.BlobListOption.pageSize(1))
      .iterateAll()
      .iterator()
      .hasNext()

  private fun listObjects(bucket: String, prefix: String): List<Blob> =
    storage.list(bucket, Storage.BlobListOption.prefix(prefix)).iterateAll().toList()

  private fun parseGcsPrefix(
    value: String,
    fieldName: String,
    allowObjectName: Boolean = false,
  ): BlobUri {
    val normalized = value.trim().trimEnd('/')
    val uri =
      try {
        SelectedStorageClient.parseBlobUri(normalized)
      } catch (e: IllegalArgumentException) {
        throw IllegalArgumentException("$fieldName must be a valid gs:// URI", e)
      }
    require(uri.scheme == "gs" && uri.bucket.isNotEmpty() && uri.key.isNotEmpty()) {
      "$fieldName must be a non-root gs:// URI"
    }
    require(uri.key.split('/').none { it.isEmpty() || it == "." || it == ".." }) {
      "$fieldName must not contain empty, dot, or parent path segments"
    }
    if (!allowObjectName) {
      require(!normalized.endsWith("/done")) { "$fieldName must be a directory prefix" }
    }
    return uri
  }

  private data class DatePlan(
    val date: LocalDate,
    val entries: List<PlannedEntry>,
    val doneMarker: BlobId,
  )

  private data class PlannedEntry(
    val sourceData: Blob,
    val sourceDetails: BlobDetails,
    val sourceEncryptedStorage: ConditionalOperationStorageClient,
    val destinationEncryptedStorage: ConditionalOperationStorageClient,
    val targetData: BlobId,
    val targetMetadata: BlobId,
    val targetMetadataBytes: ByteArray,
  )

  companion object {
    private val logger = Logger.getLogger(VidLabeledImpressionsMigrator::class.java.name)
    private const val METADATA_FILE_NAME = "metadata"
    private const val BINARY_PROTO_SUFFIX = ".binpb"
    private const val JSON_SUFFIX = ".json"
    private const val BINARY_PROTO_CONTENT_TYPE = "application/x-protobuf"
    private const val BINARY_CONTENT_TYPE = "application/octet-stream"
    private const val GCP_KMS_URI_PREFIX = "gcp-kms://"
    private const val TYPE_URL_TINK_KEYSET = "type.googleapis.com/google.crypto.tink.Keyset"
    private const val TYPE_URL_ENCRYPTION_KEY =
      "type.googleapis.com/wfa.measurement.edpaggregator.v1alpha.EncryptionKey"
  }
}
