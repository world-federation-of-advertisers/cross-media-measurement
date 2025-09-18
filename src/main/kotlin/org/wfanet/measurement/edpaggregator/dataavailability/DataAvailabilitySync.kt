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

package org.wfanet.measurement.edpaggregator.dataavailability

import com.google.protobuf.ByteString
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.util.JsonFormat
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.CreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ComputeModelLinesAvailabilityResponse
import org.wfanet.measurement.edpaggregator.v1alpha.computeModelLinesAvailabilityRequest
import com.google.type.interval
import io.grpc.StatusException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.DataProviderKt.dataAvailabilityMapEntry
import org.wfanet.measurement.api.v2alpha.replaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.StorageClient
import java.util.UUID
import kotlin.text.Charsets.UTF_8
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.BlobUri
import java.security.MessageDigest

/**
 * Synchronizes impression data availability between Cloud Storage and the Kingdom.
 *
 * This class coordinates the workflow that occurs after impression data has been fully uploaded
 * to Cloud Storage and signaled by the presence of a "done" blob in the relevant folder.
 * It handles:
 *
 * - Crawling the folder where the "done" blob resides to find and parse impression metadata files
 *   (`.pb` or `.json`).
 * - Validating and storing impression metadata records via the [ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub].
 * - Computing model line availability intervals using the impression metadata service.
 * - Updating the kingdom data provider’s availability intervals through
 *   [DataProvidersGrpcKt.DataProvidersCoroutineStub].
 *
 * The synchronization process is throttled via the provided [Throttler] to prevent overwhelming
 * downstream services with bursts of requests.
 *
 * Typical workflow:
 * 1. A "done" blob path is passed to [sync].
 * 2. Metadata blobs in the same folder are discovered and parsed into [ImpressionMetadata].
 * 3. Valid impression metadata are persisted.
 * 4. Model line availability intervals are computed from the persisted metadata.
 * 5. The data provider’s availability intervals are updated accordingly.
 *
 * @property storageClient Client for accessing Cloud Storage blobs, used for crawling and reading metadata files.
 * @property dataProvidersStub gRPC stub for interacting with the Kingdom Data Providers service.
 * @property impressionMetadataServiceStub gRPC stub for creating impression metadata and computing model line availability.
 * @property dataProviderName The resource name of the data provider, used as a parent identifier in gRPC requests.
 * @property throttler A throttling utility to regulate request flow to external services.
 */
class DataAvailabilitySync(
  private val storageClient: StorageClient,
  private val dataProvidersStub: DataProvidersGrpcKt.DataProvidersCoroutineStub,
  private val impressionMetadataServiceStub: ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub,
  private val dataProviderName: String,
  private val throttler: Throttler,
) {

  /**
   * Synchronizes impression availability data after a completion signal.
   *
   * This function is triggered when a "done" blob is detected in Cloud Storage,
   * indicating that all impression data for a given day have been fully uploaded.
   * The "done" blob resides in the same folder as the metadata files, and its path
   * is used to determine which folder to crawl when collecting and processing
   * metadata for that day.
   *
   * @param doneBlobPath the full Cloud Storage object path of the "done" blob.
   */
  suspend fun sync(doneBlobPath: String) {
    // 1. Crawl for metadata files
    val doneBlobUri: BlobUri =
      SelectedStorageClient.parseBlobUri(doneBlobPath)
    val folderPrefix = doneBlobUri.key.substringBeforeLast("/", "")

    require(VALID_IMPRESSION_PATH_PREFIX.matches(folderPrefix)) {
      "Folder prefix $folderPrefix does not match expected pattern $VALID_IMPRESSION_PATH_PREFIX"
    }

    val doneBlobFolderPath = doneBlobUri.key.substringBeforeLast("/")
    val impressionMetadataBlobs: Flow<StorageClient.Blob> = storageClient.listBlobs(doneBlobFolderPath)

    // 1. Retrieve blob details from storage and build a map and validate them
    val impressionMetadataMap: Map<String, List<ImpressionMetadata>> = createModelLineToImpressionMetadataMap(impressionMetadataBlobs, doneBlobUri)

    if (impressionMetadataMap.isEmpty()){
      logger.info("There were no valid impressions metadata.")
      return
    }

    // 2. Save ImpressionMetadata using ImpressionMetadataStorage
    impressionMetadataMap.values.forEach { saveImpressionMetadata(it) }

    // 3. Retrieve model line availability from ImpressionMetadataStorage for all model line
    // found in the storage folder and update kingdom availability
    // Collect all model lines
    val modelLines = impressionMetadataMap.keys.toList()
    val modelLinesAvailabilityInterval: ComputeModelLinesAvailabilityResponse =
      impressionMetadataServiceStub.computeModelLinesAvailability(
        computeModelLinesAvailabilityRequest {
          parent = dataProviderName
          this.modelLines += modelLines
        }
      )

    // Build availability entries from the response
    val availabilityEntries = modelLinesAvailabilityInterval.modelLineAvailabilitiesList.map { availability ->
      dataAvailabilityMapEntry {
        key = availability.modelLine
        value = interval {
          startTime = availability.availability.startTime
          endTime = availability.availability.endTime
        }
      }
    }
    if (availabilityEntries.isNotEmpty()) {
      throttler.onReady {
        try {
          dataProvidersStub.replaceDataAvailabilityIntervals(
            replaceDataAvailabilityIntervalsRequest {
              name = dataProviderName
              dataAvailabilityIntervals += availabilityEntries
            }
          )
        } catch (e: StatusException) {
          throw Exception("Error replacing DataAvailability intervals", e)
        }
      }
    }

  }

  suspend private fun saveImpressionMetadata(impressionMetadataList: List<ImpressionMetadata>) {
    val createImpressionMetadataRequests: MutableList<CreateImpressionMetadataRequest> = mutableListOf()
    impressionMetadataList.forEach {
      createImpressionMetadataRequests.add(
        createImpressionMetadataRequest {
          parent = dataProviderName
          this.impressionMetadata = it
          requestId = uuidV4FromPath(it.blobUri)
        }
      )
    }
    try {
      throttler.onReady {
        impressionMetadataServiceStub.batchCreateImpressionMetadata(batchCreateImpressionMetadataRequest {
          parent = dataProviderName
          requests += createImpressionMetadataRequests
        })
      }
    } catch (e: StatusException) {
      throw Exception("Error creating Impressions Metadata", e)
    }
  }

  /**
   * Reads impression metadata blobs from a given flow of storage objects and groups them by
   * model line.
   *
   * This function iterates over each blob in [impressionMetadataBlobs], filtering out those
   * that does not have the string "metadata" in the file name.
   *
   * For each blob:
   * - Determines the format based on the file extension:
   *   - If the file name ends with `.pb`, parses the content as a binary `BlobDetails` protobuf.
   *   - If the file name ends with `.json`, parses the content as JSON using [JsonFormat.parser] with `ignoringUnknownFields()`
   *   - Ignore otherwise
   * - Reads the full blob content into a [ByteString] via [StorageClient.Blob.read] and [flatten].
   * - Attempts to parse the content as a binary `BlobDetails` protobuf.
   * - If binary parsing fails with [InvalidProtocolBufferException], falls back to parsing as JSON
   *   using [JsonFormat.parser] with `ignoringUnknownFields()`.
   * - Constructs an [ImpressionMetadata] object using:
   *   - `blobUri` set to the URI built from [bucket] and the blob's key
   *   - `eventGroupReferenceId`, `modelLine`, and `interval` from the parsed `BlobDetails`
   * - Adds the [ImpressionMetadata] to a list in a map keyed by `modelLine`.
   *
   * @param impressionMetadataBlobs the flow of [StorageClient.Blob] objects to read and parse.
   * @param blobUri the blob uri.
   * @return a map where each key is a `modelLine` string and each value is the list of
   *         [ImpressionMetadata] objects associated with that model line.
   *
   * @throws InvalidProtocolBufferException if a blob cannot be parsed as either binary or JSON
   *         `BlobDetails`.
   */
  suspend private fun createModelLineToImpressionMetadataMap(impressionMetadataBlobs : Flow<StorageClient.Blob>, blobUri: BlobUri) : Map<String, List<ImpressionMetadata>> {
    val impressionMetadataMap = mutableMapOf<String, MutableList<ImpressionMetadata>>()
    impressionMetadataBlobs.filter { blob ->
      val fileName = blob.blobKey.substringAfterLast("/").lowercase()
        METADATA_FILE_NAME in fileName
      }.collect { blob ->
      val fileName = blob.blobKey.substringAfterLast("/").lowercase()
      val bytes: ByteString = blob.read().flatten()
      // Build the blob details object
      val blobDetails = if (fileName.endsWith(PROTO_FILE_SUFFIX)) {
        BlobDetails.parseFrom(bytes)
      } else if (fileName.endsWith(JSON_FILE_SUFFIX)) {
        val builder = BlobDetails.newBuilder()
        JsonFormat.parser()
          .ignoringUnknownFields()
          .merge(bytes.toString(UTF_8), builder)
        builder.build()
      } else {
        throw IllegalArgumentException("Unsupported file extension for metadata: $fileName")
      }

      // Validate intervals
      require(blobDetails.interval.hasStartTime() && blobDetails.interval.hasEndTime()) {
        "Found interval without start or end time for blob detail with blob_uri = ${blobDetails.blobUri}"
      }

      val metadata_blob_uri = blob.blobKey
      val impressionBlob = storageClient.getBlob(blobDetails.blobUri)
      if (impressionBlob == null) {
        logger.info("Encrypted impressions blob non found for metadata: $metadata_blob_uri.")
      } else {
        val impressionMetadata = impressionMetadata {
          this.blobUri = metadata_blob_uri
          eventGroupReferenceId = blobDetails.eventGroupReferenceId
          modelLine = blobDetails.modelLine
          interval = blobDetails.interval
        }
        impressionMetadataMap
          .getOrPut(blobDetails.modelLine) { mutableListOf() }
          .add(impressionMetadata)
      }
    }
    return impressionMetadataMap
  }

  /**
   * Generates a deterministic UUIDv4 string from a blob path.
   *
   * This function ensures idempotency when the same path is used repeatedly:
   * @param metadataBlobUri The input path (e.g., a Google Cloud Storage blob URI).
   * @return A UUIDv4-compliant string that is stable for the given path.
   */
  private fun uuidV4FromPath(metadataBlobUri: String): String {
    val hash = MessageDigest.getInstance("SHA-256").digest(metadataBlobUri.toByteArray())
    val bytes = hash.copyOf(16)
    bytes[6] = (bytes[6].toInt() and 0x0F or 0x40).toByte()
    bytes[8] = (bytes[8].toInt() and 0x3F or 0x80).toByte()
    return UUID.nameUUIDFromBytes(bytes).toString()
  }

  fun BlobUri.asUriString(): String =
    if (scheme == "file") {
      "file:///$bucket/$key"
    } else {
      "$scheme://$bucket/$key"
    }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val METADATA_FILE_NAME = "metadata"
    private const val PROTO_FILE_SUFFIX = ".pb"
    private const val JSON_FILE_SUFFIX = ".json"

    private val VALID_IMPRESSION_PATH_PREFIX: Regex =
      Regex("^edp/[^/]+/[^/]+(/.*)?$")

  }

}
