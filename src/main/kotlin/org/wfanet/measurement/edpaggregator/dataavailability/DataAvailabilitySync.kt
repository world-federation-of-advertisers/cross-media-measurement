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
import com.google.protobuf.Timestamp
import com.google.protobuf.util.JsonFormat
import io.grpc.StatusException
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.impressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequestKt.filter
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata.State
import org.wfanet.measurement.edpaggregator.v1alpha.CreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest
import com.google.type.interval
import com.google.type.Interval
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.DataProviderKt.dataAvailabilityMapEntry
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.replaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.storage.StorageClient
import java.util.*
import kotlin.text.Charsets.UTF_8


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
   * @param path the full Cloud Storage object path of the "done" blob.
   */
  suspend fun sync(path: String) {

    try {

      // 1. Crawl for metadata files
      val bucket = path.substringBefore("/")
      val objectPath = path.substringAfter("/")
      val folderPrefix = objectPath.substringBeforeLast("/", "")

      require(EDP_PREFIX_REGEX.matches(folderPrefix)) {
        "Invalid folder prefix ($folderPrefix): must match pattern 'edp/<edp_name_here>/'"
      }

      val impressionMetadataBlobs: Flow<StorageClient.Blob> = storageClient.listBlobs("$folderPrefix")

      // 1. Retrieve blob details from storage and build a map
      val impressionMetadataMap: Map<String, List<ImpressionMetadata>> = createImpressionMetadataMap(impressionMetadataBlobs, objectPath, bucket)

      // 2. Retrieve ImpressionMetadata from ImpressionMetadataStorage, join the intervals and validate them
      val availabilityEntries = mutableListOf<DataProvider.DataAvailabilityMapEntry>()
      for (modelLine in impressionMetadataMap.keys) {
        val existingImpressionMetadata: List<ImpressionMetadata> = getImpressionMetadata(modelLine)
        val combinedList: List<ImpressionMetadata> = impressionMetadataMap[modelLine]!! + existingImpressionMetadata
        val intervalForModelLine: Interval? = try {
          validateAndJoinModelLineIntervals(combinedList)
        } catch (e: Exception) {
          logger.severe("Invalid intervals for model line: $modelLine. ${e.message}")
          null
        }
        if (intervalForModelLine != null) {
          saveImpressionMetadata(impressionMetadataMap[modelLine]!!)
          availabilityEntries += dataAvailabilityMapEntry {
            key = modelLine
            value = intervalForModelLine
          }
        }
      }
      if(!availabilityEntries.isEmpty()) {
        dataProvidersStub.replaceDataAvailabilityIntervals(
          replaceDataAvailabilityIntervalsRequest {
            name = dataProviderName
            dataAvailabilityIntervals += availabilityEntries
          }
        )
      }
    } catch (e: Exception) {
      logger.severe(
        "Unable to process Data Availability for ${path}: ${e.message}"
      )
    }

  }

  suspend fun saveImpressionMetadata(impressionMetadataList: List<ImpressionMetadata>) {
    val createImpressionMetadataRequests: MutableList<CreateImpressionMetadataRequest> = mutableListOf()
    impressionMetadataList.forEach {
      createImpressionMetadataRequests.add(
        createImpressionMetadataRequest {
          parent = dataProviderName
          this.impressionMetadata = it
          requestId = UUID.randomUUID().toString()
        }
      )
    }
    impressionMetadataServiceStub.batchCreateImpressionMetadata(batchCreateImpressionMetadataRequest {
      parent = dataProviderName
      requests += createImpressionMetadataRequests
    })
  }

  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  suspend fun getImpressionMetadata(modelLine: String):  List<ImpressionMetadata> {
    return impressionMetadataServiceStub.listResources { pageToken: String ->
      val response =
        try {
          throttler.onReady {
            impressionMetadataServiceStub.listImpressionMetadata(
              listImpressionMetadataRequest {
                parent = dataProviderName
                this.pageToken = pageToken
                filter = filter {
                  this.modelLine = modelLine
                  state = ImpressionMetadata.State.ACTIVE
                }
              }
            )
          }
        } catch (e: StatusException) {
          throw Exception("Error listing ImpressionMetadata", e)
        }
      ResourceList(response.impressionMetadataList, response.nextPageToken)
    }.flattenConcat().toList()
  }

  /**
   * Reads impression metadata blobs from a given flow of storage objects and groups them by
   * model line.
   *
   * This function iterates over each blob in [impressionMetadataBlobs], skipping the one whose
   * blob key exactly matches [objectPath].
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
   * @param objectPath the blob key to exclude from processing (e.g., a done/marker file path).
   * @param bucket the GCS bucket name to use when constructing URIs for blobs.
   * @return a map where each key is a `modelLine` string and each value is the list of
   *         [ImpressionMetadata] objects associated with that model line.
   *
   * @throws InvalidProtocolBufferException if a blob cannot be parsed as either binary or JSON
   *         `BlobDetails`.
   */
  suspend fun createImpressionMetadataMap(impressionMetadataBlobs : Flow<StorageClient.Blob>, objectPath: String, bucket: String) : Map<String, List<ImpressionMetadata>> {
    val modelLines = mutableMapOf<String, MutableList<ImpressionMetadata>>()
    impressionMetadataBlobs.filter { blob ->
        val fileName = blob.blobKey.substringAfterLast("/").lowercase()
        METADATA_FILE_NAME in fileName &&
                (fileName.endsWith(PROTO_FILE_SUFFIX) || fileName.endsWith(JSON_FILE_SUFFIX))
      }.collect { blob ->

      val fileName = blob.blobKey.substringAfterLast("/").lowercase()
      val bytes: ByteString = blob.read().flatten()

      val blobDetails = if (fileName.endsWith(PROTO_FILE_SUFFIX)) {
        BlobDetails.parseFrom(bytes)
      } else {
        val builder = BlobDetails.newBuilder()
        JsonFormat.parser()
          .ignoringUnknownFields()
          .merge(bytes.toString(UTF_8), builder)
        builder.build()
      }

      val metadata_blob_uri = "gs://$bucket/${blob.blobKey}"
      val impressionBlob = storageClient.getBlob(blobDetails.blobUri)
      if (impressionBlob == null) {
        logger.info("Encrypted impressions blob non found for metadata: $metadata_blob_uri.")
      } else {
        val impressionMetadata = impressionMetadata {
          blobUri = metadata_blob_uri
          eventGroupReferenceId = blobDetails.eventGroupReferenceId
          modelLine = blobDetails.modelLine
          interval = blobDetails.interval
        }
        modelLines
          .getOrPut(blobDetails.modelLine) { mutableListOf() }
          .add(impressionMetadata)
      }
    }
    return modelLines
  }

  /**
   * Validates a list of [ImpressionMetadata] intervals and returns a single [Interval]
   * that spans from the earliest start time to the latest end time.
   *
   * ### Validation:
   * 1. All intervals must have both a start time and an end time (`hasStartTime()` and `hasEndTime()` must be `true`).
   * 2. The list must not be empty.
   * 3. Each interval must be contiguous with or overlapping the previous interval
   *
   * @param impressionMetadata the list of [ImpressionMetadata] containing intervals to validate and join.
   * @return a single [Interval] representing the union of all contiguous intervals.
   * @throws IllegalArgumentException if:
   *   - the list is empty,
   *   - any interval is missing a start or end time,
   *   - intervals are not contiguous in time.
   */
  fun validateAndJoinModelLineIntervals(impressionMetadata: List<ImpressionMetadata>) : Interval {
    val intervals = impressionMetadata
      .map { it.interval }
      .also { list ->
        require(list.all { it.hasStartTime() && it.hasEndTime() }) {
          "Found interval without start or end time"
        }
      }
      .sortedBy { it.startTime.seconds }

    require(intervals.isNotEmpty()) { "No intervals provided for model line." }

    var intervalStartTime: Timestamp? = null
    var intervalEndTime: Timestamp? = null
    intervals.forEach {
      if (intervalStartTime == null && intervalEndTime == null){
        intervalStartTime = it.startTime
        intervalEndTime = it.endTime
      } else {
        require(it.startTime.seconds <= intervalEndTime!!.seconds) { "Intervals are not contiguous." }
        intervalEndTime = if (intervalEndTime!!.seconds > it.endTime.seconds) {
          intervalEndTime
        } else {
          it.endTime
        }
      }
    }

    return interval {
      startTime = intervalStartTime!!
      endTime = intervalEndTime!!
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val METADATA_FILE_NAME = "metadata"
    private val EDP_PREFIX_REGEX = Regex("""^edp/[^/]+$""")
    private const val PROTO_FILE_SUFFIX = ".pb"
    private const val JSON_FILE_SUFFIX = ".json"
  }

}
