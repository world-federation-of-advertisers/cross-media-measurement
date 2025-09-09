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

import com.google.api.gax.paging.Page
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import com.google.protobuf.Timestamp
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
import org.wfanet.measurement.edpaggregator.v1alpha.createImpressionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest
import com.google.type.interval
import com.google.type.Interval
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.DataProviderKt.dataAvailabilityMapEntry
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.replaceDataAvailabilityIntervalsRequest


class DataAvailability(
  private val storageClient: Storage,
  private val dataProvidersStub: DataProvidersGrpcKt.DataProvidersCoroutineStub,
  private val impressionMetadataServiceStub: ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub,
  private val dataProviderName: String,
  private val throttler: Throttler,
) {

  suspend fun sync(path: String) {

    try {

      // 1. Crawl for metadata files
      val bucket = path.substringBefore("/")
      val objectPath = path.substringAfter("/")
      val folderPrefix = objectPath.substringBeforeLast("/", "")

      val impressionMetadataBlobs: Page<Blob> = storageClient.list(
        bucket,
        Storage.BlobListOption.prefix(if (folderPrefix.isEmpty()) "" else "$folderPrefix/")
      )

      // 1. Retrieve blob details from storage and build a map
      val impressionMetadataMap: Map<String, List<ImpressionMetadata>> = createImpressionMetadataMap(impressionMetadataBlobs, objectPath, bucket)

      // 2. Retrieve ImpressionMetadata from ImpressionMetadataStorage, join the intervals and validate them
      val availabilityEntries = mutableListOf<DataProvider.DataAvailabilityMapEntry>()
      for (modelLine in impressionMetadataMap.keys) {
        val existingImpressionMetadata: List<ImpressionMetadata> = getImpressionMetadataFromStorage(modelLine)
        val combinedList: List<ImpressionMetadata> = impressionMetadataMap[modelLine]!! + existingImpressionMetadata
        val intervalForModelLine: Interval? = try {
          validateAndJoinModelLineIntervals(combinedList)
        } catch (e: Exception) {
          logger.severe("Invalid intervals for model line: $modelLine. ${e.message}")
          null
        }
        if (intervalForModelLine != null) {
          saveImpressionMetadataToStorage(impressionMetadataMap[modelLine]!!)
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

  suspend fun saveImpressionMetadataToStorage(impressionMetadataList: List<ImpressionMetadata>) {
    impressionMetadataList.forEach {
      impressionMetadataServiceStub.createImpressionMetadata(createImpressionMetadataRequest {
        parent = dataProviderName
        this.impressionMetadata = it
      })
    }
  }

  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  suspend fun getImpressionMetadataFromStorage(modelLine: String):  List<ImpressionMetadata> {
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
   * Builds a mapping from model line identifiers to their corresponding impression metadata entries.
   *
   * This function iterates through all blobs in the given [impressionMetadataBlobs] page, skipping the
   * blob whose name exactly matches [objectPath].
   * The resulting map groups all [ImpressionMetadata] objects by their `modelLine` field.
   *
   * @param impressionMetadataBlobs a [Page] of GCS [Blob] objects to read and parse.
   * @param objectPath the path of a blob to skip; any blob with this exact name is excluded from processing.
   * @param bucket the name of the GCS bucket containing the blobs.
   * @return a [Map] where each key is a model line identifier and each value is a [List] of
   *         [ImpressionMetadata] objects belonging to that model line.
   */
  fun createImpressionMetadataMap(impressionMetadataBlobs : Page<Blob>, objectPath: String, bucket: String) : Map<String, List<ImpressionMetadata>> {
    val modelLines = mutableMapOf<String, MutableList<ImpressionMetadata>>()
    for (blob in impressionMetadataBlobs.iterateAll()) {
      if (blob.name != objectPath) {
        val blobDetails = BlobDetails.parseFrom(blob.getContent())
        val metadata_blob_uri = "gs://$bucket/${blob.name}"
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
  }

}
