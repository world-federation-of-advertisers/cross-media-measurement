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

import com.google.protobuf.ByteString
import com.google.protobuf.util.JsonFormat
import com.google.type.Interval
import io.grpc.Status
import io.grpc.StatusException
import java.util.logging.Logger
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResourcesWithAdaptivePageSize
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * Describes an impression data source for a specific time interval.
 *
 * The interval is expressed as a closed-open [Interval] , and the [blobDetails] provides the
 * information required to read and decrypt the corresponding impression blob.
 */
data class ImpressionDataSource(
  val modelLine: String,
  val interval: Interval,
  val blobDetails: BlobDetails,
)

/**
 * @param impressionMetadataStub used to sync impressions with the ImpressionsMetadataStorage
 * @param dataProvider The DataProvider resource name
 * @param impressionsMetadataStorageConfig configuration for metadata storage
 * @param listMaxAttempts maximum total attempts for each `ListImpressionMetadata` page before
 *   giving up. Transient gRPC failures (`UNAVAILABLE` / `DEADLINE_EXCEEDED`, e.g. a server-side
 *   rate limit or an overloaded metadata service) are retried so they do not fail the whole report.
 * @param listRetryBackoff exponential backoff applied between transient list retries.
 */
class ImpressionDataSourceProvider(
  private val impressionMetadataStub: ImpressionMetadataServiceCoroutineStub,
  private val dataProvider: String,
  private val impressionsMetadataStorageConfig: StorageConfig,
  private val listMaxAttempts: Int = DEFAULT_LIST_MAX_ATTEMPTS,
  private val listRetryBackoff: ExponentialBackoff = ExponentialBackoff(),
) {

  /**
   * Lists impression data sources for a given selector within a period.
   *
   * @param modelLine the model line to query.
   * @param selector determines whether to query by entity key or event group reference ID.
   * @param period closed-open time interval in UTC.
   * @return sources covering the requested period; empty if the period maps to no dates.
   * @throws ImpressionReadException if a required metadata blob does not exist.
   * @throws com.google.protobuf.InvalidProtocolBufferException if a metadata blob is present but
   *   contains invalid `BlobDetails`.
   */
  suspend fun listImpressionDataSources(
    modelLine: String,
    selector: ImpressionQuerySelector,
    period: Interval,
  ): List<ImpressionDataSource> {
    return getImpressionsMetadata(modelLine, selector, period)
      .map { metadata ->
        logger.info("Processing impression metadata: $metadata")
        val blobDetails = readBlobDetails(metadata.blobUri)
        ImpressionDataSource(
          modelLine = modelLine,
          interval = metadata.interval,
          blobDetails = blobDetails,
        )
      }
      .toList()
  }

  /**
   * Queries impression metadata using the given [selector] strategy.
   *
   * @param reportModelLine the model line
   * @param selector determines whether to query by entity key or event group reference ID.
   * @param period the time period to filter by
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private fun getImpressionsMetadata(
    reportModelLine: String,
    selector: ImpressionQuerySelector,
    period: Interval,
  ): Flow<ImpressionMetadata> {
    logger.info("Resolving path for impression metadata: $reportModelLine, $selector, $period")

    // Fetches a single page, retrying the transient gRPC statuses that are safe to replay here
    // (ListImpressionMetadata is an idempotent read): UNAVAILABLE and DEADLINE_EXCEEDED, so a
    // server-side rate limit or an overloaded metadata service self-heals instead of failing the
    // whole report. RESOURCE_EXHAUSTED is the adaptive page-size signal (typically the gRPC inbound
    // message-size limit): it is propagated raw so listResourcesWithAdaptivePageSize can halve the
    // page size and retry. Any other status fails the report.
    suspend fun listPage(
      pageToken: String,
      pageSize: Int,
    ): ResourceList<ImpressionMetadata, String> {
      var attempt = 0
      while (true) {
        attempt++
        try {
          val response =
            impressionMetadataStub.listImpressionMetadata(
              listImpressionMetadataRequest {
                parent = dataProvider
                filter =
                  ListImpressionMetadataRequestKt.filter {
                    modelLine = reportModelLine
                    when (selector) {
                      is ImpressionQuerySelector.ByEventGroupReferenceId ->
                        eventGroupReferenceIds += selector.refId
                      is ImpressionQuerySelector.ByEventGroupReferenceIds ->
                        eventGroupReferenceIds += selector.refIds
                      is ImpressionQuerySelector.ByEntityKey ->
                        this.entityKeys += selector.entityKey
                    }
                    intervalOverlaps = period
                  }
                this.pageSize = pageSize
                this.pageToken = pageToken
              }
            )
          return ResourceList(response.impressionMetadataList, response.nextPageToken)
        } catch (e: StatusException) {
          if (e.status.code == Status.Code.RESOURCE_EXHAUSTED) throw e
          val retryable =
            e.status.code == Status.Code.UNAVAILABLE ||
              e.status.code == Status.Code.DEADLINE_EXCEEDED
          if (!retryable || attempt >= listMaxAttempts) {
            throw Exception(
              "Error listing ImpressionMetadata for dataProvider=$dataProvider, " +
                "modelLine=$reportModelLine, selector=$selector, period=$period: ${e.status}",
              e,
            )
          }
          logger.warning {
            "Transient failure listing ImpressionMetadata for dataProvider=$dataProvider on " +
              "attempt $attempt of $listMaxAttempts (${e.status.code}); retrying"
          }
          delay(listRetryBackoff.durationForAttempt(attempt).toMillis())
        }
      }
    }

    return impressionMetadataStub
      .listResourcesWithAdaptivePageSize(
        startingPageSize = LIST_IMPRESSION_METADATA_STARTING_PAGE_SIZE,
        minPageSize = MIN_LIST_IMPRESSION_METADATA_PAGE_SIZE,
        onPageSizeReduced = { oldSize, newSize ->
          logger.warning(
            "ListImpressionMetadata returned RESOURCE_EXHAUSTED at page_size=$oldSize for " +
              "dataProvider=$dataProvider; halving to $newSize and retrying"
          )
        },
      ) { pageToken: String, pageSize: Int ->
        listPage(pageToken, pageSize)
      }
      .flattenConcat()
  }

  /**
   * Reads `BlobDetails` from a metadata blob.
   *
   * @param metadataPath blob URI for the metadata (e.g., `file:///bucket/key`).
   * @return parsed [BlobDetails].
   * @throws ImpressionReadException with [ImpressionReadException.Code.BLOB_NOT_FOUND] when the
   *   blob is missing.
   * @throws com.google.protobuf.InvalidProtocolBufferException if parsing fails due to invalid
   *   metadata contents.
   */
  private suspend fun readBlobDetails(metadataPath: String): BlobDetails {
    val storageClientUri = SelectedStorageClient.parseBlobUri(metadataPath)
    val storageClient =
      SelectedStorageClient(
        storageClientUri,
        impressionsMetadataStorageConfig.rootDirectory,
        impressionsMetadataStorageConfig.projectId,
      )
    logger.info("Reading impression metadata from $metadataPath")
    val blob =
      storageClient.getBlob(storageClientUri.key)
        ?: throw ImpressionReadException(
          metadataPath,
          ImpressionReadException.Code.BLOB_NOT_FOUND,
          "BlobDetails metadata not found",
        )

    val bytes: ByteString = blob.read().flatten()
    // TODO(world-federation-of-advertisers/cross-media-measurement#2948): Determine blob parsing
    // logic based on file extension
    return try {
      BlobDetails.parseFrom(bytes)
    } catch (e: com.google.protobuf.InvalidProtocolBufferException) {
      val builder = BlobDetails.newBuilder()
      JsonFormat.parser().ignoringUnknownFields().merge(bytes.toString(Charsets.UTF_8), builder)
      builder.build()
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /**
     * Page size to request for `ListImpressionMetadata`. Large so a whole interval's metadata is
     * fetched in a handful of RPCs instead of hundreds; `listResourcesWithAdaptivePageSize` halves
     * it toward [MIN_LIST_IMPRESSION_METADATA_PAGE_SIZE] if a page ever exceeds the gRPC message
     * limit. Must be <= the services' MAX_PAGE_SIZE or the server clamps it back down.
     */
    private const val LIST_IMPRESSION_METADATA_STARTING_PAGE_SIZE = 1000

    /** Floor for the adaptive page size. */
    private const val MIN_LIST_IMPRESSION_METADATA_PAGE_SIZE = 1

    /** Default maximum total attempts (first attempt + retries) per ListImpressionMetadata page. */
    private const val DEFAULT_LIST_MAX_ATTEMPTS = 4
  }
}
