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

import com.google.type.Interval
import io.grpc.StatusException
import java.util.logging.Logger
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ListImpressionMetadataRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.listImpressionMetadataRequest

/**
 * Describes an impression data source for a specific time interval.
 *
 * The interval is expressed as a closed-open [Interval] , and the [blobDetails] provides the
 * information required to read and decrypt the corresponding impression blob.
 */
data class ImpressionDataSource(
  val modelLine: String,
  val eventGroupReferenceId: String,
  val interval: Interval,
  val blobDetails: BlobDetails,
)

/**
 * @param impressionMetadataStub used to sync impressions with the ImpressionsMetadataStorage
 * @param dataProvider The DataProvider resource name
 * @param impressionsMetadataStorageConfig configuration for metadata storage
 */
class ImpressionDataSourceProvider(
  private val impressionMetadataStub: ImpressionMetadataServiceCoroutineStub,
  private val dataProvider: String,
  private val impressionsMetadataStorageConfig: StorageConfig,
) {

  /**
   * Lists impression data sources for an event group within a period.
   *
   * @param eventGroupReferenceId event group reference identifier.
   * @param period closed-open time interval in UTC.
   * @return sources covering the requested period; empty if the period maps to no dates.
   * @throws ImpressionReadException if a required metadata blob does not exist.
   * @throws com.google.protobuf.InvalidProtocolBufferException if a metadata blob is present but
   *   contains invalid `BlobDetails`.
   */
  suspend fun listImpressionDataSources(
    modelLine: String,
    eventGroupReferenceId: String,
    period: Interval,
  ): List<ImpressionDataSource> {
    logger.info("Listing impression Data Sources...")
    val impressionMetadata: Flow<ImpressionMetadata> =
      getImpressionsMetadata(modelLine, eventGroupReferenceId, period)
    return impressionMetadata
      .map { metadata ->
        logger.info("Processing impression metadata: $metadata")
        val blobDetails =
          BlobDetailsLoader.load(metadata.blobUri, impressionsMetadataStorageConfig)

        ImpressionDataSource(
          modelLine = modelLine,
          eventGroupReferenceId = eventGroupReferenceId,
          interval = metadata.interval,
          blobDetails = blobDetails,
        )
      }
      .toList()
  }

  /**
   * Resolve a path to a blob details protobuf record.
   *
   * @param reportModelLine the model line
   * @param date the of the event data
   * @param egReferenceId referenced event group
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  fun getImpressionsMetadata(
    reportModelLine: String,
    egReferenceId: String,
    period: Interval,
  ): Flow<ImpressionMetadata> {

    logger.info("Resolving path for impression metadata: $reportModelLine, $egReferenceId, $period")
    return impressionMetadataStub
      .listResources { pageToken: String ->
        val response =
          try {
            impressionMetadataStub.listImpressionMetadata(
              listImpressionMetadataRequest {
                parent = dataProvider
                filter =
                  ListImpressionMetadataRequestKt.filter {
                    modelLine = reportModelLine
                    eventGroupReferenceId = egReferenceId
                    intervalOverlaps = period
                  }
                this.pageToken = pageToken
              }
            )
          } catch (e: StatusException) {
            throw Exception("Error listing EventGroups", e)
          }
        ResourceList(response.impressionMetadataList, response.nextPageToken)
      }
      .flattenConcat()
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
