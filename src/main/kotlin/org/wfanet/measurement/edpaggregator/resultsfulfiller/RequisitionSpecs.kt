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
import com.google.protobuf.Descriptors.Descriptor
import com.google.protobuf.TypeRegistry
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import org.projectnessie.cel.Program
import org.projectnessie.cel.common.types.BoolT
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters

/**
 * Utility functions for working with VIDs (Virtual IDs) in the EDP Aggregator.
 */
object RequisitionSpecs {

  /**
   * Retrieves sampled VIDs from a requisition specification based on a sampling interval.
   *
   * @param requisitionSpec The requisition specification containing event groups
   * @param vidSamplingInterval The sampling interval to filter VIDs
   * @param typeRegistry The registry for looking up protobuf descriptors
   * @param kmsClient The KMS client for encryption operations
   * @param impressionsStorageConfig Configuration for impressions storage
   * @param impressionMetadataStorageConfig Configuration for impression metadata storage
   * @param labeledImpressionMetadataPrefix Prefix for labeled impression metadata
   * @return A Flow of sampled VIDs (Long values)
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For flatMapConcat
  suspend fun getSampledVids(
    requisitionSpec: RequisitionSpec,
    vidSamplingInterval: MeasurementSpec.VidSamplingInterval,
    typeRegistry: TypeRegistry,
    kmsClient: KmsClient,
    impressionsStorageConfig: StorageConfig,
    impressionMetadataStorageConfig: StorageConfig,
    labeledImpressionMetadataPrefix: String
  ): Flow<Long> {
    val vidSamplingIntervalStart = vidSamplingInterval.start
    val vidSamplingIntervalWidth = vidSamplingInterval.width
    require(vidSamplingIntervalWidth > 0 && vidSamplingIntervalWidth <= 1.0) {
      "Invalid vidSamplingIntervalWidth $vidSamplingIntervalWidth"
    }
    require(
      vidSamplingIntervalStart < 1 &&
        vidSamplingIntervalStart >= 0 &&
        vidSamplingIntervalWidth > 0 &&
        vidSamplingIntervalStart + vidSamplingIntervalWidth <= 1
    ) {
      "Invalid vidSamplingInterval: start = $vidSamplingIntervalStart, width = " +
        "$vidSamplingIntervalWidth"
    }

    // Create an EventReader to read labeled impressions
    val eventReader = EventReader(
      kmsClient,
      impressionsStorageConfig,
      impressionMetadataStorageConfig,
      labeledImpressionMetadataPrefix
    )

    // Return a Flow that processes event groups and extracts valid VIDs
    return requisitionSpec.events.eventGroupsList
      .asFlow()
      .flatMapConcat { eventGroup ->
        val collectionInterval = eventGroup.value.collectionInterval

        // Create a VidFilter to filter labeled impressions
        val vidFilter = VidFilter(
          eventGroup.value.filter,
          collectionInterval,
          vidSamplingIntervalStart,
          vidSamplingIntervalWidth,
          typeRegistry
        )

        // Get labeled impressions and filter them
        val labeledImpressions = eventReader.getLabeledImpressionsFlow(
          collectionInterval,
          eventGroup.key
        )

        vidFilter.filterAndExtractVids(labeledImpressions)
      }
  }
}
