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

import com.google.protobuf.TypeRegistry
import java.time.LocalDate
import java.time.ZoneId
import kotlin.streams.asSequence
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.common.toInstant

/** Utility functions for working with VIDs (Virtual IDs) in the EDP Aggregator. */
object RequisitionSpecs {

  /**
   * Retrieves sampled VIDs from a requisition specification based on a sampling interval.
   *
   * @param requisitionSpec The requisition specification containing event groups
   * @param vidSamplingInterval The sampling interval to filter VIDs
   * @param typeRegistry The registry for looking up protobuf descriptors
   * @param eventReader The EventReader to read labeled impressions
   * @return A Flow of sampled VIDs (Long values)
   */
  @OptIn(ExperimentalCoroutinesApi::class) // For flatMapConcat
  suspend fun getSampledVids(
    requisitionSpec: RequisitionSpec,
    vidSamplingInterval: MeasurementSpec.VidSamplingInterval,
    typeRegistry: TypeRegistry,
    eventReader: EventReader,
    zoneId: ZoneId = ZoneId.of("America/New_York"),
  ): Flow<Long> {
    val vidSamplingIntervalStart = vidSamplingInterval.start
    val vidSamplingIntervalWidth = vidSamplingInterval.width
    require(
      vidSamplingIntervalStart >= 0 &&
        vidSamplingIntervalStart < 1 &&
        vidSamplingIntervalWidth > 0 &&
        vidSamplingIntervalWidth <= 1
    ) {
      "Invalid vidSamplingInterval: start = $vidSamplingIntervalStart, width = " +
        "$vidSamplingIntervalWidth"
    }

    // Return a Flow that processes event groups and extracts valid VIDs
    return requisitionSpec.events.eventGroupsList.asFlow().flatMapConcat { eventGroup ->
      val collectionInterval = eventGroup.value.collectionInterval
      val startDate = LocalDate.ofInstant(collectionInterval.startTime.toInstant(), zoneId)
      val endDate = LocalDate.ofInstant(collectionInterval.endTime.toInstant(), zoneId)
      val dates = startDate.datesUntil(endDate).asSequence().asFlow()

      // Iterates through all dates up to the end date in the collection interval(inclusive)
      val impressions =
        dates.flatMapConcat { date -> eventReader.getLabeledImpressions(date, eventGroup.key) }

      VidFilter.filterAndExtractVids(
        impressions,
        vidSamplingIntervalStart,
        vidSamplingIntervalWidth,
        eventGroup.value.filter,
        collectionInterval,
        typeRegistry,
      )
    }
  }
}
