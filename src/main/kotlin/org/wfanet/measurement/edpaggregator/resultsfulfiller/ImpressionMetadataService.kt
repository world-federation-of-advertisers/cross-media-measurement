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
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails

/**
 * Describes an impression data source for a specific time interval.
 *
 * The interval is expressed as a closed-open [Interval] in UTC, and the
 * [blobDetails] provides the information required to read and decrypt the
 * corresponding impression blob.
 */
data class ImpressionDataSource(
  val interval: Interval,
  val blobDetails: BlobDetails,
)

/**
 * Provides impression metadata for event groups over time periods.
 *
 * Implementations may retrieve metadata from storage or remote APIs. Callers
 * should not assume details of any underlying storage layout.
 */
interface ImpressionMetadataService {
  /**
   * Lists impression data sources for an event group within a time period.
   *
   * Returns one or more sources when the period spans multiple shards (for
   * example, daily partitions).
   *
   * @param eventGroupReferenceId event group reference identifier.
   * @param period time period (closed-open) in UTC.
   * @return impression data sources covering the requested period.
   */
  suspend fun listImpressionDataSources(
    eventGroupReferenceId: String,
    period: Interval,
  ): List<ImpressionDataSource>
}
