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

package org.wfanet.measurement.edpaggregator.dataavailability

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.metrics.LongGauge
import io.opentelemetry.api.metrics.Meter

/**
 * OpenTelemetry instruments for [MissingImpressionMetadataRecovery].
 *
 * @param meter Meter used to create the instruments.
 */
class MissingImpressionMetadataRecoveryMetrics(meter: Meter) {
  /** Finalized metadata blobs with no active or deleted resource. */
  val missingBlobsGauge: LongGauge =
    meter
      .gaugeBuilder("edpa.data_availability_recovery.missing_blobs")
      .setDescription("Finalized metadata blobs without ImpressionMetadata resources")
      .setUnit("{blob}")
      .ofLongs()
      .build()

  /** Deleted resources whose metadata blobs still exist. */
  val deletedRecordsWithBlobsGauge: LongGauge =
    meter
      .gaugeBuilder("edpa.data_availability_recovery.deleted_records_with_blobs")
      .setDescription("Deleted ImpressionMetadata resources whose blobs still exist")
      .setUnit("{blob}")
      .ofLongs()
      .build()

  /** Missing blobs in date folders that failed resynchronization. */
  val failedBlobsGauge: LongGauge =
    meter
      .gaugeBuilder("edpa.data_availability_recovery.failed_blobs")
      .setDescription("Missing ImpressionMetadata blobs that failed recovery in the current run")
      .setUnit("{blob}")
      .ofLongs()
      .build()

  companion object {
    /** Attribute containing the scanned EDP impression path. */
    val EDP_IMPRESSION_PATH_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("edpa.data_availability_recovery.edp_impression_path")
  }
}
