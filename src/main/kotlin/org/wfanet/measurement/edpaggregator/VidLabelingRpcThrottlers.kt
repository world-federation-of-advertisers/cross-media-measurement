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

package org.wfanet.measurement.edpaggregator

import java.time.Duration
import org.wfanet.measurement.common.throttler.MaximumRateThrottler
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.common.toDuration

/**
 * Process-scoped throttlers for outbound VID Labeling RPCs.
 *
 * Each throttler is shared by every coroutine in one process, so concurrent fan-out is converted
 * into a paced sequence for that downstream service class. These limits do not form a global
 * deployment quota: the aggregate ceiling is the per-process rate multiplied by the number of
 * concurrently running function instances or worker VMs.
 *
 * Defaults are intentionally conservative and configurable. The Kingdom interval yields a 2-QPS
 * rate, which stays below the dedicated 5-QPS Kingdom method buckets when both Cloud Functions are
 * active. Metadata reads use 10 QPS to keep per-file point lookups from dominating worker startup,
 * while metadata writes use 5 QPS because high-volume creation is already batched. Secure
 * Computation control-plane traffic uses 4 QPS so the maximum 24-worker fleet remains below 100 QPS
 * for methods shared by every worker. These are client-side pacing defaults, not statements of
 * downstream service capacity.
 */
data class VidLabelingRpcThrottlers(
  val kingdom: Throttler,
  val metadataRead: Throttler,
  val metadataWrite: Throttler,
  val controlPlane: Throttler,
) {
  companion object {
    const val KINGDOM_MIN_INTERVAL_ENV: String = "VID_LABELING_KINGDOM_RPC_MIN_INTERVAL"
    const val METADATA_READ_MIN_INTERVAL_ENV: String = "VID_LABELING_METADATA_READ_RPC_MIN_INTERVAL"
    const val METADATA_WRITE_MIN_INTERVAL_ENV: String =
      "VID_LABELING_METADATA_WRITE_RPC_MIN_INTERVAL"
    const val CONTROL_PLANE_MIN_INTERVAL_ENV: String = "VID_LABELING_CONTROL_PLANE_RPC_MIN_INTERVAL"

    val DEFAULT_KINGDOM_MIN_INTERVAL: Duration = Duration.ofMillis(500)
    val DEFAULT_METADATA_READ_MIN_INTERVAL: Duration = Duration.ofMillis(100)
    val DEFAULT_METADATA_WRITE_MIN_INTERVAL: Duration = Duration.ofMillis(200)
    val DEFAULT_CONTROL_PLANE_MIN_INTERVAL: Duration = Duration.ofMillis(250)

    fun fromMinimumIntervals(
      kingdom: Duration = DEFAULT_KINGDOM_MIN_INTERVAL,
      metadataRead: Duration = DEFAULT_METADATA_READ_MIN_INTERVAL,
      metadataWrite: Duration = DEFAULT_METADATA_WRITE_MIN_INTERVAL,
      controlPlane: Duration = DEFAULT_CONTROL_PLANE_MIN_INTERVAL,
    ): VidLabelingRpcThrottlers {
      require(kingdom > Duration.ZERO) { "Kingdom RPC minimum interval must be positive" }
      require(metadataRead > Duration.ZERO) {
        "Metadata read RPC minimum interval must be positive"
      }
      require(metadataWrite > Duration.ZERO) {
        "Metadata write RPC minimum interval must be positive"
      }
      require(controlPlane > Duration.ZERO) {
        "Control-plane RPC minimum interval must be positive"
      }
      return VidLabelingRpcThrottlers(
        kingdom = MaximumRateThrottler(rateFor(kingdom)),
        metadataRead = MaximumRateThrottler(rateFor(metadataRead)),
        metadataWrite = MaximumRateThrottler(rateFor(metadataWrite)),
        controlPlane = MaximumRateThrottler(rateFor(controlPlane)),
      )
    }

    fun fromEnvironment(getenv: (String) -> String? = System::getenv): VidLabelingRpcThrottlers =
      fromMinimumIntervals(
        kingdom = getenv(KINGDOM_MIN_INTERVAL_ENV)?.toDuration() ?: DEFAULT_KINGDOM_MIN_INTERVAL,
        metadataRead =
          getenv(METADATA_READ_MIN_INTERVAL_ENV)?.toDuration()
            ?: DEFAULT_METADATA_READ_MIN_INTERVAL,
        metadataWrite =
          getenv(METADATA_WRITE_MIN_INTERVAL_ENV)?.toDuration()
            ?: DEFAULT_METADATA_WRITE_MIN_INTERVAL,
        controlPlane =
          getenv(CONTROL_PLANE_MIN_INTERVAL_ENV)?.toDuration() ?: DEFAULT_CONTROL_PLANE_MIN_INTERVAL,
      )

    private fun rateFor(minimumInterval: Duration): Double =
      1_000_000_000.0 / minimumInterval.toNanos()
  }
}
