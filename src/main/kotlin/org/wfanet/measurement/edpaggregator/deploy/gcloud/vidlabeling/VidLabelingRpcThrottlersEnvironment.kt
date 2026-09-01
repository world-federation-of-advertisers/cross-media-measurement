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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.vidlabeling

import org.wfanet.measurement.common.toDuration
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers

/** Loads VID Labeling RPC throttler configuration from Cloud Function environment variables. */
object VidLabelingRpcThrottlersEnvironment {
  const val KINGDOM_MIN_INTERVAL_ENV: String = "VID_LABELING_KINGDOM_RPC_MIN_INTERVAL"
  const val METADATA_READ_MIN_INTERVAL_ENV: String = "VID_LABELING_METADATA_READ_RPC_MIN_INTERVAL"
  const val METADATA_WRITE_MIN_INTERVAL_ENV: String = "VID_LABELING_METADATA_WRITE_RPC_MIN_INTERVAL"
  const val CONTROL_PLANE_MIN_INTERVAL_ENV: String = "VID_LABELING_CONTROL_PLANE_RPC_MIN_INTERVAL"

  fun load(getenv: (String) -> String? = System::getenv): VidLabelingRpcThrottlers =
    VidLabelingRpcThrottlers.fromMinimumIntervals(
      kingdom =
        getenv(KINGDOM_MIN_INTERVAL_ENV)?.toDuration()
          ?: VidLabelingRpcThrottlers.DEFAULT_KINGDOM_MIN_INTERVAL,
      metadataRead =
        getenv(METADATA_READ_MIN_INTERVAL_ENV)?.toDuration()
          ?: VidLabelingRpcThrottlers.DEFAULT_METADATA_READ_MIN_INTERVAL,
      metadataWrite =
        getenv(METADATA_WRITE_MIN_INTERVAL_ENV)?.toDuration()
          ?: VidLabelingRpcThrottlers.DEFAULT_METADATA_WRITE_MIN_INTERVAL,
      controlPlane =
        getenv(CONTROL_PLANE_MIN_INTERVAL_ENV)?.toDuration()
          ?: VidLabelingRpcThrottlers.DEFAULT_CONTROL_PLANE_MIN_INTERVAL,
    )
}
