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
import org.wfanet.measurement.common.toDuration

/** Parses complete human-readable durations used by VID Labeling RPC throttlers. */
object VidLabelingRpcDurationParser {
  fun parse(value: String): Duration {
    require(HUMAN_READABLE_DURATION_PATTERN.matches(value)) {
      "Value must be a complete human-readable duration"
    }
    return value.toDuration()
  }

  private val HUMAN_READABLE_DURATION_PATTERN = Regex("(?:\\d+(?:ns|ms|s|m|h|d))+")
}
