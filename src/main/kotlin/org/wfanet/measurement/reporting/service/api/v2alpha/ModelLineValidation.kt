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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.protobuf.timestamp
import com.google.protobuf.util.Timestamps
import com.google.type.Interval
import com.google.type.interval
import org.wfanet.measurement.api.v2alpha.ModelLine


/**
 * Returns true if the interval formed by the active timestamps of the [ModelLine] completely covers
 * the [Interval] to check against, and returns false otherwise.
 */
fun Interval.isFullyContainedWithin(modelLine: ModelLine): Boolean {
  val modelLineActiveInterval = interval {
    startTime = modelLine.activeStartTime
    if (modelLine.hasActiveEndTime()) {
      endTime = modelLine.activeEndTime
    } else {
      endTime = timestamp {
        // Max seconds
        seconds = 253402300799
      }
    }
  }

  return this.isFullyContainedWithin(modelLineActiveInterval)
}

/**
 * Returns true if the [Interval] interval completely covers the [Interval] to check against, and
 * returns false otherwise.
 */
fun Interval.isFullyContainedWithin(interval: Interval): Boolean {
  return (Timestamps.compare(this.startTime, interval.startTime) >= 0 &&
      Timestamps.compare(this.endTime, interval.endTime) <= 0)
}

/**
 * Returns true if the interval formed by the active timestamps of the [ModelLine] completely covers
 * the list of [Interval]s to check against, and returns false otherwise.
 */
fun List<Interval>.areFullyContainedWithin(modelLine: ModelLine): Boolean {
  for (interval in this) {
    if (interval.isFullyContainedWithin(modelLine)) {
      continue
    } else return false
  }
  return true
}

