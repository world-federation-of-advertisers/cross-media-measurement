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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.protobuf.timestamp
import com.google.protobuf.util.Timestamps
import com.google.type.Interval
import com.google.type.interval
import org.wfanet.measurement.api.v2alpha.ModelLine

/**
 * Returns true if the interval formed by the active timestamps of the [ModelLine] contains
 * the [Interval] interval, and returns false otherwise.
 */
operator fun ModelLine.contains(interval: Interval): Boolean {
  val source = this
  val modelLineActiveInterval = interval {
    startTime = source.activeStartTime
    if (source.hasActiveEndTime()) {
      endTime = source.activeEndTime
    } else {
      endTime = timestamp {
        // Max seconds
        seconds = 253402300799
      }
    }
  }

  return modelLineActiveInterval.contains(interval)
}

/**
 * Returns true if the [Interval] contains the [Interval] interval, and returns false otherwise.
 */
fun Interval.contains(interval: Interval): Boolean {
  return (Timestamps.compare(interval.startTime, this.startTime) >= 0 &&
    Timestamps.compare(interval.endTime, this.endTime) <= 0)
}

/**
 * Returns true if every [Interval] in the list contains the [Interval] interval, and returns false
 * otherwise.
 */
fun List<Interval>.containsInterval(interval: Interval): Boolean {
  for (i in this) {
    if (i.contains(interval)) {
      continue
    } else return false
  }
  return true
}
