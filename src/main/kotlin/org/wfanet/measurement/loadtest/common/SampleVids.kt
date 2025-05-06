/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.common

import com.google.protobuf.Message
import org.wfanet.measurement.loadtest.config.VidSampling
import org.wfanet.measurement.loadtest.dataprovider.EventQuery

fun sampleVids(
  eventQuery: EventQuery<Message>,
  eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
  vidSamplingIntervalStart: Float,
  vidSamplingIntervalWidth: Float,
): Iterable<Long> {
  require(vidSamplingIntervalWidth > 0 && vidSamplingIntervalWidth <= 1.0) {
    "Invalid vidSamplingIntervalWidth $vidSamplingIntervalWidth"
  }
  require(
    vidSamplingIntervalStart < 1 &&
      vidSamplingIntervalStart >= 0 &&
      vidSamplingIntervalWidth > 0 &&
      vidSamplingIntervalWidth <= 1
  ) {
    "Invalid vidSamplingInterval: start = $vidSamplingIntervalStart, width = " +
      "$vidSamplingIntervalWidth"
  }

  return eventGroupSpecs
    .asSequence()
    .flatMap { eventQuery.getUserVirtualIds(it) }
    .filter { vid ->
      VidSampling.sampler.vidIsInSamplingBucket(
        vid,
        vidSamplingIntervalStart,
        vidSamplingIntervalWidth,
      )
    }
    .asIterable()
}
