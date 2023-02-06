// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.loadtest.dataprovider

import java.util.logging.Logger
import kotlin.random.Random
import org.wfanet.measurement.api.v2alpha.RequisitionSpec.EventFilter
import org.wfanet.measurement.api.v2alpha.TimeInterval

data class SketchGenerationParams(
  val reach: Int,
  val universeSize: Int,
)

/** Fulfill the query with randomly generated ids. */
class RandomEventQuery(private val sketchGenerationParams: SketchGenerationParams) : EventQuery {

  /** Returns VIDs generated from random values, ignoring [timeInterval] and [eventFilter]. */
  override fun getUserVirtualIds(
    timeInterval: TimeInterval,
    eventFilter: EventFilter
  ): Sequence<Long> {
    // TODO(@alberthsuu): Generate eventId, deduplicate the list of (eventId, vid), and
    // return the vids in case eventGroups overlap in some way

    val random = Random(1)

    logger.info("Generating random VIDs from RandomEventQuery...")

    return sequence {
      for (i in 1..sketchGenerationParams.reach) {
        yield(random.nextInt(1, sketchGenerationParams.universeSize + 1).toLong())
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
