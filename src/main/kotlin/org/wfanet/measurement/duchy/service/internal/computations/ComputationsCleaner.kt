// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.service.internal.computations

import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.deleteOutdatedComputationsRequest
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2.Stage

class ComputationsCleaner(
    private val computationsService: ComputationsCoroutineStub,
    private val timeToLive: Duration,
    private val dryRun: Boolean = false,
) {

  fun run() {
    if (timeToLive.toMillis() == 0L) {
      logger.warning("Computation TTL cannot be 0. TTL=${timeToLive}")
      return
    }

    logger.info("ComputationCleaner task starts. TTL=${timeToLive}. dryRun=$dryRun")
    val response = runBlocking {
      computationsService.deleteOutdatedComputations(
          deleteOutdatedComputationsRequest {
            timeToLive = this@ComputationsCleaner.timeToLive.toProtoDuration()
            stages += Stage.COMPLETE.toProtocolStage()
            dryRun = this@ComputationsCleaner.dryRun
          })
    }
    logger.info(
        "ComputationCleaner task finishes. ${response.count} Computations " +
            if (dryRun) "to delete" else "deleted")
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
