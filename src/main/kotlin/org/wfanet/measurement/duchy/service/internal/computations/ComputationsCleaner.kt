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
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.wfanet.measurement.internal.duchy.deleteOutdatedComputationsRequest

class ComputationsCleaner(
  private val computationsService: ComputationsService,
  ttlDays: Long,
  periodSeconds: Long,
  coroutineContext: CoroutineContext = Dispatchers.Default,
) {
  private val backgroundScope =
    CoroutineScope(coroutineContext + CoroutineName(javaClass.simpleName))

  private lateinit var cleanerJob: Job

  private val period = Duration.ofSeconds(periodSeconds).toMillis()
  private val ttlSecond = Duration.ofDays(ttlDays).toSeconds()

  fun start() {
    if (period == 0L) {
      return
    }
    cleanerJob =
      backgroundScope.launch {
        while (true) {
          delay(period)
          logger.info("ComputationCleaner task starts...")
          val response =
            computationsService.deleteOutdatedComputations(
              deleteOutdatedComputationsRequest {
                this.ttlSecond = this@ComputationsCleaner.ttlSecond
              }
            )
          logger.info("ComputationCleaner task finishes. ${response.count} Computations cleaned")
        }
      }
  }

  suspend fun stop() {
    if (period == 0L) {
      return
    }
    cleanerJob.cancelAndJoin()
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
