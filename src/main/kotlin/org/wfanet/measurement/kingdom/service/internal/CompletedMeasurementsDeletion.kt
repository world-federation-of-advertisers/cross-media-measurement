/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.internal

import java.time.Clock
import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.DeleteMeasurementRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.batchDeleteMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.deleteMeasurementRequest
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest

private val COMPLETED_MEASUREMENT_STATES =
  listOf(Measurement.State.SUCCEEDED, Measurement.State.FAILED, Measurement.State.CANCELLED)

class CompletedMeasurementsDeletion(
  private val measurementsService: MeasurementsCoroutineStub,
  private val timeToLive: Duration,
  private val dryRun: Boolean,
) {

  fun run() {
    if (timeToLive.toMillis() == 0L) {
      logger.warning("Time to live cannot be 0. TTL=$timeToLive")
    }
    val currentTime = Clock.systemUTC().instant()
    runBlocking {
      val measurementsToDelete: List<Measurement> =
        measurementsService
          .streamMeasurements(
            streamMeasurementsRequest {
              filter =
                StreamMeasurementsRequestKt.filter {
                  states += COMPLETED_MEASUREMENT_STATES
                  updatedBefore = currentTime.minusMillis(timeToLive.toMillis()).toProtoTime()
                }
            }
          )
          .toList()

      val deleteRequests: List<DeleteMeasurementRequest> =
        measurementsToDelete.map {
          deleteMeasurementRequest {
            externalMeasurementId = it.externalMeasurementId
            externalMeasurementConsumerId = it.externalMeasurementConsumerId
            etag = it.etag
          }
        }

      if (dryRun) {
        logger.info(
          "CompletedMeasurementsDeletion dry run requests marked for deletion $deleteRequests"
        )
      } else {
        measurementsService.batchDeleteMeasurements(
          batchDeleteMeasurementsRequest { requests += deleteRequests }
        )
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
