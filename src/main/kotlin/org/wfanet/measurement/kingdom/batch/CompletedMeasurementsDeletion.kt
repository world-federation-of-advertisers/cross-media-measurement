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

package org.wfanet.measurement.kingdom.batch

import io.opentelemetry.api.metrics.LongCounter
import java.time.Clock
import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.batchDeleteMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.deleteMeasurementRequest
import org.wfanet.measurement.internal.kingdom.measurementKey
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest

private val COMPLETED_MEASUREMENT_STATES =
  listOf(Measurement.State.SUCCEEDED, Measurement.State.FAILED, Measurement.State.CANCELLED)

class CompletedMeasurementsDeletion(
  private val measurementsService: MeasurementsCoroutineStub,
  private val maxToDeletePerRpc: Int,
  private val timeToLive: Duration,
  private val dryRun: Boolean = false,
  private val clock: Clock = Clock.systemUTC(),
) {
  private val completedMeasurementDeletionCounter: LongCounter =
    Instrumentation.meter
      .counterBuilder("${Instrumentation.ROOT_NAMESPACE}.retention.deleted_measurements")
      .setUnit("{measurement}")
      .setDescription("Total number of completed measurements deleted under retention policy")
      .build()

  fun run() = runBlocking {
    if (timeToLive.toMillis() == 0L) {
      logger.warning("Time to live cannot be 0. TTL=$timeToLive")
    }
    val currentTime = clock.instant()

    var previousPageEnd: StreamMeasurementsRequest.Filter.After? = null

    do {
      val after: StreamMeasurementsRequest.Filter.After? = previousPageEnd
      val measurementsToDelete: List<Measurement> =
        measurementsService
          .streamMeasurements(
            streamMeasurementsRequest {
              filter =
                StreamMeasurementsRequestKt.filter {
                  states += COMPLETED_MEASUREMENT_STATES
                  updatedBefore = currentTime.minus(timeToLive).toProtoTime()
                  if (after != null) {
                    this.after = after
                  }
                }
              limit = maxToDeletePerRpc
            }
          )
          .toList()
      val lastMeasurement = measurementsToDelete.last()
      previousPageEnd =
        StreamMeasurementsRequestKt.FilterKt.after {
          measurement = measurementKey {
            externalMeasurementConsumerId = lastMeasurement.externalMeasurementId
            externalMeasurementId = lastMeasurement.externalMeasurementId
          }
          updateTime = lastMeasurement.updateTime
        }

      if (dryRun) {
        logger.info { "Measurements that would have been deleted:" }
        for (measurement in measurementsToDelete) {
          logger.info { measurement.toString() }
        }
        continue
      }

      val request = batchDeleteMeasurementsRequest {
        for (measurement in measurementsToDelete) {
          requests += deleteMeasurementRequest {
            externalMeasurementId = measurement.externalMeasurementId
            externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
            etag = measurement.etag
          }
        }
      }
      measurementsService.batchDeleteMeasurements(request)
      completedMeasurementDeletionCounter.add(measurementsToDelete.size.toLong())
    } while (measurementsToDelete.isNotEmpty())
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
