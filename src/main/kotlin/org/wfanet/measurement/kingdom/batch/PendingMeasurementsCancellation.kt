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
import org.wfanet.measurement.internal.kingdom.CancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.batchCancelMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.cancelMeasurementRequest
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest

private val PENDING_MEASUREMENT_STATES =
  listOf(
    Measurement.State.PENDING_COMPUTATION,
    Measurement.State.PENDING_PARTICIPANT_CONFIRMATION,
    Measurement.State.PENDING_REQUISITION_FULFILLMENT,
    Measurement.State.PENDING_REQUISITION_PARAMS,
  )
private const val MAX_BATCH_CANCEL = 1000

class PendingMeasurementsCancellation(
  private val measurementsService: MeasurementsCoroutineStub,
  private val timeToLive: Duration,
  private val dryRun: Boolean = false,
  private val clock: Clock = Clock.systemUTC(),
) {
  private val pendingMeasurementCancellationCounter: LongCounter =
    Instrumentation.meter
      .counterBuilder("${Instrumentation.ROOT_NAMESPACE}.retention.cancelled_measurements")
      .setUnit("{measurement}")
      .setDescription("Total number of pending measurements cancelled under retention policy")
      .build()

  fun run() {
    if (timeToLive.toMillis() == 0L) {
      logger.warning("Time to live cannot be 0. TTL=$timeToLive")
    }
    val currentTime = clock.instant()
    runBlocking {
      var measurementsToCancel: List<Measurement> =
        measurementsService
          .streamMeasurements(
            streamMeasurementsRequest {
              filter =
                StreamMeasurementsRequestKt.filter {
                  states += PENDING_MEASUREMENT_STATES
                  createdBefore = currentTime.minus(timeToLive).toProtoTime()
                }
            }
          )
          .toList()

      if (dryRun) {
        logger.info { "Measurements that would have been cancelled: $measurementsToCancel" }
      } else {
        while (measurementsToCancel.isNotEmpty()) {
          val batchMeasurementsToCancel = measurementsToCancel.take(MAX_BATCH_CANCEL)
          val cancelRequests: List<CancelMeasurementRequest> =
            batchMeasurementsToCancel.map {
              cancelMeasurementRequest {
                externalMeasurementId = it.externalMeasurementId
                externalMeasurementConsumerId = it.externalMeasurementConsumerId
                etag = it.etag
              }
            }
          measurementsService.batchCancelMeasurements(
            batchCancelMeasurementsRequest { requests += cancelRequests }
          )
          pendingMeasurementCancellationCounter.add(cancelRequests.size.toLong())

          measurementsToCancel =
            measurementsService
              .streamMeasurements(
                streamMeasurementsRequest {
                  filter =
                    StreamMeasurementsRequestKt.filter {
                      states += PENDING_MEASUREMENT_STATES
                      createdBefore = currentTime.minus(timeToLive).toProtoTime()
                    }
                }
              )
              .toList()
        }
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
