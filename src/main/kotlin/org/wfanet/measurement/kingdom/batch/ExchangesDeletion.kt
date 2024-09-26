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
import java.time.LocalDate
import java.time.ZoneOffset
import java.util.logging.Logger
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.internal.kingdom.DeleteExchangeRequest
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangesGrpcKt
import org.wfanet.measurement.internal.kingdom.StreamExchangesRequestKt
import org.wfanet.measurement.internal.kingdom.batchDeleteExchangesRequest
import org.wfanet.measurement.internal.kingdom.deleteExchangeRequest
import org.wfanet.measurement.internal.kingdom.streamExchangesRequest

private const val MAX_BATCH_DELETE = 1000

class ExchangesDeletion(
  private val exchangesService: ExchangesGrpcKt.ExchangesCoroutineStub,
  private val daysToLive: Long,
  private val dryRun: Boolean = false,
) {
  private val exchangeDeletionCounter: LongCounter =
    Instrumentation.meter
      .counterBuilder("${Instrumentation.ROOT_NAMESPACE}.retention.deleted_exchanges")
      .setUnit("{exchange}")
      .setDescription("Total number of exchanges deleted under retention policy")
      .build()

  fun run() {
    val currentDate = LocalDate.now(ZoneOffset.UTC)
    runBlocking {
      val exchangesToDelete: List<Exchange> =
        exchangesService
          .streamExchanges(
            streamExchangesRequest {
              filter =
                StreamExchangesRequestKt.filter {
                  dateBefore = currentDate.minusDays(daysToLive).toProtoDate()
                }
            }
          )
          .toList()

      if (dryRun) {
        logger.info { "Exchanges that would have been deleted $exchangesToDelete" }
      } else {
        for (batchExchangesToDelete in exchangesToDelete.chunked(MAX_BATCH_DELETE)) {
          val deleteRequests: List<DeleteExchangeRequest> =
            batchExchangesToDelete.map {
              deleteExchangeRequest {
                externalRecurringExchangeId = it.externalRecurringExchangeId
                date = it.date
              }
            }
          exchangesService.batchDeleteExchanges(
            batchDeleteExchangesRequest { requests += deleteRequests }
          )
          exchangeDeletionCounter.add(deleteRequests.size.toLong())
        }
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
