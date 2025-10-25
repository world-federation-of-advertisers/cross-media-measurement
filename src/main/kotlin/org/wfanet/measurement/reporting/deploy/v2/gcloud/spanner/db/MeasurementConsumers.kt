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

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Struct
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumer
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException

data class MeasurementConsumerResult(
  val measurementConsumerId: Long,
  val measurementConsumer: MeasurementConsumer,
)

/**
 * Reads a [MeasurementConsumer] by its cmms ID.
 *
 * @throws MeasurementConsumerNotFoundException
 */
suspend fun AsyncDatabaseClient.ReadContext.getMeasurementConsumerByCmmsMeasurementConsumerId(
  cmmsMeasurementConsumerId: String
): MeasurementConsumerResult {
  val row: Struct =
    readRowUsingIndex(
      "MeasurementConsumers",
      "MeasurementConsumersByCmmsMeasurementConsumerId",
      Key.of(cmmsMeasurementConsumerId),
      "MeasurementConsumerId",
    ) ?: throw MeasurementConsumerNotFoundException(cmmsMeasurementConsumerId)

  return MeasurementConsumerResult(
    row.getLong("MeasurementConsumerId"),
    measurementConsumer { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId },
  )
}

/** Buffers an insert mutation for the MeasurementConsumers table. */
fun AsyncDatabaseClient.TransactionContext.insertMeasurementConsumer(
  measurementConsumerId: Long,
  measurementConsumer: MeasurementConsumer,
) {
  bufferInsertMutation("MeasurementConsumers") {
    set("MeasurementConsumerId").to(measurementConsumerId)
    set("CmmsMeasurementConsumerId").to(measurementConsumer.cmmsMeasurementConsumerId)
  }
}

/** Returns whether a [MeasurementConsumer] with the specified [measurementConsumerId] exists. */
suspend fun AsyncDatabaseClient.ReadContext.measurementConsumerExists(
  measurementConsumerId: Long
): Boolean {
  return readRow(
    "MeasurementConsumers",
    Key.of(measurementConsumerId),
    listOf("MeasurementConsumerId"),
  ) != null
}
