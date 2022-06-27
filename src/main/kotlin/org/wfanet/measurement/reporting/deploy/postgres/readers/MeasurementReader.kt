// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.postgres.readers

import com.google.protobuf.ByteString
import io.r2dbc.spi.Row
import kotlinx.coroutines.flow.firstOrNull
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.StatementBuilder.Companion.statementBuilder
import org.wfanet.measurement.common.db.r2dbc.get
import org.wfanet.measurement.common.db.r2dbc.getValue
import org.wfanet.measurement.internal.reporting.Measurement
import org.wfanet.measurement.internal.reporting.measurement
import org.wfanet.measurement.reporting.service.internal.MeasurementNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportingInternalException

class MeasurementReader {
  data class Result(val measurement: Measurement)

  private val baseSql: String =
    """
    SELECT
      MeasurementConsumerReferenceId,
      MeasurementReferenceId,
      State,
      Failure,
      Result
    FROM
      Measurements
    """

  fun translate(row: Row): Result = Result(buildMeasurement(row))

  /**
   * Reads a Measurement using reference IDs.
   *
   * Throws a subclass of [ReportingInternalException].
   * @throws [MeasurementNotFoundException] Measurement not found.
   */
  suspend fun readMeasurementByReferenceIds(
    readContext: ReadContext,
    measurementConsumerReferenceId: String,
    measurementReferenceId: String
  ): Result {
    val builder =
      statementBuilder(
        baseSql +
          """
        WHERE MeasurementConsumerReferenceId = $1
          AND MeasurementReferenceId = $2
        """
      ) {
        bind("$1", measurementConsumerReferenceId)
        bind("$2", measurementReferenceId)
      }

    return readContext.executeQuery(builder).consume(::translate).firstOrNull()
      ?: throw MeasurementNotFoundException()
  }

  private fun buildMeasurement(row: Row): Measurement {
    return measurement {
      measurementConsumerReferenceId = row.getValue("MeasurementConsumerReferenceId")
      measurementReferenceId = row.getValue("MeasurementReferenceId")
      state = Measurement.State.forNumber(row.getValue("State"))
      val failure = row.get("Failure", ByteString::class)
      if (failure != null) {
        this.failure = Measurement.Failure.parseFrom(failure)
      }
      val result = row.get("Result", ByteString::class)
      if (result != null) {
        this.result = Measurement.Result.parseFrom(result)
      }
    }
  }
}
