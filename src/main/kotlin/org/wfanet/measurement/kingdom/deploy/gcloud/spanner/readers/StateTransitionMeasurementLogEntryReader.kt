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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Struct
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementStateLogEntryList
import org.wfanet.measurement.internal.kingdom.MeasurementStateLogEntryListKt
import org.wfanet.measurement.internal.kingdom.measurement
import org.wfanet.measurement.internal.kingdom.measurementLogEntry
import org.wfanet.measurement.internal.kingdom.measurementStateLogEntry
import org.wfanet.measurement.internal.kingdom.measurementStateLogEntryList

class StateTransitionMeasurementLogEntryReader :
  SpannerReader<StateTransitionMeasurementLogEntryReader.Result>() {

  data class Result(val measurementStateLogEntryList: MeasurementStateLogEntryList)

  override val baseSql: String =
    """
    SELECT
      Measurements.MeasurementId,
      Measurements.MeasurementConsumerId,
      ARRAY(
        SELECT AS STRUCT
          StateTransitionMeasurementLogEntries.CreateTime,
          StateTransitionMeasurementLogEntries.PreviousMeasurementState,
          StateTransitionMeasurementLogEntries.CurrentMeasurementState,
          StateTransitionMeasurementLogEntries.MeasurementConsumerId,
          StateTransitionMeasurementLogEntries.MeasurementId
        FROM
          StateTransitionMeasurementLogEntries
          JOIN MeasurementLogEntries USING (MeasurementConsumerId, MeasurementId, CreateTime)
        WHERE
          StateTransitionMeasurementLogEntries.MeasurementConsumerId = Measurements.MeasurementConsumerId
          AND StateTransitionMeasurementLogEntries.MeasurementId = Measurements.MeasurementId
        ORDER BY MeasurementLogEntries.CreateTime DESC
      ) AS StateTransitionMeasurementLogEntries
    FROM
      Measurements
      JOIN MeasurementConsumers USING (MeasurementConsumerId)
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(measurementStateLogEntryList { fillStateTransitionLogView(struct) })

  suspend fun readStateTransitionLogByExternalIds(
    readContext: AsyncDatabaseClient.ReadContext,
    externalMeasurementConsumerId: ExternalId,
    externalMeasurementId: ExternalId
  ): Result? {
    return fillStatementBuilder {
        appendClause(
          """
            WHERE ExternalMeasurementConsumerId = @externalMeasurementConsumerId
              AND ExternalMeasurementId = @externalMeasurementId
            """
        )
        bind("externalMeasurementConsumerId").to(externalMeasurementConsumerId.value)
        bind("externalMeasurementId").to(externalMeasurementId.value)
      }
      .execute(readContext)
      .singleOrNull()
  }

  private fun MeasurementStateLogEntryListKt.Dsl.fillStateTransitionLogView(struct: Struct) {
    val stateTransitionMeasurementStructs =
      struct.getStructList("StateTransitionMeasurementLogEntries")
    for (stateTransitionMeasurementStruct in stateTransitionMeasurementStructs) {
      measurementStateLogEntry += measurementStateLogEntry {
        this.currentState =
          stateTransitionMeasurementStruct.getProtoEnum(
            "CurrentMeasurementState",
            org.wfanet.measurement.internal.kingdom.Measurement.State::forNumber
          )
        this.previousState =
          if (stateTransitionMeasurementStruct.isNull("PreviousMeasurementState"))
            org.wfanet.measurement.internal.kingdom.Measurement.State.STATE_UNSPECIFIED
          else
            stateTransitionMeasurementStruct.getProtoEnum(
              "PreviousMeasurementState",
              org.wfanet.measurement.internal.kingdom.Measurement.State::forNumber
            )
        this.logEntry = measurementLogEntry {
          this.createTime = stateTransitionMeasurementStruct.getTimestamp("CreateTime").toProto()
          this.externalMeasurementId = stateTransitionMeasurementStruct.getLong("MeasurementId")
          this.externalMeasurementConsumerId =
            stateTransitionMeasurementStruct.getLong("MeasurementConsumerId")
        }
      }
    }
  }
}
