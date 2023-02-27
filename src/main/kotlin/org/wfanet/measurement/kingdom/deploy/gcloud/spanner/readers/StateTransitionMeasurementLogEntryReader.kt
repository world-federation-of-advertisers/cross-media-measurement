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
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.StateTransitionMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.measurementLogEntry
import org.wfanet.measurement.internal.kingdom.stateTransitionMeasurementLogEntry

class StateTransitionMeasurementLogEntryReader :
  SpannerReader<StateTransitionMeasurementLogEntryReader.Result>() {

  data class Result(val stateTransitionMeasurementLogEntry: StateTransitionMeasurementLogEntry)

  override val baseSql: String =
    """
    SELECT
      Measurements.ExternalMeasurementId,
      MeasurementConsumers.ExternalMeasurementConsumerId,
      MeasurementLogEntries.MeasurementLogDetails,
      MeasurementLogEntries.CreateTime,
      StateTransitionMeasurementLogEntries.PreviousMeasurementState,
      StateTransitionMeasurementLogEntries.CurrentMeasurementState,
      StateTransitionMeasurementLogEntries.MeasurementConsumerId,
      StateTransitionMeasurementLogEntries.MeasurementId
    FROM
      StateTransitionMeasurementLogEntries
      JOIN MeasurementLogEntries USING (MeasurementConsumerId, MeasurementId, CreateTime)
      JOIN Measurements USING (MeasurementConsumerId)
      JOIN MeasurementConsumers USING (MeasurementConsumerId)
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result =
    Result(buildStateTransitionMeasurementLogEntry(struct))

  private fun buildStateTransitionMeasurementLogEntry(
    struct: Struct
  ): StateTransitionMeasurementLogEntry {
    return stateTransitionMeasurementLogEntry {
      currentState = struct.getProtoEnum("CurrentMeasurementState", Measurement.State::forNumber)
      previousState = struct.getProtoEnum("PreviousMeasurementState", Measurement.State::forNumber)
      logEntry = measurementLogEntry {
        createTime = struct.getTimestamp("CreateTime").toProto()
        externalMeasurementId = struct.getLong("ExternalMeasurementId")
        externalMeasurementConsumerId = struct.getLong("ExternalMeasurementConsumerId")
        details =
          struct.getProtoMessage("MeasurementLogDetails", MeasurementLogEntry.Details.parser())
      }
    }
  }
}
