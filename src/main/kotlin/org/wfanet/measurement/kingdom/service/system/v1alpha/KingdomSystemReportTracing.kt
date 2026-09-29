/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.system.v1alpha

import io.opentelemetry.api.common.AttributesBuilder
import io.opentelemetry.api.trace.Span
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.telemetry.ReportTraceAttributes
import org.wfanet.measurement.system.v1alpha.ComputationKey
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.StageAttempt

/** Adds Computation and Duchy attributes parsed from [participantName], when valid. */
fun AttributesBuilder.putComputationParticipantName(participantName: String): AttributesBuilder {
  val key = ComputationParticipantKey.fromName(participantName) ?: return this
  return put(ReportTraceAttributes.COMPUTATION_NAME, ComputationKey(key.computationId).toName())
    .put(ReportTraceAttributes.DUCHY_ID, key.duchyId)
}

/** Adds the stage name and attempt number from [stageAttempt]. */
fun AttributesBuilder.putComputationStageAttempt(stageAttempt: StageAttempt): AttributesBuilder {
  return put(ReportTraceAttributes.COMPUTATION_STAGE, stageAttempt.stageName)
    .put(ReportTraceAttributes.COMPUTATION_STAGE_ATTEMPT, stageAttempt.attemptNumber)
}

/** Adds a public Measurement name when both external IDs are populated. */
fun Span.setMeasurementName(externalMeasurementConsumerId: Long, externalMeasurementId: Long) {
  if (externalMeasurementConsumerId == 0L || externalMeasurementId == 0L) {
    return
  }
  setAttribute(
    ReportTraceAttributes.MEASUREMENT_NAME,
    MeasurementKey(
        externalIdToApiId(externalMeasurementConsumerId),
        externalIdToApiId(externalMeasurementId),
      )
      .toName(),
  )
}
