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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.gcloud.spanner.toInt64Array
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementDetailsReader

class StreamMeasurementsByDuchyCertificate(
  duchyCertificateId: InternalId,
  pendingMeasurementStates: List<Measurement.State>,
) : SimpleSpannerQuery<MeasurementDetailsReader.Result>() {
  override val reader =
    MeasurementDetailsReader().fillStatementBuilder {
      appendClause("JOIN ComputationParticipants USING (MeasurementConsumerId, MeasurementId)")
      appendClause(
        """
        WHERE ComputationParticipants.CertificateId = @duchyCertificateId
          AND ComputationParticipants.State = @computationParticipantState
          AND Measurements.State in UNNEST(@pendingStates)
        """
          .trimIndent()
      )
      bind("duchyCertificateId").to(duchyCertificateId.value)
      bind("computationParticipantState")
        .toInt64(ComputationParticipant.State.REQUISITION_PARAMS_SET)
      bind("pendingStates").toInt64Array(pendingMeasurementStates)
    }
}
