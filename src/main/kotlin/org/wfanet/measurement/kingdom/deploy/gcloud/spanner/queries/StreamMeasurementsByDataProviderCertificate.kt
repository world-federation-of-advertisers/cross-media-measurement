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
import org.wfanet.measurement.gcloud.spanner.toProtoEnumArray
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.MeasurementDetailsReader

class StreamMeasurementsByDataProviderCertificate(
  dataProviderCertificateId: InternalId,
  pendingMeasurementStates: List<Measurement.State>
) : SimpleSpannerQuery<MeasurementDetailsReader.Result>() {
  override val reader =
    MeasurementDetailsReader().fillStatementBuilder {
      appendClause("JOIN Requisitions USING (MeasurementConsumerId, MeasurementId)")
      appendClause(
        """
        WHERE Requisitions.DataProviderCertificateId = @dataProviderCertificateId
          AND Requisitions.State in UNNEST(@requisitionStates)
          AND Measurements.State in UNNEST(@pendingStates)
        """
          .trimIndent()
      )
      bind("dataProviderCertificateId").to(dataProviderCertificateId.value)
      bind("requisitionStates")
        .toProtoEnumArray(listOf(Requisition.State.PENDING_PARAMS, Requisition.State.UNFULFILLED))
      bind("pendingStates").toProtoEnumArray(pendingMeasurementStates)
    }
}
