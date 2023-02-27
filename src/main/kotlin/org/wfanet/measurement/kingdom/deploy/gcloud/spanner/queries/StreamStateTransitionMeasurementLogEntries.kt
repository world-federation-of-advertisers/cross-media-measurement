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

import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.StateTransitionMeasurementLogEntryReader

class StreamStateTransitionMeasurementLogEntries(
  externalMeasurementId: ExternalId,
  externalMeasurementConsumerId: ExternalId
) : SimpleSpannerQuery<StateTransitionMeasurementLogEntryReader.Result>() {

  override val reader =
    StateTransitionMeasurementLogEntryReader().fillStatementBuilder {
      appendClause(
        """
        WHERE Measurements.ExternalMeasurementId = @externalMeasurementId
          AND MeasurementConsumers.ExternalMeasurementConsumerId = @externalMeasurementConsumerId
        """
          .trimIndent()
      )
      bind("externalMeasurementId").to(externalMeasurementId.value)
      bind("externalMeasurementConsumerId").to(externalMeasurementConsumerId.value)
    }
}
