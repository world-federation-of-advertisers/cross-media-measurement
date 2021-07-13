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

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.toProtoEnumArray
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.SpannerReader

class GetExchangeStepByDataProvider(
  externalDataProviderId: ExternalId,
  states: List<ExchangeStep.State>
) : SpannerQuery<ExchangeStepReader.Result, ExchangeStep>() {
  override val reader: SpannerReader<ExchangeStepReader.Result> by lazy {
    ExchangeStepReader().withBuilder {
      appendClause("WHERE DataProviders.ExternalDataProviderId = @external_data_provider_id")
      bind("external_data_provider_id").to(externalDataProviderId.value)

      appendClause("AND ExchangeSteps.State IN UNNEST(@states)")
      bind("states").toProtoEnumArray(states)

      appendClause("ORDER BY ExchangeSteps.UpdateTime ASC")
      appendClause("LIMIT 1")
    }
  }

  override fun Flow<ExchangeStepReader.Result>.transform(): Flow<ExchangeStep> = map {
    it.exchangeStep
  }
}
