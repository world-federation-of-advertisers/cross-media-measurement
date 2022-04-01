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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import java.time.Clock
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.kingdom.deploy.common.service.KingdomDataServices

class SpannerDataServices(
  private val clock: Clock,
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : DataServices {
  override fun buildDataServices(): KingdomDataServices {
    return KingdomDataServices(
      SpannerAccountsService(idGenerator, client),
      SpannerApiKeysService(idGenerator, client),
      SpannerCertificatesService(idGenerator, client),
      SpannerDataProvidersService(idGenerator, client),
      SpannerModelProvidersService(idGenerator, client),
      SpannerEventGroupMetadataDescriptorsService(idGenerator, client),
      SpannerEventGroupsService(idGenerator, client),
      SpannerMeasurementConsumersService(idGenerator, client),
      SpannerMeasurementsService(idGenerator, client),
      SpannerPublicKeysService(idGenerator, client),
      SpannerRequisitionsService(idGenerator, client),
      SpannerComputationParticipantsService(idGenerator, client),
      SpannerMeasurementLogEntriesService(idGenerator, client),
      SpannerRecurringExchangesService(idGenerator, client),
      SpannerExchangesService(idGenerator, client),
      SpannerExchangeStepsService(clock, idGenerator, client),
      SpannerExchangeStepAttemptsService(clock, idGenerator, client)
    )
  }
}
