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

import com.google.protobuf.Descriptors
import java.time.Clock
import java.time.Duration
import kotlin.coroutines.CoroutineContext
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.kingdom.deploy.common.service.KingdomDataServices

class SpannerDataServices(
  private val clock: Clock,
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  override val knownEventGroupMetadataTypes: Iterable<Descriptors.FileDescriptor> = emptyList(),
  private val maxEventGroupReadStaleness: Duration = Duration.ofSeconds(1L),
) : DataServices {
  override fun buildDataServices(coroutineContext: CoroutineContext): KingdomDataServices {
    return KingdomDataServices(
      SpannerAccountsService(idGenerator, client, coroutineContext),
      SpannerApiKeysService(idGenerator, client, coroutineContext),
      SpannerCertificatesService(idGenerator, client, coroutineContext),
      SpannerDataProvidersService(idGenerator, client, coroutineContext),
      SpannerModelProvidersService(idGenerator, client, coroutineContext),
      SpannerEventGroupMetadataDescriptorsService(
        idGenerator,
        client,
        knownEventGroupMetadataTypes,
        coroutineContext,
      ),
      SpannerEventGroupsService(idGenerator, client, maxEventGroupReadStaleness, coroutineContext),
      SpannerMeasurementConsumersService(idGenerator, client, coroutineContext),
      SpannerMeasurementsService(idGenerator, client, coroutineContext),
      SpannerPublicKeysService(idGenerator, client, coroutineContext),
      SpannerRequisitionsService(idGenerator, client, coroutineContext),
      SpannerComputationParticipantsService(idGenerator, client, coroutineContext),
      SpannerMeasurementLogEntriesService(idGenerator, client, coroutineContext),
      SpannerRecurringExchangesService(idGenerator, client, coroutineContext),
      SpannerExchangesService(idGenerator, client, coroutineContext),
      SpannerExchangeStepsService(clock, idGenerator, client, coroutineContext),
      SpannerExchangeStepAttemptsService(clock, idGenerator, client, coroutineContext),
      SpannerModelSuitesService(idGenerator, client, coroutineContext),
      SpannerModelLinesService(clock, idGenerator, client, coroutineContext),
      SpannerModelOutagesService(idGenerator, client, coroutineContext),
      SpannerModelReleasesService(idGenerator, client, coroutineContext),
      SpannerModelShardsService(idGenerator, client, coroutineContext),
      SpannerModelRolloutsService(clock, idGenerator, client, coroutineContext),
      SpannerPopulationsService(idGenerator, client, coroutineContext),
    )
  }
}
