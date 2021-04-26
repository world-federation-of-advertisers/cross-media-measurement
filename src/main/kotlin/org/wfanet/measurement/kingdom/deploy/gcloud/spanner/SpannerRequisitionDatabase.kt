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
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.kingdom.db.RequisitionDatabase
import org.wfanet.measurement.kingdom.db.RequisitionUpdate
import org.wfanet.measurement.kingdom.db.StreamRequisitionsFilter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamRequisitions
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RequisitionReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateRequisition
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.FulfillRequisition
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.RefuseRequisition

class SpannerRequisitionDatabase(
  clock: Clock,
  idGenerator: IdGenerator,
  client: AsyncDatabaseClient
) : RequisitionDatabase, BaseSpannerDatabase(clock, idGenerator, client) {
  override suspend fun createRequisition(requisition: Requisition): Requisition {
    return CreateRequisition(requisition).execute()
  }

  override suspend fun getRequisition(externalRequisitionId: ExternalId): Requisition? {
    return RequisitionReader()
      .readExternalIdOrNull(client.singleUse(), externalRequisitionId)
      ?.requisition
  }

  override suspend fun fulfillRequisition(
    externalRequisitionId: ExternalId,
    duchyId: String
  ): RequisitionUpdate {
    return FulfillRequisition(externalRequisitionId, duchyId).execute()
  }

  override suspend fun refuseRequisition(
    externalRequisitionId: ExternalId,
    refusal: RequisitionDetails.Refusal
  ): RequisitionUpdate {
    return RefuseRequisition(externalRequisitionId, refusal).execute()
  }

  override fun streamRequisitions(
    filter: StreamRequisitionsFilter,
    limit: Long
  ): Flow<Requisition> {
    return StreamRequisitions(filter, limit).execute()
  }
}
