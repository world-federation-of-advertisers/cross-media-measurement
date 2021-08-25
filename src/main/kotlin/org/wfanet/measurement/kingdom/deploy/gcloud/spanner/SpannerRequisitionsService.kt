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

import io.grpc.Status
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.GetRequisitionByDataProviderIdRequest
import org.wfanet.measurement.internal.kingdom.GetRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamRequisitions
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RequisitionReader

class SpannerRequisitionsService(private val client: AsyncDatabaseClient) :
  RequisitionsCoroutineImplBase() {

  override suspend fun getRequisition(request: GetRequisitionRequest): Requisition {
    return RequisitionReader()
      .readByExternalId(
        client.singleUse(),
        externalMeasurementConsumerId = request.externalMeasurementConsumerId,
        externalMeasurementId = request.externalMeasurementId,
        externalRequisitionId = request.externalRequisitionId
      )
      ?: failGrpc(Status.NOT_FOUND) { "Requisition not found" }
  }

  override suspend fun getRequisitionByDataProviderId(
    request: GetRequisitionByDataProviderIdRequest
  ): Requisition {
    return RequisitionReader()
      .readByExternalDataProviderId(
        client.singleUse(),
        externalDataProviderId = request.externalDataProviderId,
        externalRequisitionId = request.externalRequisitionId
      )
      ?: failGrpc(Status.NOT_FOUND) { "Requisition not found" }
  }

  override fun streamRequisitions(request: StreamRequisitionsRequest): Flow<Requisition> {
    val requestFilter = request.filter
    if (requestFilter.externalMeasurementId != 0L) {
      grpcRequire(requestFilter.externalMeasurementConsumerId != 0L) {
        "external_measurement_consumer_id must be specified if external_measurement_id is specified"
      }
    }

    return StreamRequisitions(requestFilter, request.limit).execute(client.singleUse())
  }
}
