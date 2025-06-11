/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.DeleteModelOutageRequest
import org.wfanet.measurement.internal.kingdom.ModelOutage
import org.wfanet.measurement.internal.kingdom.ModelOutagesGrpcKt.ModelOutagesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamModelOutagesRequest
import org.wfanet.measurement.internal.kingdom.modelOutage
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelOutageInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelOutageNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelOutageStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamModelOutages
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateModelOutage
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.DeleteModelOutage

class SpannerModelOutagesService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ModelOutagesCoroutineImplBase(coroutineContext) {

  override suspend fun createModelOutage(request: ModelOutage): ModelOutage {
    grpcRequire(request.hasModelOutageStartTime() && request.hasModelOutageEndTime()) {
      "Outage interval is missing."
    }
    try {
      return CreateModelOutage(request).execute(client, idGenerator)
    } catch (e: ModelLineNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelLine not found.")
    } catch (e: ModelOutageInvalidArgsException) {
      throw e.asStatusRuntimeException(
        Status.Code.INVALID_ARGUMENT,
        e.message ?: "ModelOutageStartTime cannot precede ModelOutageEndTime.",
      )
    }
  }

  override suspend fun deleteModelOutage(request: DeleteModelOutageRequest): ModelOutage {
    grpcRequire(request.externalModelProviderId != 0L) { "ExternalModelProviderId unspecified" }
    grpcRequire(request.externalModelSuiteId != 0L) { "ExternalModelSuiteId unspecified" }
    grpcRequire(request.externalModelLineId != 0L) { "ExternalModelLineId unspecified" }
    grpcRequire(request.externalModelOutageId != 0L) { "ExternalModelOutageId unspecified" }

    val modelOutage = modelOutage {
      externalModelProviderId = request.externalModelProviderId
      externalModelSuiteId = request.externalModelSuiteId
      externalModelLineId = request.externalModelLineId
      externalModelOutageId = request.externalModelOutageId
    }

    try {
      return DeleteModelOutage(modelOutage).execute(client, idGenerator)
    } catch (e: ModelOutageNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelOutage not found.")
    } catch (e: ModelOutageStateIllegalException) {
      when (e.state) {
        ModelOutage.State.DELETED -> {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelOutage state is DELETED.")
        }
        ModelOutage.State.ACTIVE,
        ModelOutage.State.STATE_UNSPECIFIED,
        ModelOutage.State.UNRECOGNIZED -> {
          throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
        }
      }
    } catch (e: KingdomInternalException) {
      throw e.asStatusRuntimeException(Status.Code.INTERNAL, "Unexpected internal error.")
    }
  }

  override fun streamModelOutages(request: StreamModelOutagesRequest): Flow<ModelOutage> {
    grpcRequire(request.limit >= 0) { "Limit cannot be less than 0" }
    if (
      request.filter.hasAfter() &&
        (!request.filter.after.hasCreateTime() ||
          request.filter.after.externalModelOutageId == 0L ||
          request.filter.after.externalModelLineId == 0L ||
          request.filter.after.externalModelSuiteId == 0L ||
          request.filter.after.externalModelProviderId == 0L)
    ) {
      failGrpc(Status.INVALID_ARGUMENT) { "Missing After filter fields" }
    }
    if (
      request.filter.hasOutageInterval() &&
        (!request.filter.outageInterval.hasModelOutageStartTime() ||
          !request.filter.outageInterval.hasModelOutageEndTime())
    ) {
      failGrpc(Status.INVALID_ARGUMENT) { "Missing OutageInterval fields" }
    }
    return StreamModelOutages(request.filter, request.limit).execute(client.singleUse()).map {
      it.modelOutage
    }
  }
}
