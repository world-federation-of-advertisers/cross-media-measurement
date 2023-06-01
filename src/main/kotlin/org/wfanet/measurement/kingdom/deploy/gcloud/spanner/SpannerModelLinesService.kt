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
import java.time.Clock
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.SetActiveEndTimeRequest
import org.wfanet.measurement.internal.kingdom.SetModelLineHoldbackModelLineRequest
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineTypeIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelSuiteNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamModelLines
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateModelLine
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.SetActiveEndTime
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.SetModelLineHoldbackModelLine

class SpannerModelLinesService(
  private val clock: Clock,
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : ModelLinesCoroutineImplBase() {

  override suspend fun createModelLine(request: ModelLine): ModelLine {
    grpcRequire(request.hasActiveStartTime()) { "ActiveStartTime is missing." }
    grpcRequire(request.type != ModelLine.Type.TYPE_UNSPECIFIED) {
      "Unrecognized ModelLine's type ${request.type}"
    }
    try {
      return CreateModelLine(request, clock).execute(client, idGenerator)
    } catch (e: ModelSuiteNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelSuite not found.")
    } catch (e: ModelLineTypeIllegalException) {
      throw e.asStatusRuntimeException(
        Status.Code.INVALID_ARGUMENT,
        e.message
          ?: "Only ModelLines with type equal to 'PROD' can have a HoldbackModelLine having type equal to 'HOLDBACK'."
      )
    } catch (e: ModelLineInvalidArgsException) {
      throw e.asStatusRuntimeException(
        Status.Code.INVALID_ARGUMENT,
        e.message ?: "ActiveStartTime and/or ActiveEndTime is invalid."
      )
    }
  }

  override suspend fun setActiveEndTime(request: SetActiveEndTimeRequest): ModelLine {
    grpcRequire(request.activeEndTime != null) { "ActiveEndTime field is missing." }
    try {
      return SetActiveEndTime(request, clock).execute(client, idGenerator)
    } catch (e: ModelLineNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelLine not found.")
    } catch (e: ModelLineInvalidArgsException) {
      throw e.asStatusRuntimeException(
        Status.Code.INVALID_ARGUMENT,
        e.message ?: "ModelLine invalid active time argument."
      )
    }
  }

  override fun streamModelLines(request: StreamModelLinesRequest): Flow<ModelLine> {
    grpcRequire(request.limit >= 0) { "Limit cannot be less than 0" }
    if (
      request.filter.hasAfter() &&
        (!request.filter.after.hasCreateTime() ||
          request.filter.after.externalModelLineId == 0L ||
          request.filter.after.externalModelSuiteId == 0L ||
          request.filter.after.externalModelProviderId == 0L)
    ) {
      failGrpc(
        Status.INVALID_ARGUMENT,
      ) {
        "Missing After filter fields"
      }
    }
    return StreamModelLines(request.filter, request.limit).execute(client.singleUse()).map {
      it.modelLine
    }
  }

  override suspend fun setModelLineHoldbackModelLine(
    request: SetModelLineHoldbackModelLineRequest
  ): ModelLine {
    grpcRequire(request.externalModelProviderId != 0L) {
      "external_model_provider_id not specified"
    }
    grpcRequire(request.externalModelSuiteId != 0L) { "external_model_suite_id not specified" }
    grpcRequire(request.externalModelLineId != 0L) { "external_model_line_id not specified" }
    grpcRequire(request.externalHoldbackModelProviderId != 0L) {
      "external_holdback_model_provider_id not specified"
    }
    grpcRequire(request.externalHoldbackModelSuiteId != 0L) {
      "external_holdback_model_suite_id not specified"
    }
    grpcRequire(request.externalHoldbackModelLineId != 0L) {
      "external_holdback_model_line_id not specified"
    }
    grpcRequire(
      request.externalModelProviderId == request.externalHoldbackModelProviderId &&
        request.externalModelSuiteId == request.externalHoldbackModelSuiteId
    ) {
      "HoldbackModelLine and ModelLine must be part of the same ModelSuite."
    }
    try {
      return SetModelLineHoldbackModelLine(request).execute(client, idGenerator)
    } catch (e: ModelLineNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, e.message ?: "ModelLine not found.")
    } catch (e: ModelLineTypeIllegalException) {
      throw e.asStatusRuntimeException(
        Status.Code.INVALID_ARGUMENT,
        e.message
          ?: "Only ModelLines with type equal to 'PROD' can have a HoldbackModelLine having type equal to 'HOLDBACK'."
      )
    }
  }
}
