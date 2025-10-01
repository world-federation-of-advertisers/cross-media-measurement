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

import com.google.protobuf.util.Timestamps
import io.grpc.Status
import java.time.Clock
import java.time.Instant
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.DeleteModelRolloutRequest
import org.wfanet.measurement.internal.kingdom.ModelRollout
import org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt.ModelRolloutsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ScheduleModelRolloutFreezeRequest
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.InvalidFieldValueException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelReleaseNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelRolloutAlreadyStartedException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelRolloutFreezeScheduledException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelRolloutFreezeTimeOutOfRangeException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelRolloutNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelRolloutOlderThanPreviousException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequiredFieldNotSetException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamModelRollouts
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateModelRollout
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.DeleteModelRollout
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ScheduleModelRolloutFreeze

class SpannerModelRolloutsService(
  private val clock: Clock,
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ModelRolloutsCoroutineImplBase(coroutineContext) {

  override suspend fun createModelRollout(request: ModelRollout): ModelRollout {
    if (request.externalModelProviderId == 0L) {
      throw RequiredFieldNotSetException("external_model_provider_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.externalModelSuiteId == 0L) {
      throw RequiredFieldNotSetException("external_model_suite_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.externalModelLineId == 0L) {
      throw RequiredFieldNotSetException("external_model_line_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.externalModelReleaseId == 0L) {
      throw RequiredFieldNotSetException("external_model_release_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (!request.hasRolloutPeriodStartTime()) {
      throw RequiredFieldNotSetException("rollout_period_start_time")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (!request.hasRolloutPeriodEndTime()) {
      throw RequiredFieldNotSetException("rollout_period_end_time")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (Timestamps.compare(request.rolloutPeriodStartTime, request.rolloutPeriodEndTime) > 0) {
      throw InvalidFieldValueException("rollout_period_end_time") { fieldName ->
          "$fieldName is before rollout_period_start_time"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    try {
      return CreateModelRollout(request).execute(client, idGenerator)
    } catch (e: ModelRolloutOlderThanPreviousException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: ModelLineNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: ModelReleaseNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }

  override suspend fun scheduleModelRolloutFreeze(
    request: ScheduleModelRolloutFreezeRequest
  ): ModelRollout {
    if (!request.hasRolloutFreezeTime()) {
      throw RequiredFieldNotSetException("rollout_freeze_time")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val now: Instant = clock.instant()
    if (request.rolloutFreezeTime.toInstant() < now) {
      throw InvalidFieldValueException("rollout_freeze_time") { fieldName ->
          "$fieldName is in the past"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    try {
      return ScheduleModelRolloutFreeze(request).execute(client, idGenerator)
    } catch (e: ModelRolloutNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: ModelRolloutFreezeScheduledException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: ModelRolloutFreezeTimeOutOfRangeException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }
  }

  override fun streamModelRollouts(request: StreamModelRolloutsRequest): Flow<ModelRollout> {
    grpcRequire(request.limit >= 0) { "Limit cannot be less than 0" }
    if (
      request.filter.hasAfter() &&
        (!request.filter.after.hasRolloutPeriodStartTime() ||
          request.filter.after.externalModelRolloutId == 0L ||
          request.filter.after.externalModelLineId == 0L ||
          request.filter.after.externalModelSuiteId == 0L ||
          request.filter.after.externalModelProviderId == 0L)
    ) {
      failGrpc(Status.INVALID_ARGUMENT) { "Missing After filter fields" }
    }
    if (
      request.filter.hasRolloutPeriod() &&
        (!request.filter.rolloutPeriod.hasRolloutPeriodStartTime() ||
          !request.filter.rolloutPeriod.hasRolloutPeriodEndTime())
    ) {
      failGrpc(Status.INVALID_ARGUMENT) { "Missing RolloutPeriod fields" }
    }
    return StreamModelRollouts(request.filter, request.limit).execute(client.singleUse()).map {
      it.modelRollout
    }
  }

  override suspend fun deleteModelRollout(request: DeleteModelRolloutRequest): ModelRollout {
    grpcRequire(request.externalModelProviderId != 0L) { "ExternalModelProviderId unspecified" }
    grpcRequire(request.externalModelSuiteId != 0L) { "ExternalModelSuiteId unspecified" }
    grpcRequire(request.externalModelLineId != 0L) { "ExternalModelLineId unspecified" }
    grpcRequire(request.externalModelRolloutId != 0L) { "ExternalModelRolloutId unspecified" }
    try {
      return DeleteModelRollout(request, clock).execute(client, idGenerator)
    } catch (e: ModelRolloutNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: ModelRolloutAlreadyStartedException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }
  }
}
