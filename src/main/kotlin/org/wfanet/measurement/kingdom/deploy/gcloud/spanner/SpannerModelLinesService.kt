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
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.EnumerateValidModelLinesRequest
import org.wfanet.measurement.internal.kingdom.EnumerateValidModelLinesResponse
import org.wfanet.measurement.internal.kingdom.GetModelLineRequest
import org.wfanet.measurement.internal.kingdom.ModelLine
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.SetActiveEndTimeRequest
import org.wfanet.measurement.internal.kingdom.SetModelLineHoldbackModelLineRequest
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequest
import org.wfanet.measurement.internal.kingdom.enumerateValidModelLinesResponse
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.InvalidFieldValueException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineTypeIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelSuiteNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RequiredFieldNotSetException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamModelLines
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ModelLineReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateModelLine
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.SetActiveEndTime
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.SetModelLineHoldbackModelLine

class SpannerModelLinesService(
  private val clock: Clock,
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ModelLinesCoroutineImplBase(coroutineContext) {

  override suspend fun createModelLine(request: ModelLine): ModelLine {
    val now = clock.instant()
    if (!request.hasActiveStartTime()) {
      throw RequiredFieldNotSetException("active_start_time")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val activeStartTime = request.activeStartTime.toInstant()
    if (activeStartTime <= now) {
      throw InvalidFieldValueException("active_start_time") { fieldName ->
          "$fieldName must be in the future"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.hasActiveEndTime()) {
      val activeEndTime = request.activeEndTime.toInstant()
      if (activeEndTime < activeStartTime) {
        throw InvalidFieldValueException("active_end_time") { fieldName ->
            "$fieldName must be at least active_start_time"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }
    if (request.externalHoldbackModelLineId != 0L && request.type != ModelLine.Type.PROD) {
      throw InvalidFieldValueException("external_holdback_model_line_id") { fieldName ->
          "$fieldName may only be specified when type is PROD"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum accessors cannot return null.
    when (request.type) {
      ModelLine.Type.DEV,
      ModelLine.Type.HOLDBACK,
      ModelLine.Type.PROD -> {}
      ModelLine.Type.TYPE_UNSPECIFIED ->
        throw RequiredFieldNotSetException("type")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      ModelLine.Type.UNRECOGNIZED ->
        throw InvalidFieldValueException("type") { fieldName ->
            "Unrecognized value for $fieldName"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    try {
      return CreateModelLine(request).execute(client, idGenerator)
    } catch (e: ModelSuiteNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    } catch (e: ModelLineNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: ModelLineTypeIllegalException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }
  }

  override suspend fun getModelLine(request: GetModelLineRequest): ModelLine {
    grpcRequire(request.externalModelProviderId != 0L) {
      "external_model_provider_id not specified"
    }
    grpcRequire(request.externalModelSuiteId != 0L) { "external_model_suite_id not specified" }
    grpcRequire(request.externalModelLineId != 0L) { "external_model_line_id not specified" }

    val result: ModelLineReader.Result? =
      ModelLineReader()
        .readByExternalModelLineId(
          client.singleUseReadOnlyTransaction(),
          externalModelProviderId = ExternalId(request.externalModelProviderId),
          externalModelSuiteId = ExternalId(request.externalModelSuiteId),
          externalModelLineId = ExternalId(request.externalModelLineId),
        )

    if (result == null) {
      throw ModelLineNotFoundException(
          ExternalId(request.externalModelProviderId),
          ExternalId(request.externalModelSuiteId),
          ExternalId(request.externalModelLineId),
        )
        .asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    return result.modelLine
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
        e.message ?: "ModelLine invalid active time argument.",
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
      failGrpc(Status.INVALID_ARGUMENT) { "Missing After filter fields" }
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
          ?: "Only ModelLines with type equal to 'PROD' can have a HoldbackModelLine having type equal to 'HOLDBACK'.",
      )
    }
  }

  override suspend fun enumerateValidModelLines(
    request: EnumerateValidModelLinesRequest
  ): EnumerateValidModelLinesResponse {
    val types: List<ModelLine.Type> =
      if (request.typesList.isEmpty()) {
        listOf(ModelLine.Type.PROD)
      } else {
        request.typesList
      }
    val modelLineResults =
      ModelLineReader.readValidModelLines(
        client.singleUseReadOnlyTransaction(),
        externalModelProviderId = ExternalId(request.externalModelProviderId),
        externalModelSuiteId = ExternalId(request.externalModelSuiteId),
        request.timeInterval,
        types,
        request.externalDataProviderIdsList.map { ExternalId(it) },
      )

    return enumerateValidModelLinesResponse {
      /**
       * [ModelLine.Type.PROD] appears before [ModelLine.Type.HOLDBACK] and
       * [ModelLine.Type.HOLDBACK] appears before [ModelLine.Type.DEV]. If the types are the same,
       * then the more recent `activeStartTime` appears first.
       */
      modelLines +=
        modelLineResults
          .map { it.modelLine }
          .toList()
          .sortedWith(
            compareBy<ModelLine> {
                @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
                when (it.type) {
                  ModelLine.Type.PROD -> 1
                  ModelLine.Type.HOLDBACK -> 2
                  ModelLine.Type.DEV -> 3
                  ModelLine.Type.TYPE_UNSPECIFIED,
                  ModelLine.Type.UNRECOGNIZED -> error("Unknown ModelLine type")
                }
              }
              .thenByDescending(Timestamps::compare) { it.activeStartTime }
          )
    }
  }
}
