/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.protobuf.util.Timestamps
import io.grpc.Status
import io.grpc.StatusException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.check
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EnumerateValidModelLinesRequest
import org.wfanet.measurement.api.v2alpha.EnumerateValidModelLinesResponse
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.reporting.service.api.InvalidFieldValueException
import org.wfanet.measurement.reporting.service.api.RequiredFieldNotSetException

class ModelLinesService(
  private val kingdomModelLinesStub: ModelLinesCoroutineStub,
  private val authorization: Authorization,
  private val apiAuthenticationKey: String,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ModelLinesGrpcKt.ModelLinesCoroutineImplBase(coroutineContext) {
  override suspend fun enumerateValidModelLines(
    request: EnumerateValidModelLinesRequest
  ): EnumerateValidModelLinesResponse {
    authorization.check(Authorization.ROOT_RESOURCE_NAME, LIST_MODEL_LINES_PERMISSIONS)

    if (!request.hasTimeInterval()) {
      throw RequiredFieldNotSetException("time_interval")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (!Timestamps.isValid(request.timeInterval.startTime)) {
      throw InvalidFieldValueException("time_interval.start_time")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (!Timestamps.isValid(request.timeInterval.endTime)) {
      throw InvalidFieldValueException("time_interval.end_time")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (Timestamps.compare(request.timeInterval.startTime, request.timeInterval.endTime) >= 0) {
      throw InvalidFieldValueException("time_interval") { fieldName ->
          "$fieldName must have a start_time that is before the end_time"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.dataProvidersList.size == 0) {
      throw RequiredFieldNotSetException("data_providers")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    for (dataProvider in request.dataProvidersList) {
      DataProviderKey.fromName(dataProvider)
        ?: throw InvalidFieldValueException("data_providers") { fieldName ->
            "$dataProvider in $fieldName is invalid"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    return try {
      kingdomModelLinesStub
        .withAuthenticationKey(apiAuthenticationKey)
        .enumerateValidModelLines(request)
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
        Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
        else -> Status.INTERNAL
      }.asRuntimeException()
    }
  }

  companion object {
    private const val LIST_MODEL_LINES_PERMISSION = "reporting.modelLines.list"
    val LIST_MODEL_LINES_PERMISSIONS = setOf(LIST_MODEL_LINES_PERMISSION)
  }
}
