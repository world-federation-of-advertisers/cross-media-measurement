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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.protobuf.util.Timestamps
import com.google.type.Interval
import io.grpc.Status
import io.grpc.StatusException
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.math.min
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.CreateModelLineRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.EnumerateValidModelLinesRequest
import org.wfanet.measurement.api.v2alpha.EnumerateValidModelLinesResponse
import org.wfanet.measurement.api.v2alpha.ListModelLinesPageToken
import org.wfanet.measurement.api.v2alpha.ListModelLinesPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListModelLinesRequest
import org.wfanet.measurement.api.v2alpha.ListModelLinesResponse
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineImplBase as ModelLinesCoroutineService
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.SetModelLineActiveEndTimeRequest
import org.wfanet.measurement.api.v2alpha.SetModelLineHoldbackModelLineRequest
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.enumerateValidModelLinesResponse
import org.wfanet.measurement.api.v2alpha.listModelLinesPageToken
import org.wfanet.measurement.api.v2alpha.listModelLinesResponse
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelLine as InternalModelLine
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequest
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequestKt.filter
import org.wfanet.measurement.internal.kingdom.StreamModelRolloutsRequestKt
import org.wfanet.measurement.internal.kingdom.getDataProviderRequest
import org.wfanet.measurement.internal.kingdom.setActiveEndTimeRequest as internalSetActiveEndTimeRequest
import org.wfanet.measurement.internal.kingdom.setModelLineHoldbackModelLineRequest
import org.wfanet.measurement.internal.kingdom.streamModelLinesRequest
import org.wfanet.measurement.internal.kingdom.streamModelRolloutsRequest

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

class ModelLinesService(
  private val internalClient: ModelLinesCoroutineStub,
  private val internalDataProvidersClient: DataProvidersCoroutineStub,
  private val internalModelRolloutsClient: ModelRolloutsCoroutineStub,
) : ModelLinesCoroutineService() {

  override suspend fun createModelLine(request: CreateModelLineRequest): ModelLine {
    val parentKey =
      grpcRequireNotNull(ModelSuiteKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != parentKey.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) { "Cannot create ModelLine for another ModelProvider" }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to create ModelLine" }
      }
    }

    val createModelLineRequest = request.modelLine.toInternal(parentKey)
    return try {
      internalClient.createModelLine(createModelLineRequest).toModelLine()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun setModelLineActiveEndTime(
    request: SetModelLineActiveEndTimeRequest
  ): ModelLine {
    val key =
      grpcRequireNotNull(ModelLineKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != key.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) { "Cannot update ModelLine for another ModelProvider" }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to update ModelLine" }
      }
    }

    val internalRequest = internalSetActiveEndTimeRequest {
      externalModelLineId = apiIdToExternalId(key.modelLineId)
      externalModelSuiteId = apiIdToExternalId(key.modelSuiteId)
      externalModelProviderId = apiIdToExternalId(key.modelProviderId)
      activeEndTime = request.activeEndTime
    }

    try {
      return internalClient.setActiveEndTime(internalRequest).toModelLine()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun setModelLineHoldbackModelLine(
    request: SetModelLineHoldbackModelLineRequest
  ): ModelLine {
    val key =
      grpcRequireNotNull(ModelLineKey.fromName(request.name)) {
        "Resource name is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != key.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) { "Cannot update ModelLine for another ModelProvider" }
        }
      }
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to update ModelLine" }
      }
    }

    val holdbackModelLineKey =
      grpcRequireNotNull(ModelLineKey.fromName(request.holdbackModelLine)) {
        "Holdback model line name is either unspecified or invalid"
      }

    val internalSetHoldbackModelLineRequest = setModelLineHoldbackModelLineRequest {
      externalModelLineId = apiIdToExternalId(key.modelLineId)
      externalModelSuiteId = apiIdToExternalId(key.modelSuiteId)
      externalModelProviderId = apiIdToExternalId(key.modelProviderId)
      externalHoldbackModelLineId = apiIdToExternalId(holdbackModelLineKey.modelLineId)
      externalHoldbackModelSuiteId = apiIdToExternalId(holdbackModelLineKey.modelSuiteId)
      externalHoldbackModelProviderId = apiIdToExternalId(holdbackModelLineKey.modelProviderId)
    }

    try {
      return internalClient
        .setModelLineHoldbackModelLine(internalSetHoldbackModelLineRequest)
        .toModelLine()
    } catch (e: StatusException) {
      throw when (e.status.code) {
        Status.Code.NOT_FOUND -> Status.NOT_FOUND
        Status.Code.INVALID_ARGUMENT -> Status.INVALID_ARGUMENT
        else -> Status.UNKNOWN
      }.toExternalStatusRuntimeException(e)
    }
  }

  override suspend fun listModelLines(request: ListModelLinesRequest): ListModelLinesResponse {
    val parent =
      grpcRequireNotNull(ModelSuiteKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid"
      }

    val listModelLinesPageToken = request.toListModelLinesPageToken()

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != parent.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) { "Cannot list ModelLines for another ModelProvider" }
        }
      }
      is DataProviderPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to list ModelLines" }
      }
    }

    val results: List<InternalModelLine> =
      internalClient.streamModelLines(listModelLinesPageToken.toStreamModelLinesRequest()).toList()

    if (results.isEmpty()) {
      return ListModelLinesResponse.getDefaultInstance()
    }

    return listModelLinesResponse {
      modelLines +=
        results.subList(0, min(results.size, listModelLinesPageToken.pageSize)).map {
          internalModelLine ->
          internalModelLine.toModelLine()
        }
      if (results.size > listModelLinesPageToken.pageSize) {
        val pageToken =
          listModelLinesPageToken.copy {
            lastModelLine = previousPageEnd {
              createTime = results[results.lastIndex - 1].createTime
              externalModelProviderId = results[results.lastIndex - 1].externalModelProviderId
              externalModelSuiteId = results[results.lastIndex - 1].externalModelSuiteId
              externalModelLineId = results[results.lastIndex - 1].externalModelLineId
            }
          }
        nextPageToken = pageToken.toByteArray().base64UrlEncode()
      }
    }
  }

  override suspend fun enumerateValidModelLines(
    request: EnumerateValidModelLinesRequest
  ): EnumerateValidModelLinesResponse {
    val parent =
      grpcRequireNotNull(ModelSuiteKey.fromName(request.parent)) {
        "parent is either unspecified or invalid"
      }

    when (val principal: MeasurementPrincipal = principalFromCurrentContext) {
      is ModelProviderPrincipal -> {
        if (principal.resourceKey.modelProviderId != parent.modelProviderId) {
          failGrpc(Status.PERMISSION_DENIED) { "Cannot list ModelLines for another ModelProvider" }
        }
      }
      is DataProviderPrincipal -> {}
      else -> {
        failGrpc(Status.PERMISSION_DENIED) { "Caller does not have permission to list ModelLines" }
      }
    }

    if (!request.hasTimeInterval()) {
      failGrpc(Status.INVALID_ARGUMENT) { "Missing time_interval" }
    }

    if (!Timestamps.isValid(request.timeInterval.startTime)) {
      failGrpc(Status.INVALID_ARGUMENT) { "Missing time_interval.start_time" }
    }

    if (!Timestamps.isValid(request.timeInterval.endTime)) {
      failGrpc(Status.INVALID_ARGUMENT) { "Missing time_interval.end_time" }
    }

    if (Timestamps.compare(request.timeInterval.startTime, request.timeInterval.endTime) >= 0) {
      failGrpc(Status.INVALID_ARGUMENT) {
        "time_interval.start_time must be before time_interval.end_time"
      }
    }

    if (request.dataProvidersList.size == 0) {
      failGrpc(Status.INVALID_ARGUMENT) { "Missing data_providers" }
    }

    for (dataProviderName in request.dataProvidersList) {
      DataProviderKey.fromName(dataProviderName)
        ?: failGrpc(Status.INVALID_ARGUMENT) { "$dataProviderName in data_providers is invalid" }
    }

    val dataProvidersSet = mutableSetOf<String>()
    // Map of external ModelLine IDs to data availability intervals. Populated by retrieving the
    // data availability intervals from every data provider specified in the request.
    val dataProvidersDataAvailabilityIntervalMap: Map<Long, MutableList<Interval>> = buildMap {
      for (dataProviderName in request.dataProvidersList) {
        if (!dataProvidersSet.contains(dataProviderName)) {
          dataProvidersSet.add(dataProviderName)
          val dataProviderKey = DataProviderKey.fromName(dataProviderName)
          val dataProvider =
            try {
              internalDataProvidersClient.getDataProvider(
                getDataProviderRequest {
                  externalDataProviderId = apiIdToExternalId(dataProviderKey!!.dataProviderId)
                }
              )
            } catch (e: StatusException) {
              throw when (e.status.code) {
                Status.Code.NOT_FOUND ->
                  failGrpc(Status.NOT_FOUND) { "$dataProviderName in data_providers not found" }

                Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED.asRuntimeException()
                else -> Status.UNKNOWN.asRuntimeException()
              }
            }

          for (dataAvailabilityInterval in dataProvider.dataAvailabilityIntervalsList) {
            val intervalList =
              getOrDefault(dataAvailabilityInterval.key.externalModelLineId, mutableListOf())
            intervalList.add(dataAvailabilityInterval.value)
            put(dataAvailabilityInterval.key.externalModelLineId, intervalList)
          }
        }
      }
    }

    val modelSuiteKey = ModelSuiteKey.fromName(request.parent)

    // Retrieve all model lines that will be checked.
    val internalModelLines: List<InternalModelLine> =
      try {
        internalClient
          .streamModelLines(
            streamModelLinesRequest {
              filter = filter {
                externalModelProviderId = apiIdToExternalId(modelSuiteKey!!.modelProviderId)
                externalModelSuiteId = apiIdToExternalId(modelSuiteKey.modelSuiteId)
                if (request.typesList.isEmpty()) {
                  this.type += InternalModelLine.Type.PROD
                } else {
                  for (type in request.typesList) {
                    this.type += type.toInternalType()
                  }
                }
              }
            }
          )
          .toList()
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }.asRuntimeException()
      }

    // Checks the following items for every model line. It will move on to the next model line if
    // any check fails. All checks need to pass for the model line to be in the final response:
    // 1. time interval in request
    // 2. data availability intervals from every data provider in request
    // 3. model rollouts
    val validModelLines: List<ModelLine> = buildList {
      for (internalModelLine in internalModelLines) {
        val modelLine = internalModelLine.toModelLine()
        if (request.timeInterval.isFullyContainedWithin(modelLine)) {
          val dataAvailabilityIntervals: List<Interval>? =
            dataProvidersDataAvailabilityIntervalMap[internalModelLine.externalModelLineId]
          if (
            dataAvailabilityIntervals != null &&
              dataAvailabilityIntervals.size == dataProvidersSet.size
          ) {
            if (request.timeInterval.isFullyContainedWithin(dataAvailabilityIntervals)) {
              val streamModelRolloutsResponse =
                try {
                  internalModelRolloutsClient
                    .streamModelRollouts(
                      streamModelRolloutsRequest {
                        filter =
                          StreamModelRolloutsRequestKt.filter {
                            externalModelProviderId = internalModelLine.externalModelProviderId
                            externalModelSuiteId = internalModelLine.externalModelSuiteId
                            externalModelLineId = internalModelLine.externalModelLineId
                          }
                      }
                    )
                    .toList()
                } catch (e: StatusException) {
                  throw when (e.status.code) {
                    Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
                    else -> Status.UNKNOWN
                  }.asRuntimeException()
                }

              // Currently, there is only 1 `ModelRollout` per `ModelLine`
              if (streamModelRolloutsResponse.size == 1) {
                add(modelLine)
              } else {
                logger.log(
                  Level.WARNING,
                  "Must have 1 model rollout per model line to have a valid model line.",
                )
              }
            }
          }
        }
      }
    }

    return enumerateValidModelLinesResponse {
      this.modelLines += validModelLines.sortedWith(modelLineComparator())
    }
  }

  /** Converts a public [ListModelLinesRequest] to an internal [ListModelLinesPageToken]. */
  private fun ListModelLinesRequest.toListModelLinesPageToken(): ListModelLinesPageToken {
    val source = this

    val key =
      grpcRequireNotNull(ModelSuiteKey.fromName(source.parent)) {
        "Resource name is either unspecified or invalid"
      }
    grpcRequire(source.pageSize >= 0) { "Page size cannot be less than 0" }

    val externalModelProviderId = apiIdToExternalId(key.modelProviderId)
    val externalModelSuiteId = apiIdToExternalId(key.modelSuiteId)

    return if (source.pageToken.isNotBlank()) {
      ListModelLinesPageToken.parseFrom(source.pageToken.base64UrlDecode()).copy {
        grpcRequire(this.externalModelProviderId == externalModelProviderId) {
          "Arguments must be kept the same when using a page token"
        }
        grpcRequire(this.externalModelSuiteId == externalModelSuiteId) {
          "Arguments must be kept the same when using a page token"
        }

        if (source.pageSize in 1..MAX_PAGE_SIZE) {
          pageSize = source.pageSize
        }
      }
    } else {
      listModelLinesPageToken {
        pageSize =
          when {
            source.pageSize == 0 -> DEFAULT_PAGE_SIZE
            source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
            else -> source.pageSize
          }
        this.externalModelProviderId = externalModelProviderId
        this.externalModelSuiteId = externalModelSuiteId
        this.types += source.filter.typesList
      }
    }
  }

  /** Converts an internal [ListModelLinesPageToken] to an internal [StreamModelLinesRequest]. */
  private fun ListModelLinesPageToken.toStreamModelLinesRequest(): StreamModelLinesRequest {
    val source = this
    return streamModelLinesRequest {
      // get 1 more than the actual page size for deciding whether to set page token
      limit = source.pageSize + 1
      filter = filter {
        externalModelProviderId = source.externalModelProviderId
        externalModelSuiteId = source.externalModelSuiteId
        type += source.typesList.map { type -> type.toInternalType() }
        if (source.hasLastModelLine()) {
          after = afterFilter {
            createTime = source.lastModelLine.createTime
            externalModelProviderId = source.lastModelLine.externalModelProviderId
            externalModelSuiteId = source.lastModelLine.externalModelSuiteId
            externalModelLineId = source.lastModelLine.externalModelLineId
          }
        }
      }
    }
  }

  /**
   * [ModelLine.Type.PROD] appears first before [ModelLine.Type.HOLDBACK] and
   * [ModelLine.Type.HOLDBACK] appears first before [ModelLine.Type.DEV]. If the types are the same,
   * then the more recent `activeStartTime` appears first.
   */
  private fun modelLineComparator() =
    Comparator<ModelLine> { a, b ->
      when {
        a.type == b.type -> {
          Timestamps.compare(a.activeStartTime, b.activeStartTime) * -1
        }
        a.type == ModelLine.Type.PROD -> -1
        a.type == ModelLine.Type.HOLDBACK -> {
          if (b.type == ModelLine.Type.PROD) {
            1
          } else -1
        }
        a.type == ModelLine.Type.DEV -> 1
        else -> -1
      }
    }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
