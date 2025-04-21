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
import com.google.type.Interval
import io.grpc.Status
import io.grpc.StatusException
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.check
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupKey as KingdomEventGroupKey
import java.util.logging.Level
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getEventGroupRequest
import org.wfanet.measurement.api.v2alpha.listModelLinesRequest
import org.wfanet.measurement.api.v2alpha.listModelRolloutsRequest
import org.wfanet.measurement.reporting.service.api.EventGroupNotFoundException
import org.wfanet.measurement.reporting.service.api.InvalidFieldValueException
import org.wfanet.measurement.reporting.service.api.RequiredFieldNotSetException
import org.wfanet.measurement.reporting.v2alpha.ListValidModelLinesRequest
import org.wfanet.measurement.reporting.v2alpha.ListValidModelLinesResponse
import org.wfanet.measurement.reporting.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.listValidModelLinesResponse

class ModelLinesService(
  private val kingdomModelLinesStub: ModelLinesCoroutineStub,
  private val kingdomModelRolloutsStub: ModelRolloutsCoroutineStub,
  private val kingdomEventGroupsStub: EventGroupsCoroutineStub,
  private val kingdomDataProvidersStub: DataProvidersCoroutineStub,
  private val modelSuite: String,
  private val authorization: Authorization,
) : ModelLinesCoroutineImplBase() {
  override suspend fun listValidModelLines(
    request: ListValidModelLinesRequest
  ): ListValidModelLinesResponse {
    authorization.check(modelSuite, Permission.LIST)

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

    if (request.eventGroupsList.size == 0) {
      throw RequiredFieldNotSetException("event_groups")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    for (eventGroup in request.eventGroupsList) {
      EventGroupKey.fromName(eventGroup)
        ?: throw InvalidFieldValueException("event_groups") { fieldName ->
            "$eventGroup in $fieldName is invalid"
          }
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val dataProvidersSet = mutableSetOf<String>()
    val dataAvailabilityIntervals: List<Interval> = buildList {
      for (eventGroupName in request.eventGroupsList) {
        val kingdomEventGroup =
          try {
            kingdomEventGroupsStub.getEventGroup(getEventGroupRequest { name = eventGroupName })
          } catch (e: StatusException) {
            throw when (e.status.code) {
              Status.Code.NOT_FOUND ->
                EventGroupNotFoundException(eventGroupName)
                  .asStatusRuntimeException(Status.Code.NOT_FOUND)
              Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED.asRuntimeException()
              else -> Status.UNKNOWN.asRuntimeException()
            }
          }
        add(kingdomEventGroup.dataAvailabilityInterval)

        val dataProviderName =
          KingdomEventGroupKey.fromName(kingdomEventGroup.name)!!.parentKey.toName()
        if (!dataProvidersSet.contains(dataProviderName)) {
          dataProvidersSet.add(dataProviderName)
          val kingdomDataProvider =
            try {
              kingdomDataProvidersStub.getDataProvider(
                getDataProviderRequest { name = dataProviderName }
              )
            } catch (e: StatusException) {
              throw when (e.status.code) {
                Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
                else -> Status.UNKNOWN
              }.asRuntimeException()
            }
          // TODO(@tristanvuong2021): Change this once dataAvailabilityInterval is deprecated.
          add(kingdomDataProvider.dataAvailabilityInterval)
        }
      }
    }

    val modelLines: List<ModelLine> =
      try {
        kingdomModelLinesStub
          .listModelLines(listModelLinesRequest { parent = modelSuite })
          .modelLinesList
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
          else -> Status.UNKNOWN
        }.asRuntimeException()
      }

    val validModelLines: List<ModelLine> = buildList {
      for (modelLine in modelLines) {
        if (request.timeInterval.isFullyContainedWithin(modelLine)) {
          if (dataAvailabilityIntervals.areFullyContainedWithin(modelLine)) {
            val listModelRolloutsResponse =
              try {
                kingdomModelRolloutsStub.listModelRollouts(
                  listModelRolloutsRequest { parent = modelLine.name }
                )
              } catch (e: StatusException) {
                throw when (e.status.code) {
                  Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
                  else -> Status.UNKNOWN
                }.asRuntimeException()
              }

            // Currently, there is only 1 `ModelRollout` per `ModelLine`
            if (listModelRolloutsResponse.modelRolloutsList.size == 1) {
              add(modelLine)
            } else {
              logger.log(Level.WARNING, "Must have 1 model rollout per model line to have a valid model line.")
            }
          }
        }
      }
    }

    return listValidModelLinesResponse {
      this.modelLines += validModelLines.sortedWith(modelLineComparator())
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

  object Permission {
    private const val TYPE = "reporting.modelLines"
    const val LIST = "$TYPE.list"
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
