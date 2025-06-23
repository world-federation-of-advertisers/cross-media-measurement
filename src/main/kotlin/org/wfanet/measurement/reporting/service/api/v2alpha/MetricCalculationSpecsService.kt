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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.type.DayOfWeek
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.random.Random
import org.projectnessie.cel.Env
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.check
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.getModelLineRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.api.ResourceIds
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.internal.reporting.v2.ListMetricCalculationSpecsRequest as InternalListMetricCalculationSpecsRequest
import org.wfanet.measurement.internal.reporting.v2.ListMetricCalculationSpecsResponse as InternalListMetricCalculationSpecsResponse
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec as InternalMetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecKt as InternalMetricCalculationSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.createMetricCalculationSpecRequest
import org.wfanet.measurement.internal.reporting.v2.getMetricCalculationSpecRequest
import org.wfanet.measurement.internal.reporting.v2.listMetricCalculationSpecsRequest
import org.wfanet.measurement.internal.reporting.v2.metricCalculationSpec as internalMetricCalculationSpec
import org.wfanet.measurement.reporting.service.api.InvalidFieldValueException
import org.wfanet.measurement.reporting.v2alpha.CreateMetricCalculationSpecRequest
import org.wfanet.measurement.reporting.v2alpha.GetMetricCalculationSpecRequest
import org.wfanet.measurement.reporting.v2alpha.ListMetricCalculationSpecsPageToken
import org.wfanet.measurement.reporting.v2alpha.ListMetricCalculationSpecsPageTokenKt
import org.wfanet.measurement.reporting.v2alpha.ListMetricCalculationSpecsRequest
import org.wfanet.measurement.reporting.v2alpha.ListMetricCalculationSpecsResponse
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.listMetricCalculationSpecsPageToken
import org.wfanet.measurement.reporting.v2alpha.listMetricCalculationSpecsResponse
import org.wfanet.measurement.reporting.v2alpha.metricCalculationSpec

class MetricCalculationSpecsService(
  private val internalMetricCalculationSpecsStub: MetricCalculationSpecsCoroutineStub,
  private val kingdomModelLinesStub: ModelLinesCoroutineStub,
  private val metricSpecConfig: MetricSpecConfig,
  private val authorization: Authorization,
  private val secureRandom: Random,
  private val measurementConsumerConfigs: MeasurementConsumerConfigs,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : MetricCalculationSpecsCoroutineImplBase(coroutineContext) {
  override suspend fun createMetricCalculationSpec(
    request: CreateMetricCalculationSpecRequest
  ): MetricCalculationSpec {
    val parentKey: MeasurementConsumerKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }

    grpcRequire(request.metricCalculationSpec.displayName.isNotEmpty()) {
      "display_name must be set."
    }
    grpcRequire(request.metricCalculationSpec.metricSpecsList.isNotEmpty()) {
      "No metric_spec is specified."
    }
    // Expand groupings to predicate groups in Cartesian product
    val groupings: List<List<String>> =
      request.metricCalculationSpec.groupingsList.map {
        grpcRequire(it.predicatesList.isNotEmpty()) {
          "The predicates in Grouping must be specified."
        }
        it.predicatesList
      }
    val allGroupingPredicates = groupings.flatten()
    grpcRequire(allGroupingPredicates.size == allGroupingPredicates.distinct().size) {
      "Cannot have duplicate predicates in different groupings."
    }

    grpcRequire(request.metricCalculationSpecId.matches(RESOURCE_ID_REGEX)) {
      "metric_calculation_spec_id is invalid."
    }

    authorization.check(request.parent, Permission.CREATE)

    val measurementConsumerConfig =
      measurementConsumerConfigs.configsMap[parentKey.toName()]
        ?: throw Status.INTERNAL.withDescription("Config not found for ${parentKey.toName()}")
          .asRuntimeException()
    val measurementConsumerCredentials =
      MeasurementConsumerCredentials.fromConfig(parentKey, measurementConsumerConfig)

    val internalCreateMetricCalculationSpecRequest = createMetricCalculationSpecRequest {
      metricCalculationSpec =
        request.metricCalculationSpec.toInternal(
          parentKey.measurementConsumerId,
          measurementConsumerCredentials,
        )
      externalMetricCalculationSpecId = request.metricCalculationSpecId
    }

    val internalMetricCalculationSpec =
      try {
        internalMetricCalculationSpecsStub.createMetricCalculationSpec(
          internalCreateMetricCalculationSpecRequest
        )
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.NOT_FOUND -> Status.NOT_FOUND.withDescription("${request.parent} not found")
            Status.Code.ALREADY_EXISTS ->
              Status.ALREADY_EXISTS.withDescription(
                "MetricCalculationSpec with ID ${request.metricCalculationSpecId} already exists"
              )
            else -> Status.INTERNAL.withDescription("Unable to create MetricCalculationSpec")
          }
          .withCause(e)
          .asRuntimeException()
      }

    return internalMetricCalculationSpec.toPublic()
  }

  override suspend fun getMetricCalculationSpec(
    request: GetMetricCalculationSpecRequest
  ): MetricCalculationSpec {
    val metricCalculationSpecKey =
      grpcRequireNotNull(MetricCalculationSpecKey.fromName(request.name)) {
        "MetricCalculationSpec name is either unspecified or invalid"
      }

    authorization.check(
      listOf(request.name, metricCalculationSpecKey.parentKey.toName()),
      Permission.GET,
    )

    val internalMetricCalculationSpec =
      try {
        internalMetricCalculationSpecsStub.getMetricCalculationSpec(
          getMetricCalculationSpecRequest {
            cmmsMeasurementConsumerId = metricCalculationSpecKey.cmmsMeasurementConsumerId
            externalMetricCalculationSpecId = metricCalculationSpecKey.metricCalculationSpecId
          }
        )
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.NOT_FOUND -> Status.NOT_FOUND.withDescription("${request.name} not found")
            else -> Status.INTERNAL.withDescription("Unable to get MetricCalculationSpec")
          }
          .withCause(e)
          .asRuntimeException()
      }

    return internalMetricCalculationSpec.toPublic()
  }

  override suspend fun listMetricCalculationSpecs(
    request: ListMetricCalculationSpecsRequest
  ): ListMetricCalculationSpecsResponse {
    grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
      "Parent is either unspecified or invalid."
    }
    val listMetricCalculationSpecsPageToken = request.toListMetricCalculationSpecsPageToken()

    authorization.check(request.parent, Permission.LIST)

    val internalListMetricCalculationSpecsRequest =
      listMetricCalculationSpecsPageToken.toInternalListMetricCalculationSpecsRequest()

    val response: InternalListMetricCalculationSpecsResponse =
      try {
        internalMetricCalculationSpecsStub.listMetricCalculationSpecs(
          internalListMetricCalculationSpecsRequest
        )
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.NOT_FOUND -> Status.NOT_FOUND.withDescription("${request.parent} not found")
            else -> Status.INTERNAL.withDescription("Unable to list MetricCalculationSpecs")
          }
          .withCause(e)
          .asRuntimeException()
      }

    val results = response.metricCalculationSpecsList
    if (results.isEmpty()) {
      return ListMetricCalculationSpecsResponse.getDefaultInstance()
    }

    val nextPageToken: ListMetricCalculationSpecsPageToken? =
      if (response.limited) {
        val lastResult = results.last()
        listMetricCalculationSpecsPageToken.copy {
          lastMetricCalculationSpec =
            ListMetricCalculationSpecsPageTokenKt.previousPageEnd {
              cmmsMeasurementConsumerId = lastResult.cmmsMeasurementConsumerId
              externalMetricCalculationSpecId = lastResult.externalMetricCalculationSpecId
            }
        }
      } else {
        null
      }

    return listMetricCalculationSpecsResponse {
      metricCalculationSpecs +=
        filterMetricCalculationSpecs(
          results.map { internalMetricCalculationSpec -> internalMetricCalculationSpec.toPublic() },
          request.filter,
        )

      if (nextPageToken != null) {
        this.nextPageToken = nextPageToken.toByteString().base64UrlEncode()
      }
    }
  }

  /** Converts a public [MetricCalculationSpec] to an internal [InternalMetricCalculationSpec]. */
  private suspend fun MetricCalculationSpec.toInternal(
    cmmsMeasurementConsumerId: String,
    measurementConsumerCredentials: MeasurementConsumerCredentials,
  ): InternalMetricCalculationSpec {
    val source = this

    if (source.hasTrailingWindow()) {
      grpcRequire(source.hasMetricFrequencySpec()) {
        "metric_frequency_spec must be set if trailing_window is set"
      }
    }

    val internalMetricSpecs =
      source.metricSpecsList.map { metricSpec ->
        try {
          metricSpec.withDefaults(metricSpecConfig, secureRandom).toInternal()
        } catch (e: MetricSpecDefaultsException) {
          failGrpc(Status.INVALID_ARGUMENT) {
            listOfNotNull("Invalid metric_spec.", e.message, e.cause?.message)
              .joinToString(separator = "\n")
          }
        } catch (e: Exception) {
          failGrpc(Status.UNKNOWN) { "Failed to read the metric_spec." }
        }
      }

    if (source.modelLine.isNotEmpty()) {
      ModelLineKey.fromName(source.modelLine)
        ?: throw InvalidFieldValueException("request.metric_calculation_spec.model_line")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

      try {
        kingdomModelLinesStub
          .withAuthenticationKey(
            measurementConsumerCredentials.callCredentials.apiAuthenticationKey
          )
          .getModelLine(getModelLineRequest { name = source.modelLine })
      } catch (e: StatusException) {
        throw when (e.status.code) {
          Status.Code.NOT_FOUND ->
            InvalidFieldValueException("request.metric_calculation_spec.model_line")
              .asStatusRuntimeException(Status.Code.NOT_FOUND)
          else -> StatusRuntimeException(Status.INTERNAL)
        }
      }
    }

    return internalMetricCalculationSpec {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      cmmsModelLine = source.modelLine
      details =
        InternalMetricCalculationSpecKt.details {
          displayName = source.displayName
          metricSpecs += internalMetricSpecs
          filter = source.filter
          groupings +=
            source.groupingsList.map { grouping ->
              InternalMetricCalculationSpecKt.grouping { predicates += grouping.predicatesList }
            }
          if (source.hasMetricFrequencySpec()) {
            metricFrequencySpec = source.metricFrequencySpec.toInternal()
          }
          if (source.hasTrailingWindow()) {
            trailingWindow = source.trailingWindow.toInternal()
          }
          tags.putAll(source.tagsMap)
        }
    }
  }

  private fun filterMetricCalculationSpecs(
    metricCalculationSpecs: List<MetricCalculationSpec>,
    filter: String,
  ): List<MetricCalculationSpec> {
    return try {
      filterList(ENV, metricCalculationSpecs, filter)
    } catch (e: IllegalArgumentException) {
      throw Status.INVALID_ARGUMENT.withDescription(e.message).asRuntimeException()
    }
  }

  object Permission {
    private const val TYPE = "reporting.metricCalculationSpecs"
    const val GET = "$TYPE.get"
    const val LIST = "$TYPE.list"
    const val CREATE = "$TYPE.create"
  }

  companion object {
    private val RESOURCE_ID_REGEX = ResourceIds.AIP_122_REGEX
    private const val MIN_PAGE_SIZE = 1
    private const val DEFAULT_PAGE_SIZE = 50
    private const val MAX_PAGE_SIZE = 1000
    private val ENV: Env = buildCelEnvironment(MetricCalculationSpec.getDefaultInstance())

    /**
     * Converts a public [ListMetricCalculationSpecsRequest] to a
     * [ListMetricCalculationSpecsPageToken].
     */
    private fun ListMetricCalculationSpecsRequest.toListMetricCalculationSpecsPageToken():
      ListMetricCalculationSpecsPageToken {
      grpcRequire(pageSize >= 0) { "Page size cannot be less than 0" }

      val source = this
      val parentKey: MeasurementConsumerKey =
        grpcRequireNotNull(MeasurementConsumerKey.fromName(parent)) {
          "Parent is either unspecified or invalid."
        }
      val cmmsMeasurementConsumerId = parentKey.measurementConsumerId

      return if (pageToken.isNotBlank()) {
        ListMetricCalculationSpecsPageToken.parseFrom(pageToken.base64UrlDecode()).copy {
          grpcRequire(this.cmmsMeasurementConsumerId == cmmsMeasurementConsumerId) {
            "Arguments must be kept the same when using a page token"
          }

          if (source.pageSize in MIN_PAGE_SIZE..MAX_PAGE_SIZE) {
            pageSize = source.pageSize
          }
        }
      } else {
        listMetricCalculationSpecsPageToken {
          pageSize =
            when {
              source.pageSize < MIN_PAGE_SIZE -> DEFAULT_PAGE_SIZE
              source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
              else -> source.pageSize
            }
          this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        }
      }
    }

    /**
     * Converts a [ListMetricCalculationSpecsPageToken] to an internal
     * [InternalListMetricCalculationSpecsRequest].
     */
    private fun ListMetricCalculationSpecsPageToken.toInternalListMetricCalculationSpecsRequest():
      InternalListMetricCalculationSpecsRequest {
      val source = this
      return listMetricCalculationSpecsRequest {
        limit = pageSize
        cmmsMeasurementConsumerId = source.cmmsMeasurementConsumerId
        if (source.hasLastMetricCalculationSpec()) {
          externalMetricCalculationSpecIdAfter =
            source.lastMetricCalculationSpec.externalMetricCalculationSpecId
        }
      }
    }

    /**
     * Converts a public [MetricCalculationSpec.MetricFrequencySpec] to an internal
     * [InternalMetricCalculationSpec.MetricFrequencySpec].
     */
    private fun MetricCalculationSpec.MetricFrequencySpec.toInternal():
      InternalMetricCalculationSpec.MetricFrequencySpec {
      val source = this
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      return InternalMetricCalculationSpecKt.metricFrequencySpec {
        when (source.frequencyCase) {
          MetricCalculationSpec.MetricFrequencySpec.FrequencyCase.DAILY -> {
            daily = InternalMetricCalculationSpec.MetricFrequencySpec.Daily.getDefaultInstance()
          }
          MetricCalculationSpec.MetricFrequencySpec.FrequencyCase.WEEKLY -> {
            grpcRequire(
              source.weekly.dayOfWeek != DayOfWeek.DAY_OF_WEEK_UNSPECIFIED &&
                source.weekly.dayOfWeek != DayOfWeek.UNRECOGNIZED
            ) {
              "day_of_week in weekly frequency is unspecified or invalid."
            }
            weekly =
              InternalMetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                dayOfWeek = source.weekly.dayOfWeek
              }
          }
          MetricCalculationSpec.MetricFrequencySpec.FrequencyCase.MONTHLY -> {
            grpcRequire(source.monthly.dayOfMonth > 0) {
              "day_of_month in monthly frequency is unspecified or invalid."
            }
            monthly =
              InternalMetricCalculationSpecKt.MetricFrequencySpecKt.monthly {
                dayOfMonth = source.monthly.dayOfMonth
              }
          }
          MetricCalculationSpec.MetricFrequencySpec.FrequencyCase.FREQUENCY_NOT_SET -> {}
        }
      }
    }

    /**
     * Converts a public [MetricCalculationSpec.TrailingWindow] to an internal
     * [InternalMetricCalculationSpec.TrailingWindow].
     */
    private fun MetricCalculationSpec.TrailingWindow.toInternal():
      InternalMetricCalculationSpec.TrailingWindow {
      val source = this

      grpcRequire(source.count >= 1) { "count in trailing_window must be greater than 0." }

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      return InternalMetricCalculationSpecKt.trailingWindow {
        count = source.count
        increment =
          when (source.increment) {
            MetricCalculationSpec.TrailingWindow.Increment.DAY ->
              InternalMetricCalculationSpec.TrailingWindow.Increment.DAY
            MetricCalculationSpec.TrailingWindow.Increment.WEEK ->
              InternalMetricCalculationSpec.TrailingWindow.Increment.WEEK
            MetricCalculationSpec.TrailingWindow.Increment.MONTH ->
              InternalMetricCalculationSpec.TrailingWindow.Increment.MONTH
            MetricCalculationSpec.TrailingWindow.Increment.UNRECOGNIZED,
            MetricCalculationSpec.TrailingWindow.Increment.INCREMENT_UNSPECIFIED ->
              throw Status.INVALID_ARGUMENT.withDescription(
                  "increment in trailing_window is not specified."
                )
                .asRuntimeException()
          }
      }
    }

    /** Converts an internal [InternalMetricCalculationSpec] to a public [MetricCalculationSpec]. */
    private fun InternalMetricCalculationSpec.toPublic(): MetricCalculationSpec {
      val source = this
      val metricCalculationSpecKey =
        MetricCalculationSpecKey(
          source.cmmsMeasurementConsumerId,
          source.externalMetricCalculationSpecId,
        )

      return metricCalculationSpec {
        name = metricCalculationSpecKey.toName()
        displayName = source.details.displayName
        metricSpecs += source.details.metricSpecsList.map(InternalMetricSpec::toMetricSpec)
        filter = source.details.filter
        groupings +=
          source.details.groupingsList.map { grouping ->
            MetricCalculationSpecKt.grouping { predicates += grouping.predicatesList }
          }
        if (source.details.hasMetricFrequencySpec()) {
          metricFrequencySpec = source.details.metricFrequencySpec.toPublic()
        }
        if (source.details.hasTrailingWindow()) {
          trailingWindow = source.details.trailingWindow.toPublic()
        }
        tags.putAll(source.details.tagsMap)
        modelLine = source.cmmsModelLine
      }
    }

    /**
     * Converts an internal [InternalMetricCalculationSpec.MetricFrequencySpec] to a public
     * [MetricCalculationSpec.MetricFrequencySpec].
     */
    private fun InternalMetricCalculationSpec.MetricFrequencySpec.toPublic():
      MetricCalculationSpec.MetricFrequencySpec {
      val source = this
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      return MetricCalculationSpecKt.metricFrequencySpec {
        when (source.frequencyCase) {
          InternalMetricCalculationSpec.MetricFrequencySpec.FrequencyCase.DAILY -> {
            daily = MetricCalculationSpec.MetricFrequencySpec.Daily.getDefaultInstance()
          }
          InternalMetricCalculationSpec.MetricFrequencySpec.FrequencyCase.WEEKLY -> {
            weekly =
              MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                dayOfWeek = source.weekly.dayOfWeek
              }
          }
          InternalMetricCalculationSpec.MetricFrequencySpec.FrequencyCase.MONTHLY -> {
            monthly =
              MetricCalculationSpecKt.MetricFrequencySpecKt.monthly {
                dayOfMonth = source.monthly.dayOfMonth
              }
          }
          InternalMetricCalculationSpec.MetricFrequencySpec.FrequencyCase.FREQUENCY_NOT_SET -> {}
        }
      }
    }

    /**
     * Converts an internal [InternalMetricCalculationSpec.TrailingWindow] to a public
     * [MetricCalculationSpec.TrailingWindow].
     */
    private fun InternalMetricCalculationSpec.TrailingWindow.toPublic():
      MetricCalculationSpec.TrailingWindow {
      val source = this
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      return MetricCalculationSpecKt.trailingWindow {
        count = source.count
        increment =
          when (source.increment) {
            InternalMetricCalculationSpec.TrailingWindow.Increment.DAY ->
              MetricCalculationSpec.TrailingWindow.Increment.DAY
            InternalMetricCalculationSpec.TrailingWindow.Increment.WEEK ->
              MetricCalculationSpec.TrailingWindow.Increment.WEEK
            InternalMetricCalculationSpec.TrailingWindow.Increment.MONTH ->
              MetricCalculationSpec.TrailingWindow.Increment.MONTH
            InternalMetricCalculationSpec.TrailingWindow.Increment.UNRECOGNIZED,
            InternalMetricCalculationSpec.TrailingWindow.Increment.INCREMENT_UNSPECIFIED ->
              throw Status.FAILED_PRECONDITION.withDescription(
                  "MetricCalculationSpec trailing_window missing increment"
                )
                .asRuntimeException()
          }
      }
    }
  }
}
