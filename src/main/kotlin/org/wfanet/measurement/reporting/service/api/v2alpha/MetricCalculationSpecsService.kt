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
import org.projectnessie.cel.Env
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
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
  private val metricSpecConfig: MetricSpecConfig,
) : MetricCalculationSpecsCoroutineImplBase() {
  override suspend fun createMetricCalculationSpec(
    request: CreateMetricCalculationSpecRequest
  ): MetricCalculationSpec {
    val parentKey: MeasurementConsumerKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }

    when (val principal: ReportingPrincipal = principalFromCurrentContext) {
      is MeasurementConsumerPrincipal -> {
        if (parentKey.measurementConsumerId != principal.resourceKey.measurementConsumerId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create a MetricCalculationSpec for another MeasurementConsumer."
          }
        }
      }
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

    val internalCreateMetricCalculationSpecRequest = createMetricCalculationSpecRequest {
      metricCalculationSpec =
        request.metricCalculationSpec.toInternal(parentKey.measurementConsumerId)
      externalMetricCalculationSpecId = request.metricCalculationSpecId
    }

    val internalMetricCalculationSpec =
      try {
        internalMetricCalculationSpecsStub.createMetricCalculationSpec(
          internalCreateMetricCalculationSpecRequest
        )
      } catch (e: StatusException) {
        throw Exception("Unable to create Metric Calculation Spec.", e)
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

    when (val principal: ReportingPrincipal = principalFromCurrentContext) {
      is MeasurementConsumerPrincipal -> {
        if (metricCalculationSpecKey.parentKey != principal.resourceKey) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot get MetricCalculationSpec belonging to other MeasurementConsumers."
          }
        }
      }
    }

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
            Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
            Status.Code.CANCELLED -> Status.CANCELLED
            Status.Code.NOT_FOUND -> Status.NOT_FOUND
            else -> Status.UNKNOWN
          }
          .withCause(e)
          .withDescription("Unable to get MetricCalculationSpec.")
          .asRuntimeException()
      }

    return internalMetricCalculationSpec.toPublic()
  }

  override suspend fun listMetricCalculationSpecs(
    request: ListMetricCalculationSpecsRequest
  ): ListMetricCalculationSpecsResponse {
    val parentKey: MeasurementConsumerKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }
    val listMetricCalculationSpecsPageToken = request.toListMetricCalculationSpecsPageToken()

    when (val principal: ReportingPrincipal = principalFromCurrentContext) {
      is MeasurementConsumerPrincipal -> {
        if (parentKey.measurementConsumerId != principal.resourceKey.measurementConsumerId) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot list MetricCalculationSpecs belonging to other MeasurementConsumers."
          }
        }
      }
    }

    val internalListMetricCalculationSpecsRequest =
      listMetricCalculationSpecsPageToken.toInternalListMetricCalculationSpecsRequest()

    val response: InternalListMetricCalculationSpecsResponse =
      try {
        internalMetricCalculationSpecsStub.listMetricCalculationSpecs(
          internalListMetricCalculationSpecsRequest
        )
      } catch (e: StatusException) {
        throw when (e.status.code) {
            Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
            Status.Code.CANCELLED -> Status.CANCELLED
            else -> Status.UNKNOWN
          }
          .withCause(e)
          .withDescription("Unable to list MetricCalculationSpecs.")
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
          request.filter
        )

      if (nextPageToken != null) {
        this.nextPageToken = nextPageToken.toByteString().base64UrlEncode()
      }
    }
  }

  /** Converts a public [MetricCalculationSpec] to an internal [InternalMetricCalculationSpec]. */
  private fun MetricCalculationSpec.toInternal(
    cmmsMeasurementConsumerId: String
  ): InternalMetricCalculationSpec {
    val source = this

    if (source.hasMetricFrequencySpecAndWindow()) {
      grpcRequire(source.metricFrequencySpecAndWindow.hasMetricFrequencySpec() && source.metricFrequencySpecAndWindow.hasWindow()) {
        "metric_frequency_spec and window must both be set"
      }
    }

    val internalMetricSpecs =
      source.metricSpecsList.map { metricSpec ->
        try {
          metricSpec.withDefaults(metricSpecConfig).toInternal()
        } catch (e: MetricSpecDefaultsException) {
          failGrpc(Status.INVALID_ARGUMENT) {
            listOfNotNull("Invalid metric_spec.", e.message, e.cause?.message)
              .joinToString(separator = "\n")
          }
        } catch (e: Exception) {
          failGrpc(Status.UNKNOWN) { "Failed to read the metric_spec." }
        }
      }

    return internalMetricCalculationSpec {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      details =
        InternalMetricCalculationSpecKt.details {
          displayName = source.displayName
          metricSpecs += internalMetricSpecs
          filter = source.filter
          groupings +=
            source.groupingsList.map { grouping ->
              InternalMetricCalculationSpecKt.grouping { predicates += grouping.predicatesList }
            }
          if (source.hasMetricFrequencySpecAndWindow()) {
            metricFrequencySpec = source.metricFrequencySpecAndWindow.metricFrequencySpec.toInternal()
            window = source.metricFrequencySpecAndWindow.window.toInternal()
          }
          tags.putAll(source.tagsMap)
        }
    }
  }

  private fun filterMetricCalculationSpecs(
    metricCalculationSpecs: List<MetricCalculationSpec>,
    filter: String
  ): List<MetricCalculationSpec> {
    return try {
      filterList(ENV, metricCalculationSpecs, filter)
    } catch (e: IllegalArgumentException) {
      throw Status.INVALID_ARGUMENT.withDescription(e.message).asRuntimeException()
    }
  }

  companion object {
    private val RESOURCE_ID_REGEX = Regex("^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$")
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
     * Converts a public [MetricCalculationSpec.Window] to an internal
     * [InternalMetricCalculationSpec.Window].
     */
    private fun MetricCalculationSpec.Window.toInternal(): InternalMetricCalculationSpec.Window {
      val source = this
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      return InternalMetricCalculationSpecKt.window {
        when (source.windowCase) {
          MetricCalculationSpec.Window.WindowCase.TRAILING_WINDOW -> {
            grpcRequire(source.trailingWindow.count >= 1) {
              "count in trailing_window must be greater than 0."
            }
            trailingWindow =
              InternalMetricCalculationSpecKt.WindowKt.trailingWindow {
                count = source.trailingWindow.count
                increment =
                  when (source.trailingWindow.increment) {
                    MetricCalculationSpec.Window.TrailingWindow.Increment.DAY ->
                      InternalMetricCalculationSpec.Window.TrailingWindow.Increment.DAY
                    MetricCalculationSpec.Window.TrailingWindow.Increment.WEEK ->
                      InternalMetricCalculationSpec.Window.TrailingWindow.Increment.WEEK
                    MetricCalculationSpec.Window.TrailingWindow.Increment.MONTH ->
                      InternalMetricCalculationSpec.Window.TrailingWindow.Increment.MONTH
                    MetricCalculationSpec.Window.TrailingWindow.Increment.UNRECOGNIZED,
                    MetricCalculationSpec.Window.TrailingWindow.Increment.INCREMENT_UNSPECIFIED ->
                      throw Status.INVALID_ARGUMENT.withDescription(
                          "increment in trailing_window is not specified."
                        )
                        .asRuntimeException()
                  }
              }
          }
          MetricCalculationSpec.Window.WindowCase.REPORT_START_TIME -> {
            grpcRequire(source.reportStartTime) {
              "report_start_time is false"
            }
            reportStartTime = true
          }
          MetricCalculationSpec.Window.WindowCase.WINDOW_NOT_SET -> {}
        }
      }
    }

    /** Converts an internal [InternalMetricCalculationSpec] to a public [MetricCalculationSpec]. */
    private fun InternalMetricCalculationSpec.toPublic(): MetricCalculationSpec {
      val source = this
      val metricCalculationSpecKey =
        MetricCalculationSpecKey(
          source.cmmsMeasurementConsumerId,
          source.externalMetricCalculationSpecId
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
        if (source.details.hasMetricFrequencySpec() && source.details.hasWindow()) {
          metricFrequencySpecAndWindow = MetricCalculationSpecKt.metricFrequencySpecAndWindow {
            metricFrequencySpec = source.details.metricFrequencySpec.toPublic()
            window = source.details.window.toPublic()
          }
        }
        tags.putAll(source.details.tagsMap)
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
     * Converts an internal [InternalMetricCalculationSpec.Window] to a public
     * [MetricCalculationSpec.Window].
     */
    private fun InternalMetricCalculationSpec.Window.toPublic(): MetricCalculationSpec.Window {
      val source = this
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      return MetricCalculationSpecKt.window {
        when (source.windowCase) {
          InternalMetricCalculationSpec.Window.WindowCase.TRAILING_WINDOW -> {
            trailingWindow =
              MetricCalculationSpecKt.WindowKt.trailingWindow {
                count = source.trailingWindow.count
                increment =
                  when (source.trailingWindow.increment) {
                    InternalMetricCalculationSpec.Window.TrailingWindow.Increment.DAY ->
                      MetricCalculationSpec.Window.TrailingWindow.Increment.DAY
                    InternalMetricCalculationSpec.Window.TrailingWindow.Increment.WEEK ->
                      MetricCalculationSpec.Window.TrailingWindow.Increment.WEEK
                    InternalMetricCalculationSpec.Window.TrailingWindow.Increment.MONTH ->
                      MetricCalculationSpec.Window.TrailingWindow.Increment.MONTH
                    InternalMetricCalculationSpec.Window.TrailingWindow.Increment.UNRECOGNIZED,
                    InternalMetricCalculationSpec.Window.TrailingWindow.Increment
                      .INCREMENT_UNSPECIFIED ->
                      throw Status.FAILED_PRECONDITION.withDescription(
                          "MetricCalculationSpec trailing_window missing increment"
                        )
                        .asRuntimeException()
                  }
              }
          }
          InternalMetricCalculationSpec.Window.WindowCase.REPORT_START_TIME -> {
            reportStartTime = source.reportStartTime
          }
          InternalMetricCalculationSpec.Window.WindowCase.WINDOW_NOT_SET -> {}
        }
      }
    }
  }
}
