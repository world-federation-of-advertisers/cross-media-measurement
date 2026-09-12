/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.tools

import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import java.time.Duration
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeout
import org.wfanet.measurement.api.v2alpha.BatchGetMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.BatchGetMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.CanonicalRequisitionKey
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.batchGetMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest

internal enum class ReportTraceStageRequirement {
  REQUIRED,
  NOT_APPLICABLE,
  UNKNOWN,
  REUSED,
  SKIPPED_AFTER_FAILURE,
  SKIPPED_AFTER_REFUSAL,
}

internal enum class ReportTraceMeasurementRouteKind {
  DIRECT,
  MPC,
  UNKNOWN,
}

internal enum class ReportTraceRequisitionRouteKind {
  EDPA,
  DIRECT_EDP,
  UNKNOWN,
}

internal data class ReportTraceRequisitionRoute(
  val name: String,
  val state: String,
  val dataProvider: String,
  val route: ReportTraceRequisitionRouteKind,
)

internal data class ReportTraceMeasurementRoute(
  val name: String,
  val state: String,
  val protocol: String,
  val route: ReportTraceMeasurementRouteKind,
  val duchyIds: List<String>,
  val duchyParticipantsResolved: Boolean,
  val requisitions: List<ReportTraceRequisitionRoute>,
  val requisitionsResolved: Boolean,
)

internal data class ReportTraceRouteResolution(
  val status: String,
  val note: String,
  val topology: ReportTraceTopology,
  val measurementRoutes: List<ReportTraceMeasurementRoute>,
  val warnings: List<String>,
) {
  val correlationValues: List<String>
    get() = measurementRoutes.flatMap { route -> route.requisitions.map { it.name } }.distinct()

  val fetchedResourceCount: Int
    get() = measurementRoutes.count { it.state != UNKNOWN_VALUE } + correlationValues.size

  companion object {
    private const val UNKNOWN_VALUE = "UNKNOWN"

    fun unresolved(
      measurementNames: Collection<String>,
      topology: ReportTraceTopology,
      status: String,
      note: String,
    ): ReportTraceRouteResolution {
      return ReportTraceRouteResolution(
        status = status,
        note = note,
        topology = topology,
        measurementRoutes =
          measurementNames.distinct().sorted().map { name ->
            ReportTraceMeasurementRoute(
              name = name,
              state = UNKNOWN_VALUE,
              protocol = UNKNOWN_VALUE,
              route = ReportTraceMeasurementRouteKind.UNKNOWN,
              duchyIds = emptyList(),
              duchyParticipantsResolved = false,
              requisitions = emptyList(),
              requisitionsResolved = false,
            )
          },
        warnings = listOf(note),
      )
    }
  }
}

internal fun interface ReportTraceRouteResolver {
  suspend fun resolve(
    measurementNames: Collection<String>,
    topology: ReportTraceTopology,
  ): ReportTraceRouteResolution
}

internal interface KingdomReportTraceClient {
  suspend fun batchGetMeasurements(
    request: BatchGetMeasurementsRequest
  ): BatchGetMeasurementsResponse

  suspend fun listRequisitions(request: ListRequisitionsRequest): ListRequisitionsResponse
}

internal class GrpcKingdomReportTraceClient(
  private val measurementsStub: MeasurementsCoroutineStub,
  private val requisitionsStub: RequisitionsCoroutineStub,
) : KingdomReportTraceClient {
  override suspend fun batchGetMeasurements(
    request: BatchGetMeasurementsRequest
  ): BatchGetMeasurementsResponse = measurementsStub.batchGetMeasurements(request)

  override suspend fun listRequisitions(
    request: ListRequisitionsRequest
  ): ListRequisitionsResponse = requisitionsStub.listRequisitions(request)
}

/** Resolves protocol and Requisition routes from the Kingdom public API. */
internal class KingdomReportTraceResolver(
  private val client: KingdomReportTraceClient,
  private val perReportDeadline: Duration,
  private val maxConcurrency: Int,
  private val maxRpcAttempts: Int,
  private val initialRetryDelay: Duration,
) : ReportTraceRouteResolver {
  init {
    require(!perReportDeadline.isZero && !perReportDeadline.isNegative) {
      "perReportDeadline must be positive"
    }
    require(maxConcurrency > 0) { "maxConcurrency must be positive" }
    require(maxRpcAttempts > 0) { "maxRpcAttempts must be positive" }
    require(!initialRetryDelay.isNegative) { "initialRetryDelay must be non-negative" }
  }

  override suspend fun resolve(
    measurementNames: Collection<String>,
    topology: ReportTraceTopology,
  ): ReportTraceRouteResolution {
    val distinctMeasurementNames = measurementNames.distinct().sorted()
    if (distinctMeasurementNames.isEmpty()) {
      return ReportTraceRouteResolution.unresolved(
        measurementNames = emptyList(),
        topology = topology,
        status = "NO_INPUT",
        note = "No Kingdom Measurement names were resolved from Reporting",
      )
    }

    return try {
      withTimeout(perReportDeadline.toMillis()) {
        resolveWithinDeadline(distinctMeasurementNames, topology)
      }
    } catch (e: TimeoutCancellationException) {
      ReportTraceRouteResolution.unresolved(
        measurementNames = distinctMeasurementNames,
        topology = topology,
        status = "FAILED",
        note = "Kingdom route resolution exceeded the per-report deadline",
      )
    }
  }

  private suspend fun resolveWithinDeadline(
    measurementNames: List<String>,
    topology: ReportTraceTopology,
  ): ReportTraceRouteResolution {
    val parent =
      checkNotNull(MeasurementKey.fromName(measurementNames.first())) {
          "Invalid Measurement resource name"
        }
        .parentKey
        .toName()
    check(
      measurementNames.all { name -> MeasurementKey.fromName(name)?.parentKey?.toName() == parent }
    ) {
      "All Measurements for one report must have the same MeasurementConsumer"
    }

    val semaphore = Semaphore(maxConcurrency)
    val measurementBatchResults = coroutineScope {
      measurementNames
        .chunked(MAX_MEASUREMENT_BATCH_SIZE)
        .map { names ->
          async {
            semaphore.withPermit {
              try {
                val response = callWithRetry {
                  client.batchGetMeasurements(
                    batchGetMeasurementsRequest {
                      this.parent = parent
                      this.names += names
                    }
                  )
                }
                MeasurementBatchResult(response.measurementsList, emptyList(), null)
              } catch (e: CancellationException) {
                throw e
              } catch (e: Exception) {
                MeasurementBatchResult(emptyList(), names, failureDescription(e))
              }
            }
          }
        }
        .awaitAll()
    }
    val measurements = measurementBatchResults.flatMap { it.measurements }
    val failures =
      measurementBatchResults
        .mapNotNull { result ->
          result.failure?.let { failure ->
            "Measurements ${result.unresolvedNames.joinToString()} could not be resolved: $failure"
          }
        }
        .toMutableList()

    val resolvedRoutes = coroutineScope {
      measurements
        .map { measurement ->
          async {
            semaphore.withPermit {
              val protocol = protocolRoute(measurement)
              try {
                val requisitions = listAllRequisitions(measurement.name)
                val duchyIds: List<String> =
                  requisitions
                    .flatMap { requisition -> requisition.duchiesList.map { duchy -> duchy.key } }
                    .distinct()
                    .sorted()
                val duchyParticipantsResolved: Boolean =
                  protocol.route != ReportTraceMeasurementRouteKind.MPC || duchyIds.isNotEmpty()
                MeasurementRouteResult(
                  route =
                    ReportTraceMeasurementRoute(
                      name = measurement.name,
                      state = measurement.state.name,
                      protocol = protocol.name,
                      route = protocol.route,
                      duchyIds = duchyIds,
                      duchyParticipantsResolved = duchyParticipantsResolved,
                      requisitions =
                        requisitions.map { requisition -> requisitionRoute(requisition, topology) },
                      requisitionsResolved = true,
                    ),
                  failure =
                    if (duchyParticipantsResolved) {
                      null
                    } else {
                      "Duchy participants for ${measurement.name} could not be resolved"
                    },
                )
              } catch (e: CancellationException) {
                throw e
              } catch (e: Exception) {
                MeasurementRouteResult(
                  route =
                    ReportTraceMeasurementRoute(
                      name = measurement.name,
                      state = measurement.state.name,
                      protocol = protocol.name,
                      route = protocol.route,
                      duchyIds = emptyList(),
                      duchyParticipantsResolved = false,
                      requisitions = emptyList(),
                      requisitionsResolved = false,
                    ),
                  failure =
                    "Requisitions for ${measurement.name} could not be resolved: " +
                      failureDescription(e),
                )
              }
            }
          }
        }
        .awaitAll()
    }
    failures += resolvedRoutes.mapNotNull { it.failure }

    val unresolvedRoutes =
      measurementBatchResults.flatMap { result ->
        result.unresolvedNames.map { name ->
          ReportTraceMeasurementRoute(
            name = name,
            state = UNKNOWN_VALUE,
            protocol = UNKNOWN_VALUE,
            route = ReportTraceMeasurementRouteKind.UNKNOWN,
            duchyIds = emptyList(),
            duchyParticipantsResolved = false,
            requisitions = emptyList(),
            requisitionsResolved = false,
          )
        }
      }
    val measurementRoutes =
      (resolvedRoutes.map { it.route } + unresolvedRoutes).sortedBy { it.name }
    val missingDataProviders =
      measurementRoutes
        .flatMap { it.requisitions }
        .filter { it.route == ReportTraceRequisitionRouteKind.UNKNOWN }
        .map { it.dataProvider }
        .filter { it != UNKNOWN_VALUE }
        .distinct()
        .sorted()
    if (missingDataProviders.isNotEmpty()) {
      failures +=
        "Topology config has no route for DataProviders: ${missingDataProviders.joinToString()}"
    }

    return ReportTraceRouteResolution(
      status =
        when {
          measurements.isEmpty() -> "FAILED"
          failures.isNotEmpty() -> "PARTIAL"
          else -> "SUCCESS"
        },
      note = failures.joinToString("; "),
      topology = topology,
      measurementRoutes = measurementRoutes,
      warnings = failures,
    )
  }

  private suspend fun listAllRequisitions(measurementName: String): List<Requisition> {
    val requisitions = mutableListOf<Requisition>()
    var pageToken = ""
    do {
      val response = callWithRetry {
        client.listRequisitions(
          listRequisitionsRequest {
            parent = measurementName
            pageSize = MAX_REQUISITION_PAGE_SIZE
            this.pageToken = pageToken
          }
        )
      }
      requisitions += response.requisitionsList
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return requisitions
  }

  private suspend fun <T> callWithRetry(block: suspend () -> T): T {
    var attempt = 1
    while (true) {
      try {
        return block()
      } catch (e: CancellationException) {
        throw e
      } catch (e: Exception) {
        if (attempt >= maxRpcAttempts || Status.fromThrowable(e).code !in RETRYABLE_STATUS_CODES) {
          throw e
        }
        val retryDelay = initialRetryDelay.multipliedBy(1L shl (attempt - 1))
        delay(retryDelay.toMillis())
        attempt++
      }
    }
  }

  private fun protocolRoute(measurement: Measurement): ProtocolRoute {
    val protocolCases =
      measurement.protocolConfig.protocolsList.map { protocol -> protocol.protocolCase }.distinct()
    if (protocolCases.size == 1) {
      val protocolCase = protocolCases.single()
      return when (protocolCase) {
        ProtocolConfig.Protocol.ProtocolCase.DIRECT ->
          ProtocolRoute(protocolCase.name, ReportTraceMeasurementRouteKind.DIRECT)
        ProtocolConfig.Protocol.ProtocolCase.LIQUID_LEGIONS_V2,
        ProtocolConfig.Protocol.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2,
        ProtocolConfig.Protocol.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE,
        ProtocolConfig.Protocol.ProtocolCase.TRUS_TEE,
        ProtocolConfig.Protocol.ProtocolCase.TRUS_TEE_V2 ->
          ProtocolRoute(protocolCase.name, ReportTraceMeasurementRouteKind.MPC)
        ProtocolConfig.Protocol.ProtocolCase.PROTOCOL_NOT_SET ->
          ProtocolRoute(UNKNOWN_VALUE, ReportTraceMeasurementRouteKind.UNKNOWN)
      }
    }
    if (
      protocolCases.isEmpty() &&
        measurement.protocolConfig.protocolCase == ProtocolConfig.ProtocolCase.LIQUID_LEGIONS_V2
    ) {
      return ProtocolRoute("LIQUID_LEGIONS_V2", ReportTraceMeasurementRouteKind.MPC)
    }
    return ProtocolRoute(UNKNOWN_VALUE, ReportTraceMeasurementRouteKind.UNKNOWN)
  }

  private fun requisitionRoute(
    requisition: Requisition,
    topology: ReportTraceTopology,
  ): ReportTraceRequisitionRoute {
    val dataProvider = CanonicalRequisitionKey.fromName(requisition.name)?.parentKey?.toName()
    val route = dataProvider?.let(topology::routeFor) ?: ReportTraceRequisitionRouteKind.UNKNOWN
    return ReportTraceRequisitionRoute(
      name = requisition.name,
      state = requisition.state.name,
      dataProvider = dataProvider ?: UNKNOWN_VALUE,
      route = route,
    )
  }

  private fun failureDescription(exception: Exception): String {
    return when (exception) {
      is StatusException,
      is StatusRuntimeException -> "gRPC ${Status.fromThrowable(exception).code}"
      else -> exception::class.java.simpleName
    }
  }

  private data class ProtocolRoute(val name: String, val route: ReportTraceMeasurementRouteKind)

  private data class MeasurementBatchResult(
    val measurements: List<Measurement>,
    val unresolvedNames: List<String>,
    val failure: String?,
  )

  private data class MeasurementRouteResult(
    val route: ReportTraceMeasurementRoute,
    val failure: String?,
  )

  companion object {
    private const val MAX_MEASUREMENT_BATCH_SIZE = 50
    private const val MAX_REQUISITION_PAGE_SIZE = 500
    private const val UNKNOWN_VALUE = "UNKNOWN"
    private val RETRYABLE_STATUS_CODES =
      setOf(
        Status.Code.ABORTED,
        Status.Code.DEADLINE_EXCEEDED,
        Status.Code.RESOURCE_EXHAUSTED,
        Status.Code.UNAVAILABLE,
      )
  }
}
