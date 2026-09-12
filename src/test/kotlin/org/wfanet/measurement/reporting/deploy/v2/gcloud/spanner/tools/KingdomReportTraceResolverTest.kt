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

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import java.time.Duration
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.test.runTest
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.BatchGetMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.BatchGetMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.batchGetMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.protocolConfig
import org.wfanet.measurement.api.v2alpha.requisition

@RunWith(JUnit4::class)
class KingdomReportTraceResolverTest {
  @Test
  fun `resolve classifies direct and MPC paths from Kingdom resources`() = runTest {
    val directMeasurement = directMeasurement(MEASUREMENT_1)
    val mpcMeasurement = mpcMeasurement(MEASUREMENT_2)
    val client =
      FakeKingdomReportTraceClient(
        batchGet = {
          batchGetMeasurementsResponse { measurements += listOf(directMeasurement, mpcMeasurement) }
        },
        list = { request ->
          listRequisitionsResponse {
            requisitions +=
              when (request.parent) {
                MEASUREMENT_1 -> requisition(DIRECT_REQUISITION, Requisition.State.FULFILLED)
                MEASUREMENT_2 -> requisition(EDPA_REQUISITION, Requisition.State.UNFULFILLED)
                else -> error("Unexpected Measurement")
              }
          }
        },
      )
    val resolver = resolver(client)

    val result =
      resolver.resolve(
        listOf(MEASUREMENT_1, MEASUREMENT_2),
        topology(
          DIRECT_EDP to ReportTraceRequisitionRouteKind.DIRECT_EDP,
          EDPA to ReportTraceRequisitionRouteKind.EDPA,
        ),
      )

    assertThat(result.status).isEqualTo("SUCCESS")
    assertThat(result.measurementRoutes.map { it.route })
      .containsExactly(ReportTraceMeasurementRouteKind.DIRECT, ReportTraceMeasurementRouteKind.MPC)
      .inOrder()
    assertThat(result.measurementRoutes.flatMap { it.requisitions }.map { it.route })
      .containsExactly(
        ReportTraceRequisitionRouteKind.DIRECT_EDP,
        ReportTraceRequisitionRouteKind.EDPA,
      )
      .inOrder()
    assertThat(result.requirementFor("duchy_computation"))
      .isEqualTo(ReportTraceStageRequirement.REQUIRED)
    assertThat(result.requirementFor("results_fulfillment"))
      .isEqualTo(ReportTraceStageRequirement.REQUIRED)
    assertThat(result.correlationValues).containsExactly(DIRECT_REQUISITION, EDPA_REQUISITION)
  }

  @Test
  fun `resolve marks direct Measurement Duchy stages not applicable`() = runTest {
    val client =
      FakeKingdomReportTraceClient(
        batchGet = {
          batchGetMeasurementsResponse { measurements += directMeasurement(MEASUREMENT_1) }
        },
        list = {
          listRequisitionsResponse {
            requisitions += requisition(DIRECT_REQUISITION, Requisition.State.FULFILLED)
          }
        },
      )

    val result =
      resolver(client)
        .resolve(
          listOf(MEASUREMENT_1),
          topology(DIRECT_EDP to ReportTraceRequisitionRouteKind.DIRECT_EDP),
        )

    assertThat(result.requirementFor("duchy_computation"))
      .isEqualTo(ReportTraceStageRequirement.NOT_APPLICABLE)
    assertThat(result.requirementFor("results_fulfillment"))
      .isEqualTo(ReportTraceStageRequirement.NOT_APPLICABLE)
  }

  @Test
  fun `resolve leaves EDP route unknown when complete topology omits DataProvider`() = runTest {
    val client =
      FakeKingdomReportTraceClient(
        batchGet = {
          batchGetMeasurementsResponse { measurements += directMeasurement(MEASUREMENT_1) }
        },
        list = {
          listRequisitionsResponse {
            requisitions += requisition(DIRECT_REQUISITION, Requisition.State.UNFULFILLED)
          }
        },
      )

    val result =
      resolver(client)
        .resolve(listOf(MEASUREMENT_1), topology(EDPA to ReportTraceRequisitionRouteKind.EDPA))

    assertThat(result.status).isEqualTo("PARTIAL")
    assertThat(result.measurementRoutes.single().requisitions.single().route)
      .isEqualTo(ReportTraceRequisitionRouteKind.UNKNOWN)
    assertThat(result.requirementFor("requisition_dispatch"))
      .isEqualTo(ReportTraceStageRequirement.UNKNOWN)
    assertThat(result.note).contains(DIRECT_EDP)
  }

  @Test
  fun `resolve retries transient RPC and paginates Requisitions`() = runTest {
    var batchCalls = 0
    val pageTokens = mutableListOf<String>()
    val client =
      FakeKingdomReportTraceClient(
        batchGet = {
          batchCalls++
          if (batchCalls == 1) {
            throw Status.UNAVAILABLE.asRuntimeException()
          }
          batchGetMeasurementsResponse { measurements += mpcMeasurement(MEASUREMENT_1) }
        },
        list = { request ->
          pageTokens += request.pageToken
          if (request.pageToken.isEmpty()) {
            listRequisitionsResponse {
              requisitions += requisition(DIRECT_REQUISITION, Requisition.State.FULFILLED)
              nextPageToken = "page-2"
            }
          } else {
            listRequisitionsResponse {
              requisitions += requisition(EDPA_REQUISITION, Requisition.State.REFUSED)
            }
          }
        },
      )

    val result =
      resolver(client)
        .resolve(
          listOf(MEASUREMENT_1),
          topology(
            DIRECT_EDP to ReportTraceRequisitionRouteKind.DIRECT_EDP,
            EDPA to ReportTraceRequisitionRouteKind.EDPA,
          ),
        )

    assertThat(batchCalls).isEqualTo(2)
    assertThat(pageTokens).containsExactly("", "page-2").inOrder()
    assertThat(result.measurementRoutes.single().requisitions).hasSize(2)
  }

  @Test
  fun `resolve marks all affected routes unknown after deadline`() = runTest {
    val client =
      FakeKingdomReportTraceClient(
        batchGet = { awaitCancellation() },
        list = { error("ListRequisitions should not be called") },
      )
    val resolver =
      KingdomReportTraceResolver(
        client = client,
        perReportDeadline = Duration.ofSeconds(1),
        maxConcurrency = 1,
        maxRpcAttempts = 1,
        initialRetryDelay = Duration.ZERO,
      )

    val result =
      resolver.resolve(
        listOf(MEASUREMENT_1),
        topology(EDPA to ReportTraceRequisitionRouteKind.EDPA),
      )

    assertThat(result.status).isEqualTo("FAILED")
    assertThat(result.measurementRoutes.single().route)
      .isEqualTo(ReportTraceMeasurementRouteKind.UNKNOWN)
    assertThat(result.note).contains("deadline")
  }

  @Test
  fun `resolve batches Measurement lookups at API maximum`() = runTest {
    val batchSizes = mutableListOf<Int>()
    val names = (1..51).map { "measurementConsumers/mc-1/measurements/measurement-$it" }
    val client =
      FakeKingdomReportTraceClient(
        batchGet = { request ->
          batchSizes += request.namesCount
          batchGetMeasurementsResponse {
            measurements += request.namesList.map(::directMeasurement)
          }
        },
        list = { listRequisitionsResponse {} },
      )

    val result =
      resolver(client).resolve(names, topology(EDPA to ReportTraceRequisitionRouteKind.EDPA))

    assertThat(batchSizes).containsExactly(50, 1).inOrder()
    assertThat(result.measurementRoutes).hasSize(51)
  }

  private fun resolver(client: KingdomReportTraceClient): KingdomReportTraceResolver {
    return KingdomReportTraceResolver(
      client = client,
      perReportDeadline = Duration.ofSeconds(30),
      maxConcurrency = 2,
      maxRpcAttempts = 2,
      initialRetryDelay = Duration.ZERO,
    )
  }

  private fun topology(
    vararg routes: Pair<String, ReportTraceRequisitionRouteKind>
  ): ReportTraceTopology =
    ReportTraceTopology(routes.toMap(), "operator-provided --topology-config-file")

  private fun directMeasurement(name: String): Measurement = measurement {
    this.name = name
    state = Measurement.State.SUCCEEDED
    protocolConfig = protocolConfig {
      protocols += ProtocolConfigKt.protocol { direct = ProtocolConfigKt.direct {} }
    }
  }

  private fun mpcMeasurement(name: String): Measurement = measurement {
    this.name = name
    state = Measurement.State.COMPUTING
    protocolConfig = protocolConfig {
      protocols +=
        ProtocolConfigKt.protocol {
          honestMajorityShareShuffle = ProtocolConfigKt.honestMajorityShareShuffle {}
        }
    }
  }

  private fun requisition(name: String, state: Requisition.State): Requisition = requisition {
    this.name = name
    this.state = state
  }

  private class FakeKingdomReportTraceClient(
    private val batchGet: suspend (BatchGetMeasurementsRequest) -> BatchGetMeasurementsResponse,
    private val list: suspend (ListRequisitionsRequest) -> ListRequisitionsResponse,
  ) : KingdomReportTraceClient {
    override suspend fun batchGetMeasurements(
      request: BatchGetMeasurementsRequest
    ): BatchGetMeasurementsResponse = batchGet(request)

    override suspend fun listRequisitions(
      request: ListRequisitionsRequest
    ): ListRequisitionsResponse = list(request)
  }

  companion object {
    private const val MEASUREMENT_1 = "measurementConsumers/mc-1/measurements/measurement-1"
    private const val MEASUREMENT_2 = "measurementConsumers/mc-1/measurements/measurement-2"
    private const val DIRECT_EDP = "dataProviders/direct-edp"
    private const val EDPA = "dataProviders/edpa"
    private const val DIRECT_REQUISITION = "$DIRECT_EDP/requisitions/requisition-1"
    private const val EDPA_REQUISITION = "$EDPA/requisitions/requisition-2"
  }
}
