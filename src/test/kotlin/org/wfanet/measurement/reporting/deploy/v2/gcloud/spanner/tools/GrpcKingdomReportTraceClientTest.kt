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
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.api.v2alpha.batchGetMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.batchGetMeasurementsResponse
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService

@RunWith(JUnit4::class)
class GrpcKingdomReportTraceClientTest {
  private val measurementsService: MeasurementsGrpcKt.MeasurementsCoroutineImplBase = mockService {
    onBlocking { batchGetMeasurements(any()) }
      .thenReturn(
        batchGetMeasurementsResponse { measurements += measurement { name = MEASUREMENT_NAME } }
      )
  }
  private val requisitionsService: RequisitionsGrpcKt.RequisitionsCoroutineImplBase = mockService {
    onBlocking { listRequisitions(any()) }
      .thenReturn(
        listRequisitionsResponse { requisitions += requisition { name = REQUISITION_NAME } }
      )
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(measurementsService)
    addService(requisitionsService)
  }

  @Test
  fun `client reads Measurements and Requisitions through gRPC`(): Unit = runBlocking {
    val client =
      GrpcKingdomReportTraceClient(
        MeasurementsGrpcKt.MeasurementsCoroutineStub(grpcTestServerRule.channel),
        RequisitionsGrpcKt.RequisitionsCoroutineStub(grpcTestServerRule.channel),
      )

    val measurements =
      client.batchGetMeasurements(
        batchGetMeasurementsRequest {
          parent = "measurementConsumers/mc-1"
          names += MEASUREMENT_NAME
        }
      )
    val requisitions =
      client.listRequisitions(listRequisitionsRequest { parent = MEASUREMENT_NAME })

    assertThat(measurements.measurementsList.map { it.name }).containsExactly(MEASUREMENT_NAME)
    assertThat(requisitions.requisitionsList.map { it.name }).containsExactly(REQUISITION_NAME)
  }

  companion object {
    private const val MEASUREMENT_NAME = "measurementConsumers/mc-1/measurements/measurement-1"
    private const val REQUISITION_NAME = "dataProviders/edp-1/requisitions/requisition-1"
  }
}
