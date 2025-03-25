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

package org.wfanet.measurement.reporting.service.api

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusException
import kotlin.math.ceil
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.KArgumentCaptor
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.internal.reporting.v2.BatchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.BatchGetReportingSetsResponse
import org.wfanet.measurement.internal.reporting.v2.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsResponse
import org.wfanet.measurement.internal.reporting.v2.reportingSet as internalReportingSet

private const val MEASUREMENT_CONSUMER_ID = "mc_id"
private const val BATCH_GET_REPORTING_SETS_LIMIT = 23

@RunWith(JUnit4::class)
class SubmitBatchRequestsTest {

  private val internalReportingSetsMock: ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase =
    mockService {
      onBlocking { batchGetReportingSets(any()) }
        .thenAnswer {
          val request = it.arguments[0] as BatchGetReportingSetsRequest
          val internalReportingSetsMap =
            INTERNAL_PRIMITIVE_REPORTING_SETS.associateBy { internalReportingSet ->
              internalReportingSet.externalReportingSetId
            }
          batchGetReportingSetsResponse {
            reportingSets +=
              request.externalReportingSetIdsList.map { externalReportingSetId ->
                internalReportingSetsMap.getValue(externalReportingSetId)
              }
          }
        }
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalReportingSetsMock) }

  @Test
  fun `submitBatchRequests returns reporting sets when the number of requests is more than limits`() =
    runBlocking {
      val expectedNumberBatches =
        ceil(INTERNAL_PRIMITIVE_REPORTING_SETS.size / BATCH_GET_REPORTING_SETS_LIMIT.toFloat())
          .toInt()

      val items = INTERNAL_PRIMITIVE_REPORTING_SETS.map { it.externalReportingSetId }.asFlow()

      val parseResponse: (BatchGetReportingSetsResponse) -> List<InternalReportingSet> =
        { response ->
          response.reportingSetsList
        }

      val result =
        submitBatchRequests(
            items,
            BATCH_GET_REPORTING_SETS_LIMIT,
            ::batchGetReportingSets,
            parseResponse,
          )
          .toList()
          .flatten()

      val batchGetReportingSetsCaptor: KArgumentCaptor<BatchGetReportingSetsRequest> =
        argumentCaptor()
      verifyBlocking(internalReportingSetsMock, times(expectedNumberBatches)) {
        batchGetReportingSets(batchGetReportingSetsCaptor.capture())
      }

      assertThat(result).isEqualTo(INTERNAL_PRIMITIVE_REPORTING_SETS)
    }

  @Test
  fun `submitBatchRequests returns reporting sets when the number of requests is less than limits`() =
    runBlocking {
      val numberTargetReportingSet = BATCH_GET_REPORTING_SETS_LIMIT - 1
      val expectedReportingSets =
        INTERNAL_PRIMITIVE_REPORTING_SETS.subList(0, numberTargetReportingSet)
      val expectedNumberBatches = 1
      val items = expectedReportingSets.map { it.externalReportingSetId }.asFlow()

      val parseResponse: (BatchGetReportingSetsResponse) -> List<InternalReportingSet> =
        { response ->
          response.reportingSetsList
        }

      val result =
        submitBatchRequests(
            items,
            BATCH_GET_REPORTING_SETS_LIMIT,
            ::batchGetReportingSets,
            parseResponse,
          )
          .toList()
          .flatten()

      val batchGetReportingSetsCaptor: KArgumentCaptor<BatchGetReportingSetsRequest> =
        argumentCaptor()
      verifyBlocking(internalReportingSetsMock, times(expectedNumberBatches)) {
        batchGetReportingSets(batchGetReportingSetsCaptor.capture())
      }

      assertThat(result).isEqualTo(expectedReportingSets)
    }

  @Test
  fun `submitBatchRequests returns empty list when the number of requests is 0`() = runBlocking {
    val parseResponse: (BatchGetReportingSetsResponse) -> List<InternalReportingSet> = { response ->
      response.reportingSetsList
    }

    val result: List<InternalReportingSet> =
      submitBatchRequests(
          emptyFlow(),
          BATCH_GET_REPORTING_SETS_LIMIT,
          ::batchGetReportingSets,
          parseResponse,
        )
        .toList()
        .flatten()

    val batchGetReportingSetsCaptor: KArgumentCaptor<BatchGetReportingSetsRequest> =
      argumentCaptor()
    verifyBlocking(internalReportingSetsMock, never()) {
      batchGetReportingSets(batchGetReportingSetsCaptor.capture())
    }

    assertThat(result).isEqualTo(listOf<InternalReportingSet>())
  }

  private suspend fun batchGetReportingSets(
    externalReportingSetIds: List<String>
  ): BatchGetReportingSetsResponse {
    return try {
      internalReportingSetsMock.batchGetReportingSets(
        batchGetReportingSetsRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
          this.externalReportingSetIds += externalReportingSetIds
        }
      )
    } catch (e: StatusException) {
      throw when (e.status.code) {
          Status.Code.NOT_FOUND -> Status.NOT_FOUND.withDescription("Reporting Set not found.")
          else ->
            Status.UNKNOWN.withDescription(
              "Unable to retrieve the reporting sets used in the requesting metric."
            )
        }
        .withCause(e)
        .asRuntimeException()
    }
  }

  companion object {
    private val INTERNAL_PRIMITIVE_REPORTING_SETS: List<InternalReportingSet> =
      (0L..100L).map {
        internalReportingSet {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
          externalReportingSetId = it.toString()
        }
      }
  }
}
