// Copyright 2022 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.duchy.service.internal.testing

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.CreateComputationStatRequest
import org.wfanet.measurement.internal.duchy.CreateComputationStatResponse
import org.wfanet.measurement.internal.duchy.claimWorkRequest
import org.wfanet.measurement.internal.duchy.computationDetails
import org.wfanet.measurement.internal.duchy.computationStage
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig
import org.wfanet.measurement.internal.duchy.createComputationRequest
import org.wfanet.measurement.internal.duchy.createComputationStatRequest
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2Kt

@RunWith(JUnit4::class)
abstract class ComputationStatsServiceTest<T : ComputationStatsCoroutineImplBase> {
  protected data class Services<T>(
    val computationStatsService: T,
    val computationsService: ComputationsCoroutineImplBase,
  )
  private lateinit var computationStatsService: T
  private lateinit var computationsService: ComputationsCoroutineImplBase

  protected abstract fun newComputationStatsService(): T
  protected abstract fun newComputationsService(): ComputationsCoroutineImplBase

  @Before
  fun initService() {
    computationStatsService = newComputationStatsService()
    computationsService = newComputationsService()
  }

  companion object {
    private const val FAKE_ID = "1234"
    private const val GLOBAL_COMPUTATION_ID = FAKE_ID
    private val AGGREGATOR_COMPUTATION_DETAILS = computationDetails {
      liquidLegionsV2 =
        LiquidLegionsSketchAggregationV2Kt.computationDetails {
          role = LiquidLegionsV2SetupConfig.RoleInComputation.AGGREGATOR
        }
    }
    private val DEFAULT_CREATE_COMPUTATION_REQUEST = createComputationRequest {
      computationType = ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
      globalComputationId = GLOBAL_COMPUTATION_ID
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS
    }
  }

  @Test
  fun `createComputationStats throws INVALID_ARGUMENT when computation ID is missing`() =
    runBlocking {
      val createComputationStatRequest = CreateComputationStatRequest.getDefaultInstance()
      val exception =
        assertFailsWith<StatusRuntimeException> {
          computationStatsService.createComputationStat(createComputationStatRequest)
        }

      assertThat(exception.status.code).isEqualTo(Status.INVALID_ARGUMENT.code)
      assertThat(exception.message).contains("Missing computation ID")
    }

  @Test
  fun `createComputationStats throws INVALID_ARGUMENT when metric name is missing`() = runBlocking {
    val createComputationStatRequest = createComputationStatRequest { localComputationId = 1234L }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        computationStatsService.createComputationStat(createComputationStatRequest)
      }

    assertThat(exception.status.code).isEqualTo(Status.INVALID_ARGUMENT.code)
    assertThat(exception.message).contains("Missing Metric name")
  }

  @Test
  fun `createComputationStats throws IllegalStateException when computation stage is missing`() =
    runBlocking {
      val createComputationStatRequest = createComputationStatRequest {
        localComputationId = 1234L
        metricName = "metric_name"
      }
      val exception =
        assertFailsWith<IllegalStateException> {
          computationStatsService.createComputationStat(createComputationStatRequest)
        }

      assertThat(exception.message).contains("Stage not set")
    }

  @Test
  fun `createComputationStats returns empty response when succeeds`() = runBlocking {
    computationsService.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
    val claimWorkRequest = claimWorkRequest {
      computationType = ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
      owner = "owner"
    }
    val claimWorkResponse = computationsService.claimWork(claimWorkRequest)
    val computationToken = claimWorkResponse.token

    val createComputationStatRequest = createComputationStatRequest {
      localComputationId = computationToken.localComputationId
      metricName = "metric_name"
      metricValue = 1
      computationStage = computationToken.computationStage
      attempt = computationToken.attempt
    }
    val createComputationStatResponse =
      computationStatsService.createComputationStat(createComputationStatRequest)

    assertThat(createComputationStatResponse)
      .isEqualTo(CreateComputationStatResponse.getDefaultInstance())
  }
}
