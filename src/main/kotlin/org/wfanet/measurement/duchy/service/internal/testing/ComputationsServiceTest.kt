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
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.duchy.toProtocolStage
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineImplBase
import org.wfanet.measurement.internal.duchy.*
import org.wfanet.measurement.internal.duchy.config.LiquidLegionsV2SetupConfig
import org.wfanet.measurement.internal.duchy.protocol.LiquidLegionsSketchAggregationV2


@RunWith(JUnit4::class)
abstract class ComputationsServiceTest<T : ComputationsCoroutineImplBase> {
  /** Instance of the service under test. */
  private lateinit var service: T

  /** Constructs the service being tested. */
  protected abstract fun newService(): T

  @Before
  fun initService() {
    service = newService()
  }

  private val GLOBAL_COMPUTATION_ID = "1234"
  private val AGGREGATOR_COMPUTATION_DETAILS =
    ComputationDetails.newBuilder()
      .apply { liquidLegionsV2Builder.apply { role = LiquidLegionsV2SetupConfig.RoleInComputation.AGGREGATOR } }
      .build()
  private val DEFAULT_CREATE_COMPUTATION_REQUEST = CreateComputationRequest.newBuilder()
    .apply {
      computationType = ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2
      globalComputationId = GLOBAL_COMPUTATION_ID
      computationStage { LiquidLegionsSketchAggregationV2.Stage.EXECUTION_PHASE_ONE.toProtocolStage() }
      computationDetails = AGGREGATOR_COMPUTATION_DETAILS
    }
    .build()
  private val DEFAULT_CREATE_COMPUTATION_RESP_TOKEN = computationToken {
    localComputationId = 1234
    globalComputationId = GLOBAL_COMPUTATION_ID
    computationStage = computationStage {
      liquidLegionsSketchAggregationV2 =
        LiquidLegionsSketchAggregationV2.Stage.INITIALIZATION_PHASE
    }
    computationDetails = AGGREGATOR_COMPUTATION_DETAILS
    blobs.add(computationStageBlobMetadata{dependencyType = ComputationBlobDependency.OUTPUT})
  }

  @Test
  fun `createComputation creates a new computation`() = runBlocking {
    val createComputationResponse = service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
    assertThat(createComputationResponse)
      .isEqualTo(createComputationResponse { token = DEFAULT_CREATE_COMPUTATION_RESP_TOKEN })

    val getComputationTokenRequest = getComputationTokenRequest {
      globalComputationId = GLOBAL_COMPUTATION_ID
    }
    val getComputationTokenResponse = service.getComputationToken(getComputationTokenRequest)
    assertThat(getComputationTokenResponse)
      .isEqualTo(getComputationTokenResponse{ token = DEFAULT_CREATE_COMPUTATION_RESP_TOKEN })
  }

  @Test
  fun `createComputation throws ALREADY_EXISTS when called with existing id`() = runBlocking {
    assertThat(service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST))
      .isEqualTo(createComputationResponse { token = DEFAULT_CREATE_COMPUTATION_RESP_TOKEN })

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createComputation(DEFAULT_CREATE_COMPUTATION_REQUEST)
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }
}
