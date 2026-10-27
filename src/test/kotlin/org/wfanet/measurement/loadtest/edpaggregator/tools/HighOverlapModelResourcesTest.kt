// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.edpaggregator.tools

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.LocalDate
import kotlin.test.assertFailsWith
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.CreatePopulationRequest
import org.wfanet.measurement.api.v2alpha.GetModelLineRequest
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.Population
import org.wfanet.measurement.api.v2alpha.PopulationsGrpcKt.PopulationsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.population
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule

@RunWith(JUnit4::class)
class HighOverlapModelResourcesTest {
  private var modelLineStatus: Status = Status.UNKNOWN

  private val populationsService =
    object : PopulationsCoroutineImplBase() {
      override suspend fun createPopulation(request: CreatePopulationRequest): Population {
        return population {
          name = POPULATION_NAME
          populationSpec = request.population.populationSpec
        }
      }
    }

  private val modelLinesService =
    object : ModelLinesCoroutineImplBase() {
      override suspend fun getModelLine(request: GetModelLineRequest): ModelLine {
        throw modelLineStatus.withDescription(LOOKUP_ERROR_DESCRIPTION).asRuntimeException()
      }
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(populationsService)
    addService(modelLinesService)
  }

  @Test
  fun `provision reports missing ModelLine when lookup returns NOT_FOUND`() {
    modelLineStatus = Status.NOT_FOUND

    val exception = assertFailsWith<IllegalStateException> { provision() }

    assertThat(exception.message).contains(MODEL_LINE_NAME)
    assertThat(exception.message).contains("not found")
    assertThat(exception.message).contains("ModelRepository")
    assertRpcCause(exception, Status.Code.NOT_FOUND)
  }

  @Test
  fun `provision preserves permission failure without missing ModelLine advice`() {
    modelLineStatus = Status.PERMISSION_DENIED

    val exception = assertFailsWith<IllegalStateException> { provision() }

    assertThat(exception.message).contains(MODEL_LINE_NAME)
    assertThat(exception.message).doesNotContain("not found")
    assertThat(exception.message).doesNotContain("ModelRepository")
    assertRpcCause(exception, Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `provision preserves unavailable failure without missing ModelLine advice`() {
    modelLineStatus = Status.UNAVAILABLE

    val exception = assertFailsWith<IllegalStateException> { provision() }

    assertThat(exception.message).contains(MODEL_LINE_NAME)
    assertThat(exception.message).doesNotContain("not found")
    assertThat(exception.message).doesNotContain("ModelRepository")
    assertRpcCause(exception, Status.Code.UNAVAILABLE)
  }

  private fun provision() {
    HighOverlapModelResources(
        populationSpec = populationSpec {},
        populationDataProvider = DATA_PROVIDER_NAME,
        modelLineName = MODEL_LINE_NAME,
        earliestEventDate = LocalDate.of(2026, 4, 1),
        pdpChannel = grpcTestServerRule.channel,
        mpChannel = grpcTestServerRule.channel,
      )
      .provision()
  }

  private fun assertRpcCause(exception: IllegalStateException, expectedCode: Status.Code) {
    assertThat(exception.cause).isInstanceOf(StatusRuntimeException::class.java)
    val status = Status.fromThrowable(checkNotNull(exception.cause))
    assertThat(status.code).isEqualTo(expectedCode)
    assertThat(status.description).isEqualTo(LOOKUP_ERROR_DESCRIPTION)
  }

  companion object {
    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
    private const val POPULATION_NAME = "$DATA_PROVIDER_NAME/populations/AAAAAAAAAHs"
    private const val MODEL_LINE_NAME =
      "modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelLines/AAAAAAAAAHs"
    private const val LOOKUP_ERROR_DESCRIPTION = "ModelLine lookup failed"
  }
}
