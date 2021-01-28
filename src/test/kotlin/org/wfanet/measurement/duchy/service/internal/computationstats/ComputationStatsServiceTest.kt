// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.service.internal.computationstats

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.duchy.db.computation.testing.FakeComputationsDatabase
import org.wfanet.measurement.internal.duchy.ComputationStage
import org.wfanet.measurement.internal.duchy.CreateComputationStatRequest
import org.wfanet.measurement.internal.duchy.CreateComputationStatResponse

@RunWith(JUnit4::class)
class ComputationStatsServiceTest {
  private val fakeDatabase = FakeComputationsDatabase()
  private val service: ComputationStatsService

  init {
    service = ComputationStatsService(fakeDatabase)
  }

  @Test
  fun `createComputationStat throws INVALID_ARGUMENT when metric name not set`() = runBlocking {
    val request = CreateComputationStatRequest.newBuilder()
      .setLocalComputationId(1)
      .setComputationStage(ComputationStage.newBuilder().setLiquidLegionsSketchAggregationV1Value(1))
      .setAttempt(1)
      .setMetricValue(1234)
      .build()
    val e = assertFailsWith(StatusRuntimeException::class) {
      service.createComputationStat(request)
    }
    assertThat(e.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createComputationStat succeeds`() = runBlocking {
    val request = CreateComputationStatRequest.newBuilder()
      .setLocalComputationId(1)
      .setComputationStage(ComputationStage.newBuilder().setLiquidLegionsSketchAggregationV1Value(1))
      .setAttempt(1)
      .setMetricName("crypto_cpu_time_millis")
      .setMetricValue(1234)
      .build()
    val response = service.createComputationStat(request)
    assertThat(response).isEqualTo(CreateComputationStatResponse.getDefaultInstance())
  }
}
