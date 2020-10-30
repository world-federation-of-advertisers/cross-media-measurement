// Copyright 2020 The Measurement System Authors
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
import com.nhaarman.mockitokotlin2.mock
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.duchy.db.computationstat.ComputationStatDatabase
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.CreateComputationStatRequest
import org.wfanet.measurement.internal.duchy.CreateComputationStatResponse
import kotlin.test.assertFailsWith

@RunWith(JUnit4::class)
class ComputationStatsServiceTest {

  private val computationStatDbMock: ComputationStatDatabase = mock()
  private val service: ComputationStatsService

  init {
    service = ComputationStatsService(computationStatDbMock)
  }

  @Test
  fun `createComputationStat throws INVALID_ARGUMENT when metric name not set`() = runBlocking {
    val request = CreateComputationStatRequest.newBuilder()
      .setLocalComputationId(1)
      .setComputationStage(1)
      .setAttempt(1)
      .setGlobalComputationId("1")
      .setRole(ComputationDetails.RoleInComputation.PRIMARY)
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
      .setComputationStage(1)
      .setAttempt(1)
      .setMetricName("crypto_cpu_time_millis")
      .setGlobalComputationId("1")
      .setRole(ComputationDetails.RoleInComputation.PRIMARY)
      .setMetricValue(1234)
      .build()
    val response = service.createComputationStat(request)
    assertThat(response).isEqualTo(CreateComputationStatResponse.getDefaultInstance())
  }
}
