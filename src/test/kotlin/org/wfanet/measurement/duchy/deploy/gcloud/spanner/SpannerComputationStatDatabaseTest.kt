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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner

import com.google.cloud.spanner.Struct
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.gcloud.spanner.testing.UsingSpannerEmulator
import org.wfanet.measurement.gcloud.spanner.testing.assertQueryReturns

@RunWith(JUnit4::class)
class SpannerComputationStatDatabaseTest :
  UsingSpannerEmulator("/src/main/db/gcp/computation_stats.sdl") {

  private val computationStatDb by lazy { SpannerComputationStatDatabase(databaseClient) }

  @Test
  fun `insert computation stat succeeds`() = runBlocking<Unit> {
    computationStatDb.insertComputationStat(
      localId = 4315,
      stage = 1,
      attempt = 1,
      metricName = "crypto_cpu_time_millis",
      globalId = "55",
      role = "PRIMARY",
      isSuccessfulAttempt = true,
      value = 3125
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, Attempt, MetricName, GlobalComputationId,
             Role, IsSuccessfulAttempt, Value
      FROM ComputationStats
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(4315)
        .set("ComputationStage").to(1)
        .set("Attempt").to(1)
        .set("MetricName").to("crypto_cpu_time_millis")
        .set("GlobalComputationId").to("55")
        .set("Role").to("PRIMARY")
        .set("IsSuccessfulAttempt").to(true)
        .set("Value").to(3125)
        .build()
    )
  }
}
