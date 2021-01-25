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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation

import com.google.cloud.ByteArray
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.SpannerException
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.testing.COMPUTATIONS_SCHEMA
import org.wfanet.measurement.gcloud.spanner.testing.UsingSpannerEmulator
import org.wfanet.measurement.gcloud.spanner.testing.assertQueryReturns

@RunWith(JUnit4::class)
class ComputationsSchemaTest : UsingSpannerEmulator(COMPUTATIONS_SCHEMA) {

  private val computationId: Long = 85740L

  @Test
  fun insertOne() = runBlocking {
    val dbClient = databaseClient
    dbClient.write(listOf(makeInsertMutation()))
    assertQueryReturns(
      dbClient,
      "SELECT ComputationId, ComputationStage FROM Computations",
      Struct.newBuilder()
        .set("ComputationId").to(computationId)
        .set("ComputationStage").to(1)
        .build()
    )
  }

  @Test
  fun insertChild() = runBlocking {
    val dbClient = databaseClient
    val mutation = makeInsertMutation()
    val childMutation = Mutation.newInsertOrUpdateBuilder("ComputationStages")
      .set("ComputationId").to(computationId)
      .set("ComputationStage").to(2)
      .set("NextAttempt").to(3)
      .set("CreationTime").to(Value.COMMIT_TIMESTAMP)
      .set("Details").to(ByteArray.copyFrom("123"))
      .set("DetailsJSON").to("123")
      .build()
    dbClient.write(listOf(mutation, childMutation))
    assertQueryReturns(
      dbClient,
      "SELECT ComputationId, ComputationStage, NextAttempt FROM ComputationStages",
      Struct.newBuilder()
        .set("ComputationId").to(computationId)
        .set("ComputationStage").to(2)
        .set("NextAttempt").to(3)
        .build()
    )
  }

  @Test
  fun globalIdIsUnique() = runBlocking<Unit> {
    val dbClient = databaseClient
    dbClient.write(makeInsertMutation())
    assertFailsWith<SpannerException> {
      dbClient.write(
        Mutation.newInsertBuilder("Computations")
          .set("ComputationId").to(computationId + 6)
          .set("ComputationStage").to(1)
          .set("GlobalComputationId").to(1)
          .set("ComputationDetails").to(ByteArray.copyFrom("123"))
          .set("ComputationDetailsJSON").to("123")
          .build()
      )
    }
  }

  private fun makeInsertMutation(): Mutation {
    return Mutation.newInsertBuilder("Computations")
      .set("ComputationId").to(computationId)
      .set("Protocol").to(1000)
      .set("ComputationStage").to(1)
      .set("GlobalComputationId").to(1)
      .set("ComputationDetails").to(ByteArray.copyFrom("123"))
      .set("ComputationDetailsJSON").to("123")
      .build()
  }
}
