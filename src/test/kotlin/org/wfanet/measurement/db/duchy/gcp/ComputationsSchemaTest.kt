package org.wfanet.measurement.db.duchy.gcp

import com.google.cloud.ByteArray
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.SpannerException
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.db.gcp.testing.UsingSpannerEmulator
import org.wfanet.measurement.db.gcp.testing.assertQueryReturns

@RunWith(JUnit4::class)
class ComputationsSchemaTest : UsingSpannerEmulator("/src/main/db/gcp/computations.sdl") {

  private val computationId: Long = 85740L

  @Test
  fun insertOne() {
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
  fun insertChild() {
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
  fun globalIdIsUnique() {
    val dbClient = databaseClient
    dbClient.write(listOf(makeInsertMutation()))
    assertFailsWith<SpannerException> {
      dbClient.write(
        listOf(
          Mutation.newInsertBuilder("Computations")
            .set("ComputationId").to(computationId + 6)
            .set("ComputationStage").to(1)
            .set("GlobalComputationId").to(1)
            .set("ComputationDetails").to(ByteArray.copyFrom("123"))
            .set("ComputationDetailsJSON").to("123")
            .build()
        )
      )
    }
  }

  private fun makeInsertMutation(): Mutation {
    return Mutation.newInsertBuilder("Computations")
      .set("ComputationId").to(computationId)
      .set("ComputationStage").to(1)
      .set("GlobalComputationId").to(1)
      .set("ComputationDetails").to(ByteArray.copyFrom("123"))
      .set("ComputationDetailsJSON").to("123")
      .build()
  }
}
