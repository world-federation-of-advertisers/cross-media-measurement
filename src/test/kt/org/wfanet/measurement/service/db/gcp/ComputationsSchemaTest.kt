package org.wfanet.measurement.service.db.gcp

import com.google.cloud.ByteArray
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Value
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.service.db.gcp.testing.UsingSpannerEmulator
import kotlin.test.assertEquals

@RunWith(JUnit4::class)
class ComputationsSchemaTest : UsingSpannerEmulator("/src/main/db/gcp/computations.sdl") {

  private val computationId : Long = 85740L

  @Test
  fun insertOne() {
    val dbClient = spanner.client
    dbClient.write(listOf(makeInsertMutation()))
    val resultSet = dbClient.singleUse().executeQuery(
      Statement.of("SELECT ComputationId, ComputationStage FROM Computations;"));
    println("Results:")
    while (resultSet.next()) {
      assertEquals(computationId, resultSet.getLong(0), "Wrong ComputationId")
      assertEquals(1, resultSet.getLong(1), "Wrong ComputationStage")
    }
  }

  @Test
  fun insertChild() {
    val dbClient = spanner.client
    val mutation = makeInsertMutation()
    val childMutation = Mutation.newInsertOrUpdateBuilder("ComputationStages")
      .set("ComputationId").to(computationId)
      .set("ComputationStage").to(2)
      .set("NextAttempt").to(3)
      .set("CreationTime").to(Value.COMMIT_TIMESTAMP)
      .set("Details").to(ByteArray.copyFrom("123"))
      .set("DetailsJSON").to(ByteArray.copyFrom("123"))
      .build()
    dbClient.write(listOf(mutation, childMutation))
    val resultSet = dbClient.singleUse().executeQuery(
      Statement.of("SELECT ComputationId, ComputationStage, NextAttempt FROM ComputationStages;"));
    println("Results:")
    while (resultSet.next()) {
      assertEquals(computationId, resultSet.getLong(0), "Wrong ComputationId")
      assertEquals(2, resultSet.getLong(1), "Wrong ComputationStage")
      assertEquals(3, resultSet.getLong(2), "Wrong NextAttempt")
    }
  }

  private fun makeInsertMutation(): Mutation {
    return Mutation.newInsertBuilder("Computations")
      .set("ComputationId").to(computationId)
      .set("ComputationStage").to(1)
      .set("GlobalComputationId").to(1)
      .set("ComputationDetails").to(ByteArray.copyFrom("123"))
      .set("ComputationDetailsJSON").to(ByteArray.copyFrom("123"))
      .build()
  }
}
