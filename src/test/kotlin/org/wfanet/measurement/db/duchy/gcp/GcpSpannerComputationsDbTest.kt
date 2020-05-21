package org.wfanet.measurement.db.duchy.gcp

import com.google.cloud.Timestamp
import com.google.cloud.spanner.SpannerException
import com.google.cloud.spanner.Struct
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.db.gcp.ComputationDetails
import org.wfanet.measurement.internal.db.gcp.ComputationStageDetails
import org.wfanet.measurement.common.Duchy
import org.wfanet.measurement.common.DuchyOrder
import org.wfanet.measurement.common.DuchyRole
import org.wfanet.measurement.db.duchy.AfterTransition
import org.wfanet.measurement.db.duchy.BlobRef
import org.wfanet.measurement.db.duchy.ComputationToken
import org.wfanet.measurement.db.duchy.ProtocolStateEnumHelper
import org.wfanet.measurement.db.gcp.GcpSpannerComputationsDb
import org.wfanet.measurement.db.gcp.LocalComputationIdGenerator
import org.wfanet.measurement.db.gcp.testing.UsingSpannerEmulator
import org.wfanet.measurement.db.gcp.testing.assertQueryReturns
import org.wfanet.measurement.db.gcp.testing.assertQueryReturnsNothing
import org.wfanet.measurement.db.gcp.toJson
import org.wfanet.measurement.db.gcp.toSpannerByteArray

/**
 * +--------------+
 * |              |
 * |              v
 * A -> B -> C -> E
 *      |         ^
 *      v         |
 *      D --------+
 */
enum class FakeProtocolStates {
  A, B, C, D, E;
}

object ProtocolHelper : ProtocolStateEnumHelper<FakeProtocolStates> {
  override val validInitialStates = setOf(FakeProtocolStates.A)
  override val validSuccessors = mapOf(
    FakeProtocolStates.A to setOf(FakeProtocolStates.B, FakeProtocolStates.E),
    FakeProtocolStates.B to setOf(FakeProtocolStates.C, FakeProtocolStates.D),
    FakeProtocolStates.C to setOf(FakeProtocolStates.E),
    FakeProtocolStates.D to setOf(FakeProtocolStates.E)
  )

  override fun enumToLong(value: FakeProtocolStates): Long {
    return value.ordinal.toLong()
  }

  override fun longToEnum(value: Long): FakeProtocolStates {
    return when (value) {
      0L -> FakeProtocolStates.A
      1L -> FakeProtocolStates.B
      2L -> FakeProtocolStates.C
      3L -> FakeProtocolStates.D
      4L -> FakeProtocolStates.E
      else -> error("Bad value")
    }
  }
}

@RunWith(JUnit4::class)
class GcpSpannerComputationsDbTest : UsingSpannerEmulator("/src/main/db/gcp/computations.sdl") {

  private val order = DuchyOrder(
    setOf(
      Duchy("BOHEMIA", 10L.toBigInteger()),
      Duchy("SALZBURG", 200L.toBigInteger()),
      Duchy("AUSTRIA", 303L.toBigInteger())
    )
  )

  private val database =
    GcpSpannerComputationsDb(
      spanner.spanner,
      spanner.databaseId,
      "AUSTRIA",
      order,
      localComputationIdGenerator = LocalIdIsGlobalIdPlusOne,
      stateEnumHelper = ProtocolHelper
    )

  object LocalIdIsGlobalIdPlusOne :
    LocalComputationIdGenerator {
    override fun localId(globalId: Long): Long {
      return globalId + 1
    }
  }

  @Test
  fun `insert two computations`() {
    val fiveMinutesAgo = Timestamp.ofTimeSecondsAndNanos(System.currentTimeMillis() / 1000 - 300, 0)
    val resultId123 = database.insertComputation(123L, FakeProtocolStates.A)
    val resultId220 = database.insertComputation(220L, FakeProtocolStates.A)
    assertEquals(
      ComputationToken(
        localId = 124, nextWorker = "BOHEMIA", role = DuchyRole.SECONDARY,
        owner = null, attempt = 1, state = FakeProtocolStates.A,
        globalId = 123L
      ),
      resultId123
    )

    assertEquals(
      ComputationToken(
        localId = 221, nextWorker = "BOHEMIA", role = DuchyRole.PRIMARY,
        owner = null, attempt = 1, state = FakeProtocolStates.A,
        globalId = 220
      ),
      resultId220
    )

    val expectedDetails123 = ComputationDetails.newBuilder().apply {
      role = ComputationDetails.RoleInComputation.SECONDARY
      incomingNodeId = "SALZBURG"
      outgoingNodeId = "BOHEMIA"
      blobsStoragePrefix = "knight-computation-stage-storage/${resultId123.localId}"
    }.build()

    val expectedDetails220 = ComputationDetails.newBuilder().apply {
      role = ComputationDetails.RoleInComputation.PRIMARY
      incomingNodeId = "SALZBURG"
      outgoingNodeId = "BOHEMIA"
      blobsStoragePrefix = "knight-computation-stage-storage/${resultId220.localId}"
    }.build()
    assertQueryReturns(
      spanner.client,
      """
      SELECT ComputationId, ComputationStage, GlobalComputationId, LockOwner, LockExpirationTime,
             ComputationDetails, ComputationDetailsJSON
      FROM Computations
      ORDER BY ComputationId
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId123.localId)
        .set("ComputationStage").to(resultId123.state.ordinal.toLong())
        .set("GlobalComputationId").to(resultId123.globalId)
        .set("LockOwner").to(resultId123.owner)
        .set("LockExpirationTime").to(null as Timestamp?)
        .set("ComputationDetails").to(expectedDetails123.toSpannerByteArray())
        .set("ComputationDetailsJSON").to(expectedDetails123.toJson())
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId220.localId)
        .set("ComputationStage").to(resultId220.state.ordinal.toLong())
        .set("GlobalComputationId").to(resultId220.globalId)
        .set("LockOwner").to(resultId220.owner)
        .set("LockExpirationTime").to(null as Timestamp?)
        .set("ComputationDetails").to(expectedDetails220.toSpannerByteArray())
        .set("ComputationDetailsJSON").to(expectedDetails220.toJson())
        .build()
    )

    // Spanner doesn't have an easy way to inject a creation timestamp. Here we check that
    // the update time was set to a recent enough timestamp.
    assertQueryReturnsNothing(
      spanner.client,
      """
      SELECT ComputationId, UpdateTime FROM Computations
      WHERE UpdateTime is NULL OR UpdateTime < '$fiveMinutesAgo'
      """.trimIndent()
    )

    assertQueryReturns(
      spanner.client,
      """
      SELECT ComputationId, ComputationStage, NextAttempt, EndTime, Details, DetailsJSON
      FROM ComputationStages
      ORDER BY ComputationId
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId123.localId)
        .set("ComputationStage").to(resultId123.state.ordinal.toLong())
        .set("NextAttempt").to(resultId123.attempt + 1)
        .set("EndTime").to(null as Timestamp?)
        .set("Details").to(ComputationStageDetails.getDefaultInstance().toSpannerByteArray())
        .set("DetailsJSON").to(ComputationStageDetails.getDefaultInstance().toJson())
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId220.localId)
        .set("ComputationStage").to(resultId220.state.ordinal.toLong())
        .set("NextAttempt").to(resultId220.attempt + 1)
        .set("EndTime").to(null as Timestamp?)
        .set("Details").to(ComputationStageDetails.getDefaultInstance().toSpannerByteArray())
        .set("DetailsJSON").to(ComputationStageDetails.getDefaultInstance().toJson())
        .build()
    )

    // Spanner doesn't have an easy way to inject a creation timestamp. Here we check that
    // the creation time was set to a recent enough timestamp.
    assertQueryReturnsNothing(
      spanner.client,
      """
      SELECT ComputationId, CreationTime FROM ComputationStages
      WHERE CreationTime is NULL OR CreationTime < '$fiveMinutesAgo'
      """.trimIndent()
    )

    assertQueryReturnsNothing(
      spanner.client, "SELECT ComputationId FROM ComputationBlobReferences"
    )

    assertQueryReturns(
      spanner.client,
      """
      SELECT ComputationId, ComputationStage, Attempt, EndTime
      FROM ComputationStageAttempts
      ORDER BY ComputationId
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId123.localId)
        .set("ComputationStage").to(resultId123.state.ordinal.toLong())
        .set("Attempt").to(resultId123.attempt)
        .set("EndTime").to(null as Timestamp?)
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId220.localId)
        .set("ComputationStage").to(resultId220.state.ordinal.toLong())
        .set("Attempt").to(resultId220.attempt)
        .set("EndTime").to(null as Timestamp?)
        .build()
    )

    // Spanner doesn't have an easy way to inject a creation timestamp. Here we check that
    // the begin time was set to a recent enough timestamp.
    assertQueryReturnsNothing(
      spanner.client,
      """
      SELECT ComputationId, BeginTime FROM ComputationStageAttempts
      WHERE BeginTime is NULL OR BeginTime < '$fiveMinutesAgo'
      """.trimIndent()
    )
  }

  @Test
  fun `insert computation fails`() {
    database.insertComputation(123L, FakeProtocolStates.A)
    // This one fails because the same local id is used.
    assertFailsWith(SpannerException::class, "ALREADY_EXISTS") {
      database.insertComputation(123L, FakeProtocolStates.A)
    }
  }

  @Test
  fun `insert computation with bad initial state fails`() {
    assertFailsWith(IllegalArgumentException::class, "Invalid initial state") {
      database.insertComputation(123L, FakeProtocolStates.B)
    }
    assertFailsWith(IllegalArgumentException::class, "Invalid initial state") {
      database.insertComputation(123L, FakeProtocolStates.C)
    }
    assertFailsWith(IllegalArgumentException::class, "Invalid initial state") {
      database.insertComputation(123L, FakeProtocolStates.D)
    }
    assertFailsWith(IllegalArgumentException::class, "Invalid initial state") {
      database.insertComputation(123L, FakeProtocolStates.E)
    }
  }

  @Test
  fun `unimplemented interface functions`() {
    val token = ComputationToken(
      localId = 1, nextWorker = "", role = DuchyRole.PRIMARY,
      owner = null, attempt = 1, state = FakeProtocolStates.A,
      globalId = 0
    )
    assertFailsWith(NotImplementedError::class) { database.claimTask("owner") }
    assertFailsWith(NotImplementedError::class) { database.enqueue(token) }
    assertFailsWith(NotImplementedError::class) { database.renewTask(token) }
    assertFailsWith(NotImplementedError::class) { database.getToken(123) }
    assertFailsWith(NotImplementedError::class) { database.readBlobReferenceNames(token) }
    assertFailsWith(NotImplementedError::class) {
      database.updateComputationState(
        token,
        FakeProtocolStates.B,
        listOf(),
        listOf(),
        AfterTransition.DO_NOT_ADD_TO_QUEUE
      )
    }
    assertFailsWith(NotImplementedError::class) {
      database.writeOutputBlobReference(
        token,
        BlobRef("", "")
      )
    }
  }
}
