package org.wfanet.measurement.db.duchy.gcp

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.SpannerException
import com.google.cloud.spanner.Struct
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
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
import org.wfanet.measurement.db.gcp.toGcpTimestamp
import org.wfanet.measurement.db.gcp.toJson
import org.wfanet.measurement.db.gcp.toSpannerByteArray
import org.wfanet.measurement.internal.db.gcp.ComputationDetails
import org.wfanet.measurement.internal.db.gcp.ComputationStageDetails

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

  companion object {
    val COMPUTATION_DEATILS: ComputationDetails = ComputationDetails.newBuilder().apply {
      incomingNodeId = "AUSTRIA"
      outgoingNodeId = "BOHEMIA"
      blobsStoragePrefix = "blobs"
      role = ComputationDetails.RoleInComputation.PRIMARY
    }.build()

    val STAGE_DETAILS: ComputationStageDetails = ComputationStageDetails.getDefaultInstance()
    val TEST_INSTANT: Instant = Instant.ofEpochMilli(123456789L)
  }

  private val database =
    GcpSpannerComputationsDb(
      spanner.spanner,
      spanner.databaseId,
      "AUSTRIA",
      DuchyOrder(
        setOf(
          Duchy("BOHEMIA", 10L.toBigInteger()),
          Duchy("SALZBURG", 200L.toBigInteger()),
          Duchy("AUSTRIA", 303L.toBigInteger())
        )
      ),
      clock = Clock.fixed(TEST_INSTANT, ZoneId.systemDefault()),
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
    val resultId123 = database.insertComputation(123L, FakeProtocolStates.A)
    val resultId220 = database.insertComputation(220L, FakeProtocolStates.A)
    assertEquals(
      ComputationToken(
        localId = 124, nextWorker = "BOHEMIA", role = DuchyRole.SECONDARY,
        owner = null, attempt = 1, state = FakeProtocolStates.A,
        globalId = 123L, lastUpdateTime = TEST_INSTANT.toEpochMilli()
      ),
      resultId123
    )

    assertEquals(
      ComputationToken(
        localId = 221, nextWorker = "BOHEMIA", role = DuchyRole.PRIMARY,
        owner = null, attempt = 1, state = FakeProtocolStates.A,
        globalId = 220, lastUpdateTime = TEST_INSTANT.toEpochMilli()
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
      SELECT ComputationId, ComputationStage, UpdateTime, GlobalComputationId, LockOwner, 
             LockExpirationTime, ComputationDetails, ComputationDetailsJSON
      FROM Computations
      ORDER BY ComputationId
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId123.localId)
        .set("ComputationStage").to(resultId123.state.ordinal.toLong())
        .set("UpdateTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("GlobalComputationId").to(resultId123.globalId)
        .set("LockOwner").to(resultId123.owner)
        .set("LockExpirationTime").to(null as Timestamp?)
        .set("ComputationDetails").to(expectedDetails123.toSpannerByteArray())
        .set("ComputationDetailsJSON").to(expectedDetails123.toJson())
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId220.localId)
        .set("ComputationStage").to(resultId220.state.ordinal.toLong())
        .set("UpdateTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("GlobalComputationId").to(resultId220.globalId)
        .set("LockOwner").to(resultId220.owner)
        .set("LockExpirationTime").to(null as Timestamp?)
        .set("ComputationDetails").to(expectedDetails220.toSpannerByteArray())
        .set("ComputationDetailsJSON").to(expectedDetails220.toJson())
        .build()
    )

    assertQueryReturns(
      spanner.client,
      """
      SELECT ComputationId, ComputationStage, CreationTime, NextAttempt,
             EndTime, Details, DetailsJSON
      FROM ComputationStages
      ORDER BY ComputationId
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId123.localId)
        .set("ComputationStage").to(resultId123.state.ordinal.toLong())
        .set("CreationTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("NextAttempt").to(resultId123.attempt + 1)
        .set("EndTime").to(null as Timestamp?)
        .set("Details").to(ComputationStageDetails.getDefaultInstance().toSpannerByteArray())
        .set("DetailsJSON").to(ComputationStageDetails.getDefaultInstance().toJson())
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId220.localId)
        .set("ComputationStage").to(resultId220.state.ordinal.toLong())
        .set("CreationTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("NextAttempt").to(resultId220.attempt + 1)
        .set("EndTime").to(null as Timestamp?)
        .set("Details").to(ComputationStageDetails.getDefaultInstance().toSpannerByteArray())
        .set("DetailsJSON").to(ComputationStageDetails.getDefaultInstance().toJson())
        .build()
    )

    assertQueryReturnsNothing(
      spanner.client, "SELECT ComputationId FROM ComputationBlobReferences"
    )

    assertQueryReturns(
      spanner.client,
      """
      SELECT ComputationId, ComputationStage, Attempt, BeginTime, EndTime
      FROM ComputationStageAttempts
      ORDER BY ComputationId
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId123.localId)
        .set("ComputationStage").to(resultId123.state.ordinal.toLong())
        .set("Attempt").to(resultId123.attempt)
        .set("BeginTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId220.localId)
        .set("ComputationStage").to(resultId220.state.ordinal.toLong())
        .set("Attempt").to(resultId220.attempt)
        .set("BeginTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .build()
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
  fun `getToken for non-existing computation returns null`() {
    assertNull(database.getToken(123))
  }

  @Test
  fun getToken() {
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val lockExpires = lastUpdated.plusSeconds(1000)
    val failedPreviousAttempt = Mutation.newInsertBuilder("Computations")
      .set("ComputationId").to(411)
      .set("ComputationStage").to(4)
      .set("UpdateTime").to(lastUpdated.minusSeconds(3000).toGcpTimestamp())
      .set("GlobalComputationId").to(21231)
      .set("LockOwner").to(null as String?)
      .set("LockExpirationTime").to(null as Timestamp?)
      .set("ComputationDetails").to(COMPUTATION_DEATILS.toSpannerByteArray())
      .set("ComputationDetailsJSON").to(COMPUTATION_DEATILS.toJson())
      .build()
    val computation = Mutation.newInsertBuilder("Computations")
      .set("ComputationId").to(100)
      .set("ComputationStage").to(3)
      .set("UpdateTime").to(lastUpdated.toGcpTimestamp())
      .set("GlobalComputationId").to(21231)
      .set("LockOwner").to("Fred")
      .set("LockExpirationTime").to(lockExpires.toGcpTimestamp())
      .set("ComputationDetails").to(COMPUTATION_DEATILS.toSpannerByteArray())
      .set("ComputationDetailsJSON").to(COMPUTATION_DEATILS.toJson())
      .build()
    val computationStageB = Mutation.newInsertBuilder("ComputationStages")
      .set("ComputationId").to(100)
      .set("ComputationStage").to(2)
      .set("NextAttempt").to(45)
      .set("CreationTime").to((lastUpdated.minusSeconds(2)).toGcpTimestamp())
      .set("EndTime").to((lastUpdated.minusMillis(200)).toGcpTimestamp())
      .set("Details").to(STAGE_DETAILS.toSpannerByteArray())
      .set("DetailsJSON").to(STAGE_DETAILS.toJson())
      .build()
    val computationStageD = Mutation.newInsertBuilder("ComputationStages")
      .set("ComputationId").to(100)
      .set("ComputationStage").to(3)
      .set("NextAttempt").to(2)
      .set("CreationTime").to(lastUpdated.toGcpTimestamp())
      .set("Details").to(STAGE_DETAILS.toSpannerByteArray())
      .set("DetailsJSON").to(STAGE_DETAILS.toJson())
      .build()
    spanner.client.write(
      listOf(
        failedPreviousAttempt,
        computation,
        computationStageB,
        computationStageD
      )
    )
    assertEquals(
      ComputationToken(
        globalId = 21231,
        localId = 100,
        state = FakeProtocolStates.D,
        owner = "Fred",
        role = DuchyRole.PRIMARY,
        nextWorker = COMPUTATION_DEATILS.outgoingNodeId,
        attempt = 1,
        lastUpdateTime = lastUpdated.toEpochMilli()
      ),
      database.getToken(21231)
    )
  }

  @Test
  fun `getToken after insert matches returned token`() {
    assertEquals(
      database.insertComputation(505, FakeProtocolStates.A),
      database.getToken(505),
      "Expected token returned by inserting new computation to match one retrieved with getToken()"
    )
  }

  @Test
  fun enqueue() {
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val lockExpires = Instant.now().plusSeconds(300)
    val token = ComputationToken(
      localId = 1, globalId = 0, state = FakeProtocolStates.C,
      owner = "PeterSpacemen", nextWorker = COMPUTATION_DEATILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 1, lastUpdateTime = lastUpdated.toEpochMilli()
    )

    val computation = Mutation.newInsertBuilder("Computations")
      .set("ComputationId").to(token.localId)
      .set("ComputationStage").to(ProtocolHelper.enumToLong(token.state))
      .set("UpdateTime").to(lastUpdated.toGcpTimestamp())
      .set("GlobalComputationId").to(token.globalId)
      .set("LockOwner").to(token.owner)
      .set("LockExpirationTime").to(lockExpires.toGcpTimestamp())
      .set("ComputationDetails").to(COMPUTATION_DEATILS.toSpannerByteArray())
      .set("ComputationDetailsJSON").to(COMPUTATION_DEATILS.toJson())
      .build()
    val differentComputation = Mutation.newInsertBuilder("Computations")
      .set("ComputationId").to(456789)
      .set("ComputationStage").to(1)
      .set("UpdateTime").to(lastUpdated.toGcpTimestamp())
      .set("GlobalComputationId").to(10111213)
      .set("LockOwner").to(token.owner)
      .set("LockExpirationTime").to(lockExpires.toGcpTimestamp())
      .set("ComputationDetails").to(COMPUTATION_DEATILS.toSpannerByteArray())
      .set("ComputationDetailsJSON").to(COMPUTATION_DEATILS.toJson())
      .build()

    spanner.client.write(listOf(computation, differentComputation))
    database.enqueue(token)

    assertQueryReturns(
      spanner.client,
      """
        SELECT ComputationId, ComputationStage, GlobalComputationId, LockOwner, LockExpirationTime,
               ComputationDetails, ComputationDetailsJSON
        FROM Computations
        ORDER BY ComputationId
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolHelper.enumToLong(token.state))
        .set("GlobalComputationId").to(token.globalId)
        .set("LockOwner").to(null as String?)
        .set("LockExpirationTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("ComputationDetails").to(COMPUTATION_DEATILS.toSpannerByteArray())
        .set("ComputationDetailsJSON").to(COMPUTATION_DEATILS.toJson())
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(456789)
        .set("ComputationStage").to(1)
        .set("GlobalComputationId").to(10111213)
        .set("LockOwner").to(token.owner)
        .set("LockExpirationTime").to(lockExpires.toGcpTimestamp())
        .set("ComputationDetails").to(COMPUTATION_DEATILS.toSpannerByteArray())
        .set("ComputationDetailsJSON").to(COMPUTATION_DEATILS.toJson())
        .build()
    )
  }

  @Test
  fun `enqueue deteleted computation fails`() {
    val token = ComputationToken(
      localId = 1, globalId = 0, state = FakeProtocolStates.C,
      owner = "PeterSpacemen", nextWorker = COMPUTATION_DEATILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 1, lastUpdateTime = 0
    )
    assertFailsWith<SpannerException> { database.enqueue(token) }
  }

  @Test
  fun `enqueue with old token fails`() {
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val lockExpires = lastUpdated.plusSeconds(1)
    val token = ComputationToken(
      localId = 1, globalId = 0, state = FakeProtocolStates.C,
      owner = "PeterSpacemen", nextWorker = COMPUTATION_DEATILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 1,
      lastUpdateTime = lastUpdated.minusSeconds(200).toEpochMilli()
    )

    val computation = Mutation.newInsertBuilder("Computations")
      .set("ComputationId").to(token.localId)
      .set("ComputationStage").to(ProtocolHelper.enumToLong(token.state))
      .set("UpdateTime").to(lastUpdated.toGcpTimestamp())
      .set("GlobalComputationId").to(token.globalId)
      .set("LockOwner").to(token.owner)
      .set("LockExpirationTime").to(lockExpires.toGcpTimestamp())
      .set("ComputationDetails").to(COMPUTATION_DEATILS.toSpannerByteArray())
      .set("ComputationDetailsJSON").to(COMPUTATION_DEATILS.toJson())
      .build()
    spanner.client.write(listOf(computation))
    assertFailsWith<SpannerException> { database.enqueue(token) }
  }

  @Test
  fun claimTask() {
    val fiveMinutesAgo = TEST_INSTANT.minusSeconds(300).toGcpTimestamp()
    val sixMinutesAgo = TEST_INSTANT.minusSeconds(360).toGcpTimestamp()
    val enqueuedFiveMinutesAgo = Mutation.newInsertBuilder("Computations")
      .set("ComputationId").to(555)
      .set("ComputationStage").to(0)
      .set("UpdateTime").to(TEST_INSTANT.minusSeconds(600).toGcpTimestamp())
      .set("GlobalComputationId").to(55)
      .set("LockOwner").to(null as String?)
      .set("LockExpirationTime").to(fiveMinutesAgo)
      .set("ComputationDetails").to(COMPUTATION_DEATILS.toSpannerByteArray())
      .set("ComputationDetailsJSON").to(COMPUTATION_DEATILS.toJson())
      .build()
    val enqueuedFiveMinutesAgoStage = Mutation.newInsertBuilder("ComputationStages")
      .set("ComputationId").to(555)
      .set("ComputationStage").to(0)
      .set("NextAttempt").to(2)
      .set("CreationTime").to(Instant.ofEpochMilli(3456789L).toGcpTimestamp())
      .set("Details").to(STAGE_DETAILS.toSpannerByteArray())
      .set("DetailsJSON").to(STAGE_DETAILS.toJson())
      .build()

    val enqueuedSixMinutesAgo = Mutation.newInsertBuilder("Computations")
      .set("ComputationId").to(66)
      .set("ComputationStage").to(0)
      .set("UpdateTime").to(TEST_INSTANT.minusSeconds(540).toGcpTimestamp())
      .set("GlobalComputationId").to(6)
      .set("LockOwner").to(null as String?)
      .set("LockExpirationTime").to(sixMinutesAgo)
      .set("ComputationDetails").to(COMPUTATION_DEATILS.toSpannerByteArray())
      .set("ComputationDetailsJSON").to(COMPUTATION_DEATILS.toJson())
      .build()
    val enqueuedSixMinutesAgoStage = Mutation.newInsertBuilder("ComputationStages")
      .set("ComputationId").to(66)
      .set("ComputationStage").to(0)
      .set("NextAttempt").to(2)
      .set("CreationTime").to(Instant.ofEpochMilli(3456789L).toGcpTimestamp())
      .set("Details").to(STAGE_DETAILS.toSpannerByteArray())
      .set("DetailsJSON").to(STAGE_DETAILS.toJson())
      .build()
    spanner.client.write(
      listOf(
        enqueuedFiveMinutesAgo,
        enqueuedFiveMinutesAgoStage,
        enqueuedSixMinutesAgo,
        enqueuedSixMinutesAgoStage))
    assertEquals(
      ComputationToken(
        localId = 66, globalId = 6, state = FakeProtocolStates.A,
        owner = "the-owner-of-the-lock", nextWorker = COMPUTATION_DEATILS.outgoingNodeId,
        role = DuchyRole.PRIMARY, attempt = 1, lastUpdateTime = TEST_INSTANT.toEpochMilli()),
      database.claimTask("the-owner-of-the-lock"))
    assertEquals(
      ComputationToken(
        localId = 555, globalId = 55, state = FakeProtocolStates.A,
        owner = "the-owner-of-the-lock", nextWorker = COMPUTATION_DEATILS.outgoingNodeId,
        role = DuchyRole.PRIMARY, attempt = 1, lastUpdateTime = TEST_INSTANT.toEpochMilli()),
      database.claimTask("the-owner-of-the-lock"))
    // No tasks to claim anymore
    assertNull(database.claimTask("the-owner-of-the-lock"))
  }

  @Test
  fun `claim locked tasks`() {
    val fiveMinutesAgo = TEST_INSTANT.minusSeconds(300).toGcpTimestamp()
    val fiveMinutesFromNow = TEST_INSTANT.plusSeconds(300).toGcpTimestamp()
    val claimedButExpired = Mutation.newInsertBuilder("Computations")
      .set("ComputationId").to(111)
      .set("ComputationStage").to(0)
      .set("UpdateTime").to(TEST_INSTANT.minusSeconds(600).toGcpTimestamp())
      .set("GlobalComputationId").to(11)
      .set("LockOwner").to("owner-of-the-lock")
      .set("LockExpirationTime").to(fiveMinutesAgo)
      .set("ComputationDetails").to(COMPUTATION_DEATILS.toSpannerByteArray())
      .set("ComputationDetailsJSON").to(COMPUTATION_DEATILS.toJson())
      .build()
    val claimedButExpiredStage = Mutation.newInsertBuilder("ComputationStages")
      .set("ComputationId").to(111)
      .set("ComputationStage").to(0)
      .set("NextAttempt").to(2)
      .set("CreationTime").to(3456789L.toGcpTimestamp())
      .set("Details").to(STAGE_DETAILS.toSpannerByteArray())
      .set("DetailsJSON").to(STAGE_DETAILS.toJson())
      .build()

    val claimed = Mutation.newInsertBuilder("Computations")
      .set("ComputationId").to(333)
      .set("ComputationStage").to(0)
      .set("UpdateTime").to(TEST_INSTANT.minusSeconds(540).toGcpTimestamp())
      .set("GlobalComputationId").to(33)
      .set("LockOwner").to("owner-of-the-lock")
      .set("LockExpirationTime").to(fiveMinutesFromNow)
      .set("ComputationDetails").to(COMPUTATION_DEATILS.toSpannerByteArray())
      .set("ComputationDetailsJSON").to(COMPUTATION_DEATILS.toJson())
      .build()
    val claimedStage = Mutation.newInsertBuilder("ComputationStages")
      .set("ComputationId").to(333)
      .set("ComputationStage").to(0)
      .set("NextAttempt").to(2)
      .set("CreationTime").to(3456789L.toGcpTimestamp())
      .set("Details").to(STAGE_DETAILS.toSpannerByteArray())
      .set("DetailsJSON").to(STAGE_DETAILS.toJson())
      .build()

    spanner.client.write(listOf(claimed, claimedStage, claimedButExpired, claimedButExpiredStage))
    // Claim a task that is owned but the lock expired.
    assertEquals(
      ComputationToken(
        localId = 111, globalId = 11, state = FakeProtocolStates.A,
        owner = "new-owner", nextWorker = COMPUTATION_DEATILS.outgoingNodeId,
        role = DuchyRole.PRIMARY, attempt = 1, lastUpdateTime = TEST_INSTANT.toEpochMilli()),
      database.claimTask("new-owner"))
    // No task may be claimed at this point
    assertNull(database.claimTask("new-owner"))
  }

  @Test
  fun `unimplemented interface functions`() {
    val token = ComputationToken(
      localId = 1, nextWorker = "", role = DuchyRole.PRIMARY,
      owner = null, attempt = 1, state = FakeProtocolStates.A,
      globalId = 0, lastUpdateTime = 0
    )
    assertFailsWith(NotImplementedError::class) { database.renewTask(token) }
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
