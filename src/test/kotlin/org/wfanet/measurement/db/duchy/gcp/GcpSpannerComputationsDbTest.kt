package org.wfanet.measurement.db.duchy.gcp

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.SpannerException
import com.google.cloud.spanner.Struct
import java.lang.IllegalStateException
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.Duchy
import org.wfanet.measurement.common.DuchyOrder
import org.wfanet.measurement.common.DuchyRole
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.db.duchy.AfterTransition
import org.wfanet.measurement.db.duchy.BlobDependencyType
import org.wfanet.measurement.db.duchy.BlobRef
import org.wfanet.measurement.db.duchy.ComputationToken
import org.wfanet.measurement.db.duchy.ProtocolStateEnumHelper
import org.wfanet.measurement.db.gcp.testing.UsingSpannerEmulator
import org.wfanet.measurement.db.gcp.testing.assertQueryReturns
import org.wfanet.measurement.db.gcp.testing.assertQueryReturnsNothing
import org.wfanet.measurement.db.gcp.toGcpTimestamp
import org.wfanet.measurement.db.gcp.toProtoBytes
import org.wfanet.measurement.db.gcp.toProtoJson
import org.wfanet.measurement.internal.ComputationBlobDependency
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

  private val testClock =
    TestClockWithNamedInstants(TEST_INSTANT)

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
      clock = testClock,
      stateEnumHelper = ProtocolHelper
    )

  @Test
  fun `insert two computations`() {
    val idGenerator = HalfOfGlobalBitsAndTimeStampIdGenerator(testClock)
    val id1 = 0xABCDEF0123
    val resultId1 = database.insertComputation(id1, FakeProtocolStates.A)
    val id2 = 0x6699AA231
    val resultId2 = database.insertComputation(id2, FakeProtocolStates.A)
    assertEquals(
      ComputationToken(
        localId = idGenerator.localId(id1), nextWorker = "BOHEMIA", role = DuchyRole.SECONDARY,
        owner = null, attempt = 1, state = FakeProtocolStates.A,
        globalId = id1, lastUpdateTime = TEST_INSTANT.toEpochMilli()
      ),
      resultId1
    )

    assertEquals(
      ComputationToken(
        localId = idGenerator.localId(id2), nextWorker = "BOHEMIA", role = DuchyRole.PRIMARY,
        owner = null, attempt = 1, state = FakeProtocolStates.A,
        globalId = id2, lastUpdateTime = TEST_INSTANT.toEpochMilli()
      ),
      resultId2
    )

    val expectedDetails123 = ComputationDetails.newBuilder().apply {
      role = ComputationDetails.RoleInComputation.SECONDARY
      incomingNodeId = "SALZBURG"
      outgoingNodeId = "BOHEMIA"
      blobsStoragePrefix = "knight-computation-stage-storage/${resultId1.localId}"
    }.build()

    val expectedDetails220 = ComputationDetails.newBuilder().apply {
      role = ComputationDetails.RoleInComputation.PRIMARY
      incomingNodeId = "SALZBURG"
      outgoingNodeId = "BOHEMIA"
      blobsStoragePrefix = "knight-computation-stage-storage/${resultId2.localId}"
    }.build()
    assertQueryReturns(
      spanner.client,
      """
      SELECT ComputationId, ComputationStage, UpdateTime, GlobalComputationId, LockOwner, 
             LockExpirationTime, ComputationDetails, ComputationDetailsJSON
      FROM Computations
      ORDER BY ComputationId DESC
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId1.localId)
        .set("ComputationStage").to(resultId1.state.ordinal.toLong())
        .set("UpdateTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("GlobalComputationId").to(resultId1.globalId)
        .set("LockOwner").to(resultId1.owner)
        .set("LockExpirationTime").to(null as Timestamp?)
        .set("ComputationDetails").toProtoBytes(expectedDetails123)
        .set("ComputationDetailsJSON").toProtoJson(expectedDetails123)
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId2.localId)
        .set("ComputationStage").to(resultId2.state.ordinal.toLong())
        .set("UpdateTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("GlobalComputationId").to(resultId2.globalId)
        .set("LockOwner").to(resultId2.owner)
        .set("LockExpirationTime").to(null as Timestamp?)
        .set("ComputationDetails").toProtoBytes(expectedDetails220)
        .set("ComputationDetailsJSON").toProtoJson(expectedDetails220)
        .build()
    )

    assertQueryReturns(
      spanner.client,
      """
      SELECT ComputationId, ComputationStage, CreationTime, NextAttempt,
             EndTime, Details, DetailsJSON
      FROM ComputationStages
      ORDER BY ComputationId DESC
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId1.localId)
        .set("ComputationStage").to(resultId1.state.ordinal.toLong())
        .set("CreationTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("NextAttempt").to(resultId1.attempt + 1)
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(ComputationStageDetails.getDefaultInstance())
        .set("DetailsJSON").toProtoJson(ComputationStageDetails.getDefaultInstance())
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId2.localId)
        .set("ComputationStage").to(resultId2.state.ordinal.toLong())
        .set("CreationTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("NextAttempt").to(resultId2.attempt + 1)
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(ComputationStageDetails.getDefaultInstance())
        .set("DetailsJSON").toProtoJson(ComputationStageDetails.getDefaultInstance())
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
      ORDER BY ComputationId DESC
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId1.localId)
        .set("ComputationStage").to(resultId1.state.ordinal.toLong())
        .set("Attempt").to(resultId1.attempt)
        .set("BeginTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId2.localId)
        .set("ComputationStage").to(resultId2.state.ordinal.toLong())
        .set("Attempt").to(resultId2.attempt)
        .set("BeginTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .build()
    )
  }

  @Test
  fun `insert computations with same global id fails`() {
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
    val computation = insertComputation(
      localId = 100,
      updateTime = lastUpdated.toGcpTimestamp(),
      globalId = 21231,
      stage = 3,
      lockOwner = "Fred",
      lockExpirationTime = lockExpires.toGcpTimestamp(),
      details = COMPUTATION_DEATILS
    )
    val computationStageB = insertComputationStage(
      localId = 100,
      stage = 2,
      nextAttempt = 45,
      creationTime = lastUpdated.minusSeconds(2).toGcpTimestamp(),
      endTime = lastUpdated.minusMillis(200).toGcpTimestamp(),
      details = STAGE_DETAILS
    )
    val computationStageD = insertComputationStage(
      localId = 100,
      stage = 3,
      nextAttempt = 2,
      creationTime = lastUpdated.toGcpTimestamp(),
      details = STAGE_DETAILS
    )
    spanner.client.write(
      listOf(
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

    val computation = insertComputation(
      localId = token.localId,
      updateTime = lastUpdated.toGcpTimestamp(),
      globalId = token.globalId,
      stage = ProtocolHelper.enumToLong(token.state),
      lockOwner = token.owner!!,
      lockExpirationTime = lockExpires.toGcpTimestamp(),
      details = COMPUTATION_DEATILS
    )
    val differentComputation =
      insertComputation(
        localId = 456789,
        updateTime = lastUpdated.toGcpTimestamp(),
        globalId = 10111213,
        stage = 1,
        lockOwner = token.owner!!,
        lockExpirationTime = lockExpires.toGcpTimestamp(),
        details = COMPUTATION_DEATILS
      )

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
        .set("ComputationDetails").toProtoBytes(COMPUTATION_DEATILS)
        .set("ComputationDetailsJSON").toProtoJson(COMPUTATION_DEATILS)
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(456789)
        .set("ComputationStage").to(1)
        .set("GlobalComputationId").to(10111213)
        .set("LockOwner").to(token.owner)
        .set("LockExpirationTime").to(lockExpires.toGcpTimestamp())
        .set("ComputationDetails").toProtoBytes(COMPUTATION_DEATILS)
        .set("ComputationDetailsJSON").toProtoJson(COMPUTATION_DEATILS)
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

    val computation = insertComputation(
      localId = token.localId,
      updateTime = lastUpdated.toGcpTimestamp(),
      globalId = token.globalId,
      stage = ProtocolHelper.enumToLong(token.state),
      lockOwner = token.owner!!,
      lockExpirationTime = lockExpires.toGcpTimestamp(),
      details = COMPUTATION_DEATILS
    )
    spanner.client.write(listOf(computation))
    assertFailsWith<SpannerException> { database.enqueue(token) }
  }

  @Test
  fun claimTask() {
    testClock.tickSeconds("6_minutes_ago")
    testClock.tickSeconds("5_minutes_ago", 60)
    testClock.tickSeconds("TimeOfTest", 300)
    val fiveMinutesAgo = testClock["5_minutes_ago"].toGcpTimestamp()
    val sixMinutesAgo = testClock["6_minutes_ago"].toGcpTimestamp()
    val enqueuedFiveMinutesAgo = insertComputation(
      localId = 555,
      updateTime = fiveMinutesAgo,
      globalId = 55,
      stage = 0,
      lockOwner = WRITE_NULL_STRING,
      lockExpirationTime = fiveMinutesAgo,
      details = COMPUTATION_DEATILS
    )
    val enqueuedFiveMinutesAgoStage = insertComputationStage(
      localId = 555,
      stage = 0,
      nextAttempt = 1,
      creationTime = Instant.ofEpochMilli(3456789L).toGcpTimestamp(),
      details = STAGE_DETAILS
    )

    val enqueuedSixMinutesAgo = insertComputation(
      localId = 66,
      stage = 0,
      updateTime = sixMinutesAgo,
      globalId = 6,
      lockOwner = WRITE_NULL_STRING,
      lockExpirationTime = sixMinutesAgo,
      details = COMPUTATION_DEATILS
    )
    val enqueuedSixMinutesAgoStage = insertComputationStage(
      localId = 66,
      stage = 0,
      nextAttempt = 1,
      creationTime = Instant.ofEpochMilli(3456789L).toGcpTimestamp(),
      details = STAGE_DETAILS
    )
    spanner.client.write(
      listOf(
        enqueuedFiveMinutesAgo,
        enqueuedFiveMinutesAgoStage,
        enqueuedSixMinutesAgo,
        enqueuedSixMinutesAgoStage
      )
    )
    assertEquals(
      ComputationToken(
        localId = 66, globalId = 6, state = FakeProtocolStates.A,
        owner = "the-owner-of-the-lock", nextWorker = COMPUTATION_DEATILS.outgoingNodeId,
        role = DuchyRole.PRIMARY, attempt = 1, lastUpdateTime = testClock.last().toEpochMilli()
      ),
      database.claimTask("the-owner-of-the-lock")
    )

    assertQueryReturns(
      spanner.client,
      """
      SELECT Attempt, BeginTime, EndTime FROM ComputationStageAttempts
      WHERE ComputationId = 66 AND ComputationStage = 0
      """.trimIndent(),
      Struct.newBuilder()
        .set("Attempt").to(1)
        .set("BeginTime").to(testClock.last().toGcpTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .build()
    )

    assertEquals(
      ComputationToken(
        localId = 555, globalId = 55, state = FakeProtocolStates.A,
        owner = "the-owner-of-the-lock", nextWorker = COMPUTATION_DEATILS.outgoingNodeId,
        role = DuchyRole.PRIMARY, attempt = 1, lastUpdateTime = testClock.last().toEpochMilli()
      ),
      database.claimTask("the-owner-of-the-lock")
    )

    assertQueryReturns(
      spanner.client,
      """
      SELECT Attempt, BeginTime, EndTime FROM ComputationStageAttempts
      WHERE ComputationId = 555 AND ComputationStage = 0
      """.trimIndent(),
      Struct.newBuilder()
        .set("Attempt").to(1)
        .set("BeginTime").to(testClock.last().toGcpTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .build()
    )

    // No tasks to claim anymore
    assertNull(database.claimTask("the-owner-of-the-lock"))
  }

  @Test
  fun `claim locked tasks`() {
    testClock.tickSeconds("5_minutes_ago", 60)
    testClock.tickSeconds("TimeOfTest", 300)
    val fiveMinutesAgo = testClock["5_minutes_ago"].toGcpTimestamp()
    val fiveMinutesFromNow = testClock.last().plusSeconds(300).toGcpTimestamp()
    val expiredClaim = insertComputation(
      localId = 111L,
      stage = 0,
      updateTime = testClock["start"].toGcpTimestamp(),
      globalId = 11,
      lockOwner = "owner-of-the-lock",
      lockExpirationTime = fiveMinutesAgo,
      details = COMPUTATION_DEATILS
    )
    val expiredClaimStage = insertComputationStage(
      localId = 111,
      stage = 0,
      nextAttempt = 2,
      creationTime = fiveMinutesAgo,
      details = STAGE_DETAILS
    )
    val expiredClaimAttempt = insertComputationStageAttempt(
      localId = 111,
      stage = 0,
      attempt = 1,
      beginTime = fiveMinutesAgo
    )
    spanner.client.write(listOf(expiredClaim, expiredClaimStage, expiredClaimAttempt))

    val claimed = insertComputation(
      localId = 333,
      stage = 0,
      updateTime = testClock["start"].toGcpTimestamp(),
      globalId = 33,
      lockOwner = "owner-of-the-lock",
      lockExpirationTime = fiveMinutesFromNow,
      details = COMPUTATION_DEATILS
    )
    val claimedStage = insertComputationStage(
      localId = 333,
      stage = 0,
      nextAttempt = 2,
      creationTime = fiveMinutesAgo,
      details = STAGE_DETAILS
    )
    val claimedAttempt = insertComputationStageAttempt(
      localId = 333,
      stage = 0,
      attempt = 1,
      beginTime = fiveMinutesAgo
    )
    spanner.client.write(listOf(claimed, claimedStage, claimedAttempt))

    // Claim a task that is owned but the lock expired.
    assertEquals(
      ComputationToken(
        localId = 111, globalId = 11, state = FakeProtocolStates.A,
        owner = "new-owner", nextWorker = COMPUTATION_DEATILS.outgoingNodeId,
        role = DuchyRole.PRIMARY, attempt = 2, lastUpdateTime = testClock.last().toEpochMilli()
      ),
      database.claimTask("new-owner")
    )

    assertQueryReturns(
      spanner.client,
      """
      SELECT Attempt, BeginTime, EndTime FROM ComputationStageAttempts
      WHERE ComputationId = 111 AND ComputationStage = 0
      ORDER BY Attempt
      """.trimIndent(),
      Struct.newBuilder()
        .set("Attempt").to(1)
        .set("BeginTime").to(fiveMinutesAgo)
        .set("EndTime").to(testClock.last().toGcpTimestamp())
        .build(),
      Struct.newBuilder()
        .set("Attempt").to(2)
        .set("BeginTime").to(testClock.last().toGcpTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .build()
    )

    // No task may be claimed at this point
    assertNull(database.claimTask("new-owner"))
  }

  @Test
  fun renewTask() {
    val lastUpdated = TEST_INSTANT.minusSeconds(355)
    val token = ComputationToken(
      localId = 4315, globalId = 55, state = FakeProtocolStates.E,
      owner = "the-owner-of-the-lock", nextWorker = COMPUTATION_DEATILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 2, lastUpdateTime = lastUpdated.toEpochMilli()
    )
    val fiveSecondsFromNow = Instant.now().plusSeconds(5).toGcpTimestamp()
    val computation = insertComputation(
      token.localId,
      lastUpdated.toGcpTimestamp(),
      token.globalId,
      stage = 4,
      lockOwner = token.owner!!,
      lockExpirationTime = fiveSecondsFromNow,
      details = COMPUTATION_DEATILS
    )
    val stage = insertComputationStage(
      localId = token.localId,
      stage = 4,
      nextAttempt = 3,
      creationTime = Instant.ofEpochMilli(3456789L).toGcpTimestamp(),
      details = STAGE_DETAILS
    )
    spanner.client.write(listOf(computation, stage))
    assertEquals(
      token.copy(lastUpdateTime = TEST_INSTANT.toEpochMilli()),
      database.renewTask(token)
    )

    assertQueryReturns(
      spanner.client,
      """
      SELECT LockOwner, LockExpirationTime
      FROM Computations
      """.trimIndent(),
      Struct.newBuilder()
        .set("LockOwner").to(token.owner)
        .set("LockExpirationTime").to(TEST_INSTANT.plusSeconds(300).toGcpTimestamp())
        .build()
    )
  }

  @Test
  fun `renewTask for missing computation fails`() {
    val token = ComputationToken(
      localId = 4315, globalId = 55, state = FakeProtocolStates.E,
      owner = "the-owner-of-the-lock", nextWorker = COMPUTATION_DEATILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 2, lastUpdateTime = 12345L
    )
    assertFailsWith<SpannerException> { database.renewTask(token) }
  }

  @Test
  fun `renewTask for token with no owner fails`() {
    val token = ComputationToken(
      localId = 4315, globalId = 55, state = FakeProtocolStates.E,
      owner = null, nextWorker = COMPUTATION_DEATILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 2, lastUpdateTime = 12345L
    )
    assertFailsWith<IllegalStateException> { database.renewTask(token) }
  }

  private fun testTransitionOfStageWhere(
    afterTransition: AfterTransition
  ): ComputationToken<FakeProtocolStates> {
    testClock.tickSeconds("stage_b_created")
    testClock.tickSeconds("last_updated")
    testClock.tickSeconds("lock_expires", 100)
    val token = ComputationToken(
      localId = 4315, globalId = 55, state = FakeProtocolStates.B,
      owner = "the-owner-of-the-lock", nextWorker = COMPUTATION_DEATILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 2,
      lastUpdateTime = testClock["last_updated"].toEpochMilli()
    )
    val computation = insertComputation(
      localId = token.localId,
      updateTime = testClock["last_updated"].toGcpTimestamp(),
      globalId = token.globalId,
      stage = 1,
      lockOwner = token.owner!!,
      lockExpirationTime = testClock["lock_expires"].toGcpTimestamp(),
      details = COMPUTATION_DEATILS
    )
    val stage = insertComputationStage(
      localId = token.localId,
      stage = 1,
      nextAttempt = 3,
      creationTime = testClock["stage_b_created"].toGcpTimestamp(),
      details = STAGE_DETAILS
    )
    val attempt =
      insertComputationStageAttempt(
        localId = token.localId,
        stage = 1,
        attempt = 2,
        beginTime = testClock["stage_b_created"].toGcpTimestamp()
      )
    spanner.client.write(listOf(computation, stage, attempt))
    testClock.tickSeconds("update_stage", 100)
    assertEquals(
      database.updateComputationState(
        token,
        FakeProtocolStates.D,
        listOf(),
        listOf(),
        afterTransition
      ),
      database.getToken(token.globalId)
    )
    // Ensure the Computation and ComputationStage were updated. This does not check if the
    // lock is held or the exact configurations of the ComputationStageAttempts because they
    // differ depending on what to do after the transition.
    assertStageTransitioned(token)
    return token
  }

  private fun assertStageTransitioned(token: ComputationToken<FakeProtocolStates>) {
    assertQueryReturns(
      spanner.client,
      """
      SELECT ComputationId, ComputationStage, UpdateTime, GlobalComputationId
      FROM Computations
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolHelper.enumToLong(FakeProtocolStates.D))
        .set("UpdateTime").to(testClock["update_stage"].toGcpTimestamp())
        .set("GlobalComputationId").to(token.globalId)
        .build()
    )

    assertQueryReturns(
      spanner.client,
      """
      SELECT ComputationId, ComputationStage, CreationTime, EndTime, Details
      FROM ComputationStages
      ORDER BY ComputationStage
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolHelper.enumToLong(FakeProtocolStates.B))
        .set("CreationTime").to(testClock["stage_b_created"].toGcpTimestamp())
        .set("EndTime").to(testClock["update_stage"].toGcpTimestamp())
        .set("Details").toProtoBytes(
          ComputationStageDetails.newBuilder().apply {
            followingStageValue = ProtocolHelper.enumToLong(FakeProtocolStates.D).toInt()
          }.build()
        )
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolHelper.enumToLong(FakeProtocolStates.D))
        .set("CreationTime").to(testClock["update_stage"].toGcpTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(
          ComputationStageDetails.newBuilder().apply {
            previousStageValue = ProtocolHelper.enumToLong(FakeProtocolStates.B).toInt()
          }.build()
        )
        .build()
    )
  }

  @Test
  fun `updateComputationState and continue working`() {
    val token = testTransitionOfStageWhere(AfterTransition.CONTINUE_WORKING)

    assertQueryReturns(
      spanner.client,
      "SELECT ComputationId, LockOwner, LockExpirationTime FROM Computations",
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("LockOwner").to(token.owner)
        .set("LockExpirationTime").to(testClock.last().plusSeconds(300).toGcpTimestamp())
        .build()
    )

    assertQueryReturns(
      spanner.client,
      """
      SELECT ComputationId, ComputationStage, Attempt, BeginTime, EndTime
      FROM ComputationStageAttempts
      ORDER BY ComputationStage, Attempt
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolHelper.enumToLong(FakeProtocolStates.B))
        .set("Attempt").to(2)
        .set("BeginTime").to(testClock["stage_b_created"].toGcpTimestamp())
        .set("EndTime").to(testClock.last().toGcpTimestamp())
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolHelper.enumToLong(FakeProtocolStates.D))
        .set("Attempt").to(1)
        .set("BeginTime").to(testClock.last().toGcpTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .build()
    )
  }

  @Test
  fun `updateComputationState and add to queue`() {
    val token = testTransitionOfStageWhere(AfterTransition.ADD_UNCLAIMED_TO_QUEUE)

    assertQueryReturns(
      spanner.client,
      "SELECT ComputationId, LockOwner, LockExpirationTime FROM Computations",
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("LockOwner").to(null as String?)
        .set("LockExpirationTime").to(testClock.last().toGcpTimestamp())
        .build()
    )

    assertQueryReturns(
      spanner.client,
      """
      SELECT ComputationId, ComputationStage, Attempt, BeginTime, EndTime
      FROM ComputationStageAttempts
      ORDER BY ComputationStage, Attempt
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolHelper.enumToLong(FakeProtocolStates.B))
        .set("Attempt").to(2)
        .set("BeginTime").to(testClock["stage_b_created"].toGcpTimestamp())
        .set("EndTime").to(testClock.last().toGcpTimestamp())
        .build()
    )
  }

  @Test
  fun `updateComputationState and do not add to queue`() {
    val token = testTransitionOfStageWhere(AfterTransition.DO_NOT_ADD_TO_QUEUE)

    assertQueryReturns(
      spanner.client,
      "SELECT ComputationId, LockOwner, LockExpirationTime FROM Computations",
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("LockOwner").to(null as String?)
        .set("LockExpirationTime").to(null as Timestamp?)
        .build()
    )

    assertQueryReturns(
      spanner.client,
      """
      SELECT ComputationId, ComputationStage, Attempt, BeginTime, EndTime
      FROM ComputationStageAttempts
      ORDER BY ComputationStage, Attempt
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolHelper.enumToLong(FakeProtocolStates.B))
        .set("Attempt").to(2)
        .set("BeginTime").to(testClock["stage_b_created"].toGcpTimestamp())
        .set("EndTime").to(testClock.last().toGcpTimestamp())
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolHelper.enumToLong(FakeProtocolStates.D))
        .set("Attempt").to(1)
        .set("BeginTime").to(testClock.last().toGcpTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .build()
    )
  }

  @Test
  fun `many state transitions`() {
    testClock.tickSeconds("insert_computation", 20)
    var token = database.insertComputation(12345, FakeProtocolStates.A)
    assertNull(database.claimTask("TestJob"))
    testClock.tickSeconds("move_to_B", 21)
    token = database.updateComputationState(
      token, FakeProtocolStates.B,
      listOf(BlobRef(1, "/path/to/inputs/123")),
      listOf(2),
      AfterTransition.CONTINUE_WORKING
    )
    testClock.tickSeconds("move_to_C", 3)
    token = database.updateComputationState(
      token, FakeProtocolStates.C,
      listOf(),
      listOf(2),
      AfterTransition.ADD_UNCLAIMED_TO_QUEUE
    )
    database.enqueue(token)
    testClock.tickSeconds("claimed_in_C", 3)
    token = assertNotNull(database.claimTask("a-second-worker"))
    testClock.tickSeconds("move_to_E", 3)
    token = database.updateComputationState(
      token, FakeProtocolStates.E,
      listOf(BlobRef(1, "/path/to/inputs/789")),
      listOf(),
      AfterTransition.DO_NOT_ADD_TO_QUEUE
    )
    assertNull(database.claimTask("nothing-to-claim"))

    assertQueryReturns(
      spanner.client,
      """
      SELECT ComputationId, ComputationStage, UpdateTime, GlobalComputationId, LockOwner,
          LockExpirationTime
      FROM Computations
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolHelper.enumToLong(FakeProtocolStates.E))
        .set("UpdateTime").to(testClock.last().toGcpTimestamp())
        .set("GlobalComputationId").to(12345)
        .set("LockOwner").to(null as String?)
        .set("LockExpirationTime").to(null as Timestamp?)
        .build()
    )

    assertQueryReturns(
      spanner.client,
      """
      SELECT ComputationId, ComputationStage, CreationTime, NextAttempt, EndTime, Details
      FROM ComputationStages
      ORDER BY ComputationStage
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolHelper.enumToLong(FakeProtocolStates.A))
        .set("CreationTime").to(testClock["insert_computation"].toGcpTimestamp())
        .set("NextAttempt").to(2)
        .set("EndTime").to(testClock["move_to_B"].toGcpTimestamp())
        .set("Details").toProtoBytes(
          ComputationStageDetails.newBuilder().apply {
            followingStageValue = ProtocolHelper.enumToLong(FakeProtocolStates.B).toInt()
          }.build()
        )
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolHelper.enumToLong(FakeProtocolStates.B))
        .set("CreationTime").to(testClock["move_to_B"].toGcpTimestamp())
        .set("NextAttempt").to(2)
        .set("EndTime").to(testClock["move_to_C"].toGcpTimestamp())
        .set("Details").toProtoBytes(
          ComputationStageDetails.newBuilder().apply {
            previousStageValue = ProtocolHelper.enumToLong(FakeProtocolStates.A).toInt()
            followingStageValue = ProtocolHelper.enumToLong(FakeProtocolStates.C).toInt()
          }.build()
        )
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolHelper.enumToLong(FakeProtocolStates.C))
        .set("CreationTime").to(testClock["move_to_C"].toGcpTimestamp())
        .set("NextAttempt").to(2)
        .set("EndTime").to(testClock["move_to_E"].toGcpTimestamp())
        .set("Details").toProtoBytes(
          ComputationStageDetails.newBuilder().apply {
            previousStageValue = ProtocolHelper.enumToLong(FakeProtocolStates.B).toInt()
            followingStageValue = ProtocolHelper.enumToLong(FakeProtocolStates.E).toInt()
          }.build()
        )
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolHelper.enumToLong(FakeProtocolStates.E))
        .set("CreationTime").to(testClock["move_to_E"].toGcpTimestamp())
        .set("NextAttempt").to(2)
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(
          ComputationStageDetails.newBuilder().apply {
            previousStageValue = ProtocolHelper.enumToLong(FakeProtocolStates.C).toInt()
          }.build()
        )
        .build()
    )

    assertQueryReturns(
      spanner.client,
      """
      SELECT ComputationId, ComputationStage, BlobId, PathToBlob, DependencyType
      FROM ComputationBlobReferences
      ORDER BY ComputationStage, BlobId
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolHelper.enumToLong(FakeProtocolStates.B))
        .set("BlobId").to(0L)
        .set("PathToBlob").to("/path/to/inputs/123")
        .set("DependencyType").to(ComputationBlobDependency.INPUT.ordinal.toLong())
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolHelper.enumToLong(FakeProtocolStates.B))
        .set("BlobId").to(1L)
        .set("PathToBlob").to(null as String?)
        .set("DependencyType").to(ComputationBlobDependency.OUTPUT.ordinal.toLong())
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolHelper.enumToLong(FakeProtocolStates.C))
        .set("BlobId").to(0L)
        .set("PathToBlob").to(null as String?)
        .set("DependencyType").to(ComputationBlobDependency.OUTPUT.ordinal.toLong())
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolHelper.enumToLong(FakeProtocolStates.E))
        .set("BlobId").to(0L)
        .set("PathToBlob").to("/path/to/inputs/789")
        .set("DependencyType").to(ComputationBlobDependency.INPUT.ordinal.toLong())
        .build()
    )
  }

  @Test
  fun `updateComputationState illegal state transition fails`() {
    val token = ComputationToken(
      localId = 1, nextWorker = "", role = DuchyRole.PRIMARY,
      owner = null, attempt = 1, state = FakeProtocolStates.A,
      globalId = 0, lastUpdateTime = 0
    )
    assertFailsWith<IllegalArgumentException> {
      database.updateComputationState(
        token,
        FakeProtocolStates.D,
        listOf(),
        listOf(),
        AfterTransition.DO_NOT_ADD_TO_QUEUE
      )
    }
  }

  @Test
  fun readBlobReferences() {
    val token = ComputationToken(
      localId = 4315, globalId = 55, state = FakeProtocolStates.B,
      owner = null, nextWorker = COMPUTATION_DEATILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 1, lastUpdateTime = testClock.last().toEpochMilli()
    )
    val computation = insertComputation(
      localId = token.localId,
      updateTime = testClock.last().toGcpTimestamp(),
      stage = 1,
      globalId = token.globalId,
      lockOwner = WRITE_NULL_STRING,
      lockExpirationTime = testClock.last().toGcpTimestamp(),
      details = COMPUTATION_DEATILS
    )
    val stage = insertComputationStage(
      localId = token.localId,
      stage = 1,
      nextAttempt = 3,
      creationTime = testClock.last().toGcpTimestamp(),
      details = STAGE_DETAILS
    )

    val inputBlobA = Mutation.newInsertBuilder("ComputationBlobReferences")
      .set("ComputationId").to(token.localId)
      .set("ComputationStage").to(1)
      .set("BlobId").to(0L)
      .set("PathToBlob").to("/path/to/blob/A")
      .set("DependencyType").to(ComputationBlobDependency.INPUT_VALUE.toLong())
      .build()
    val inputBlobB = Mutation.newInsertBuilder("ComputationBlobReferences")
      .set("ComputationId").to(token.localId)
      .set("ComputationStage").to(1)
      .set("BlobId").to(1L)
      .set("PathToBlob").to("/path/to/blob/B")
      .set("DependencyType").to(ComputationBlobDependency.INPUT_VALUE.toLong())
      .build()
    val outputBlobC = Mutation.newInsertBuilder("ComputationBlobReferences")
      .set("ComputationId").to(token.localId)
      .set("ComputationStage").to(1)
      .set("BlobId").to(2L)
      .set("PathToBlob").to("/path/to/blob/C")
      .set("DependencyType").to(ComputationBlobDependency.OUTPUT_VALUE.toLong())
      .build()
    val outputBlobD = Mutation.newInsertBuilder("ComputationBlobReferences")
      .set("ComputationId").to(token.localId)
      .set("ComputationStage").to(1)
      .set("BlobId").to(3L)
      .set("DependencyType").to(ComputationBlobDependency.OUTPUT_VALUE.toLong())
      .build()
    spanner.client.write(listOf(computation, stage))
    spanner.client.write(listOf(inputBlobA, inputBlobB, outputBlobC, outputBlobD))
    assertEquals(
      mapOf(
        0L to "/path/to/blob/A",
        1L to "/path/to/blob/B",
        2L to "/path/to/blob/C",
        3L to null
      ),
      database.readBlobReferences(token, BlobDependencyType.ANY))
    assertEquals(
      mapOf(
        2L to "/path/to/blob/C",
        3L to null
      ),
      database.readBlobReferences(token, BlobDependencyType.OUTPUT))
    assertEquals(
      mapOf(
        0L to "/path/to/blob/A",
        1L to "/path/to/blob/B"
      ),
      database.readBlobReferences(token, BlobDependencyType.INPUT))
  }

  @Test
  fun `unimplemented interface functions`() {
    val token = ComputationToken(
      localId = 1, nextWorker = "", role = DuchyRole.PRIMARY,
      owner = null, attempt = 1, state = FakeProtocolStates.A,
      globalId = 0, lastUpdateTime = 0
    )
    assertFailsWith(NotImplementedError::class) {
      database.writeOutputBlobReference(
        token,
        BlobRef(0L, "")
      )
    }
  }
}
