package org.wfanet.measurement.db.duchy.gcp

import com.google.cloud.Timestamp
import com.google.cloud.spanner.SpannerException
import com.google.cloud.spanner.Struct
import com.google.common.truth.extensions.proto.ProtoTruth
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlinx.coroutines.runBlocking
import org.junit.Before
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
import org.wfanet.measurement.db.duchy.EndComputationReason
import org.wfanet.measurement.db.duchy.ProtocolStageDetails
import org.wfanet.measurement.db.duchy.ProtocolStageEnumHelper
import org.wfanet.measurement.db.gcp.testing.UsingSpannerEmulator
import org.wfanet.measurement.db.gcp.testing.assertQueryReturns
import org.wfanet.measurement.db.gcp.toGcpTimestamp
import org.wfanet.measurement.db.gcp.toProtoBytes
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.gcp.toProtoJson
import org.wfanet.measurement.internal.ComputationBlobDependency
import org.wfanet.measurement.internal.db.gcp.ComputationDetails
import org.wfanet.measurement.internal.db.gcp.ComputationStageAttemptDetails
import org.wfanet.measurement.internal.db.gcp.FakeProtocolStageDetails

/**
 * +--------------+
 * |              |
 * |              v
 * A -> B -> C -> E
 *      |         ^
 *      v         |
 *      D --------+
 */
enum class FakeProtocolStages {
  A, B, C, D, E;
}

object ProtocolStages : ProtocolStageEnumHelper<FakeProtocolStages> {
  override val validInitialStages = setOf(FakeProtocolStages.A)
  override val validTerminalStages = setOf(FakeProtocolStages.E)
  override val validSuccessors = mapOf(
    FakeProtocolStages.A to setOf(FakeProtocolStages.B),
    FakeProtocolStages.B to setOf(FakeProtocolStages.C, FakeProtocolStages.D)
  )

  override fun enumToLong(value: FakeProtocolStages): Long {
    return value.ordinal.toLong()
  }

  override fun longToEnum(value: Long): FakeProtocolStages {
    return when (value) {
      0L -> FakeProtocolStages.A
      1L -> FakeProtocolStages.B
      2L -> FakeProtocolStages.C
      3L -> FakeProtocolStages.D
      4L -> FakeProtocolStages.E
      else -> error("Bad value")
    }
  }
}

class StageDetailsHelper : ProtocolStageDetails<FakeProtocolStages, FakeProtocolStageDetails> {
  override fun detailsFor(stage: FakeProtocolStages): FakeProtocolStageDetails {
    return FakeProtocolStageDetails.newBuilder()
      .setName(stage.name)
      .build()
  }

  override fun parseDetails(bytes: ByteArray): FakeProtocolStageDetails =
    FakeProtocolStageDetails.parseFrom(bytes)
}

@RunWith(JUnit4::class)
class GcpSpannerComputationsDbTest : UsingSpannerEmulator("/src/main/db/gcp/computations.sdl") {

  companion object {
    val COMPUTATION_DETAILS: ComputationDetails = ComputationDetails.newBuilder().apply {
      incomingNodeId = "AUSTRIA"
      outgoingNodeId = "BOHEMIA"
      blobsStoragePrefix = "blobs"
      role = ComputationDetails.RoleInComputation.PRIMARY
    }.build()

    val TEST_INSTANT: Instant = Instant.ofEpochMilli(123456789L)
  }

  private val testClock = TestClockWithNamedInstants(TEST_INSTANT)
  private val computationMutations = ComputationMutations(ProtocolStages, StageDetailsHelper())

  private lateinit var database:
    GcpSpannerComputationsDb<FakeProtocolStages, FakeProtocolStageDetails>

  @Before
  fun initDatabase() {
    database = GcpSpannerComputationsDb(
      databaseClient,
      "AUSTRIA",
      DuchyOrder(
        setOf(
          Duchy("BOHEMIA", 10L.toBigInteger()),
          Duchy("SALZBURG", 200L.toBigInteger()),
          Duchy("AUSTRIA", 303L.toBigInteger())
        )
      ),
      clock = testClock,
      computationMutations = computationMutations
    )
  }

  @Test
  fun `insert two computations`() = runBlocking<Unit> {
    val idGenerator = HalfOfGlobalBitsAndTimeStampIdGenerator(testClock)
    val id1 = 0xABCDEF0123
    val resultId1 = database.insertComputation(id1, FakeProtocolStages.A)
    val id2 = 0x6699AA231
    val resultId2 = database.insertComputation(id2, FakeProtocolStages.A)
    assertEquals(
      ComputationToken(
        localId = idGenerator.localId(id1), nextWorker = "BOHEMIA", role = DuchyRole.SECONDARY,
        owner = null, attempt = 1, stage = FakeProtocolStages.A,
        globalId = id1, lastUpdateTime = TEST_INSTANT.toEpochMilli()
      ),
      resultId1
    )

    assertEquals(
      ComputationToken(
        localId = idGenerator.localId(id2), nextWorker = "BOHEMIA", role = DuchyRole.PRIMARY,
        owner = null, attempt = 1, stage = FakeProtocolStages.A,
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
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, UpdateTime, GlobalComputationId, LockOwner, 
             LockExpirationTime, ComputationDetails, ComputationDetailsJSON
      FROM Computations
      ORDER BY ComputationId DESC
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId1.localId)
        .set("ComputationStage").to(resultId1.stage.ordinal.toLong())
        .set("UpdateTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("GlobalComputationId").to(resultId1.globalId)
        .set("LockOwner").to(resultId1.owner)
        .set("LockExpirationTime").to(null as Timestamp?)
        .set("ComputationDetails").toProtoBytes(expectedDetails123)
        .set("ComputationDetailsJSON").toProtoJson(expectedDetails123)
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId2.localId)
        .set("ComputationStage").to(resultId2.stage.ordinal.toLong())
        .set("UpdateTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("GlobalComputationId").to(resultId2.globalId)
        .set("LockOwner").to(resultId2.owner)
        .set("LockExpirationTime").to(null as Timestamp?)
        .set("ComputationDetails").toProtoBytes(expectedDetails220)
        .set("ComputationDetailsJSON").toProtoJson(expectedDetails220)
        .build()
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, CreationTime, NextAttempt,
             EndTime, Details, DetailsJSON
      FROM ComputationStages
      ORDER BY ComputationId DESC
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId1.localId)
        .set("ComputationStage").to(resultId1.stage.ordinal.toLong())
        .set("CreationTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("NextAttempt").to(resultId1.attempt + 1)
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(computationMutations.detailsFor(resultId1.stage))
        .set("DetailsJSON").toProtoJson(computationMutations.detailsFor(resultId1.stage))
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId2.localId)
        .set("ComputationStage").to(resultId2.stage.ordinal.toLong())
        .set("CreationTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("NextAttempt").to(resultId2.attempt + 1)
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(computationMutations.detailsFor(resultId2.stage))
        .set("DetailsJSON").toProtoJson(computationMutations.detailsFor(resultId2.stage))
        .build()
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, COUNT(1) as N
      FROM ComputationBlobReferences 
      GROUP BY ComputationId
      ORDER BY ComputationId DESC  
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId1.localId)
        .set("N").to(1)
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId2.localId)
        .set("N").to(1)
        .build()
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, Attempt, BeginTime, EndTime, Details
      FROM ComputationStageAttempts
      ORDER BY ComputationId DESC
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId1.localId)
        .set("ComputationStage").to(resultId1.stage.ordinal.toLong())
        .set("Attempt").to(1)
        .set("BeginTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(ComputationStageAttemptDetails.getDefaultInstance())
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(resultId2.localId)
        .set("ComputationStage").to(resultId2.stage.ordinal.toLong())
        .set("Attempt").to(1)
        .set("BeginTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(ComputationStageAttemptDetails.getDefaultInstance())
        .build()
    )
  }

  @Test
  fun `insert computations with same global id fails`() = runBlocking<Unit> {
    database.insertComputation(123L, FakeProtocolStages.A)
    // This one fails because the same local id is used.
    assertFailsWith(SpannerException::class, "ALREADY_EXISTS") {
      database.insertComputation(123L, FakeProtocolStages.A)
    }
  }

  @Test
  fun `insert computation with bad initial stage fails`() = runBlocking<Unit> {
    assertFailsWith(IllegalArgumentException::class, "Invalid initial stage") {
      database.insertComputation(123L, FakeProtocolStages.B)
    }
    assertFailsWith(IllegalArgumentException::class, "Invalid initial stage") {
      database.insertComputation(123L, FakeProtocolStages.C)
    }
    assertFailsWith(IllegalArgumentException::class, "Invalid initial stage") {
      database.insertComputation(123L, FakeProtocolStages.D)
    }
    assertFailsWith(IllegalArgumentException::class, "Invalid initial stage") {
      database.insertComputation(123L, FakeProtocolStages.E)
    }
  }

  @Test
  fun `getToken for non-existing computation returns null`() = runBlocking<Unit> {
    assertNull(database.getToken(123))
  }

  @Test
  fun getToken() = runBlocking<Unit> {
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val lockExpires = lastUpdated.plusSeconds(1000)
    val computation = computationMutations.insertComputation(
      localId = 100,
      updateTime = lastUpdated.toGcpTimestamp(),
      globalId = 21231,
      stage = FakeProtocolStages.D,
      lockOwner = "Fred",
      lockExpirationTime = lockExpires.toGcpTimestamp(),
      details = COMPUTATION_DETAILS
    )
    val computationStageB = computationMutations.insertComputationStage(
      localId = 100,
      stage = FakeProtocolStages.B,
      nextAttempt = 45,
      creationTime = lastUpdated.minusSeconds(2).toGcpTimestamp(),
      endTime = lastUpdated.minusMillis(200).toGcpTimestamp(),
      details = computationMutations.detailsFor(FakeProtocolStages.B)
    )
    val computationStageD = computationMutations.insertComputationStage(
      localId = 100,
      stage = FakeProtocolStages.D,
      nextAttempt = 2,
      creationTime = lastUpdated.toGcpTimestamp(),
      details = computationMutations.detailsFor(FakeProtocolStages.D)
    )
    databaseClient.write(
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
        stage = FakeProtocolStages.D,
        owner = "Fred",
        role = DuchyRole.PRIMARY,
        nextWorker = COMPUTATION_DETAILS.outgoingNodeId,
        attempt = 1,
        lastUpdateTime = lastUpdated.toEpochMilli()
      ),
      database.getToken(21231)
    )
  }

  @Test
  fun `getToken after insert matches returned token`() = runBlocking<Unit> {
    assertEquals(
      database.insertComputation(505, FakeProtocolStages.A),
      database.getToken(505),
      "Expected token returned by inserting new computation to match one retrieved with getToken()"
    )
  }

  @Test
  fun enqueue() = runBlocking<Unit> {
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val lockExpires = Instant.now().plusSeconds(300)
    val token = ComputationToken(
      localId = 1, globalId = 0, stage = FakeProtocolStages.C,
      owner = "PeterSpacemen", nextWorker = COMPUTATION_DETAILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 1, lastUpdateTime = lastUpdated.toEpochMilli()
    )

    val computation = computationMutations.insertComputation(
      localId = token.localId,
      updateTime = lastUpdated.toGcpTimestamp(),
      globalId = token.globalId,
      stage = token.stage,
      lockOwner = token.owner!!,
      lockExpirationTime = lockExpires.toGcpTimestamp(),
      details = COMPUTATION_DETAILS
    )
    val differentComputation = computationMutations.insertComputation(
      localId = 456789,
      updateTime = lastUpdated.toGcpTimestamp(),
      globalId = 10111213,
      stage = FakeProtocolStages.B,
      lockOwner = token.owner!!,
      lockExpirationTime = lockExpires.toGcpTimestamp(),
      details = COMPUTATION_DETAILS
    )

    databaseClient.write(listOf(computation, differentComputation))
    database.enqueue(token)

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, GlobalComputationId, LockOwner, LockExpirationTime,
             ComputationDetails, ComputationDetailsJSON
      FROM Computations
      ORDER BY ComputationId
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(token.stage))
        .set("GlobalComputationId").to(token.globalId)
        .set("LockOwner").to(null as String?)
        .set("LockExpirationTime").to(TEST_INSTANT.toGcpTimestamp())
        .set("ComputationDetails").toProtoBytes(COMPUTATION_DETAILS)
        .set("ComputationDetailsJSON").toProtoJson(COMPUTATION_DETAILS)
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(456789)
        .set("ComputationStage").to(1)
        .set("GlobalComputationId").to(10111213)
        .set("LockOwner").to(token.owner)
        .set("LockExpirationTime").to(lockExpires.toGcpTimestamp())
        .set("ComputationDetails").toProtoBytes(COMPUTATION_DETAILS)
        .set("ComputationDetailsJSON").toProtoJson(COMPUTATION_DETAILS)
        .build()
    )
  }

  @Test
  fun `enqueue deteleted computation fails`() = runBlocking<Unit> {
    val token = ComputationToken(
      localId = 1, globalId = 0, stage = FakeProtocolStages.C,
      owner = "PeterSpacemen", nextWorker = COMPUTATION_DETAILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 1, lastUpdateTime = 0
    )
    assertFailsWith<SpannerException> { database.enqueue(token) }
  }

  @Test
  fun `enqueue with old token fails`() = runBlocking<Unit> {
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val lockExpires = lastUpdated.plusSeconds(1)
    val token = ComputationToken(
      localId = 1, globalId = 0, stage = FakeProtocolStages.C,
      owner = "PeterSpacemen", nextWorker = COMPUTATION_DETAILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 1,
      lastUpdateTime = lastUpdated.minusSeconds(200).toEpochMilli()
    )

    val computation = computationMutations.insertComputation(
      localId = token.localId,
      updateTime = lastUpdated.toGcpTimestamp(),
      globalId = token.globalId,
      stage = token.stage,
      lockOwner = token.owner!!,
      lockExpirationTime = lockExpires.toGcpTimestamp(),
      details = COMPUTATION_DETAILS
    )
    databaseClient.write(listOf(computation))
    assertFailsWith<SpannerException> { database.enqueue(token) }
  }

  @Test
  fun claimTask() = runBlocking<Unit> {
    testClock.tickSeconds("6_minutes_ago")
    testClock.tickSeconds("5_minutes_ago", 60)
    testClock.tickSeconds("TimeOfTest", 300)
    val fiveMinutesAgo = testClock["5_minutes_ago"].toGcpTimestamp()
    val sixMinutesAgo = testClock["6_minutes_ago"].toGcpTimestamp()
    val enqueuedFiveMinutesAgo = computationMutations.insertComputation(
      localId = 555,
      updateTime = fiveMinutesAgo,
      globalId = 55,
      stage = FakeProtocolStages.A,
      lockOwner = WRITE_NULL_STRING,
      lockExpirationTime = fiveMinutesAgo,
      details = COMPUTATION_DETAILS
    )
    val enqueuedFiveMinutesAgoStage = computationMutations.insertComputationStage(
      localId = 555,
      stage = FakeProtocolStages.A,
      nextAttempt = 1,
      creationTime = Instant.ofEpochMilli(3456789L).toGcpTimestamp(),
      details = computationMutations.detailsFor(FakeProtocolStages.A)
    )

    val enqueuedSixMinutesAgo = computationMutations.insertComputation(
      localId = 66,
      stage = FakeProtocolStages.A,
      updateTime = sixMinutesAgo,
      globalId = 6,
      lockOwner = WRITE_NULL_STRING,
      lockExpirationTime = sixMinutesAgo,
      details = COMPUTATION_DETAILS
    )
    val enqueuedSixMinutesAgoStage = computationMutations.insertComputationStage(
      localId = 66,
      stage = FakeProtocolStages.A,
      nextAttempt = 1,
      creationTime = Instant.ofEpochMilli(3456789L).toGcpTimestamp(),
      details = computationMutations.detailsFor(FakeProtocolStages.A)
    )
    databaseClient.write(
      listOf(
        enqueuedFiveMinutesAgo,
        enqueuedFiveMinutesAgoStage,
        enqueuedSixMinutesAgo,
        enqueuedSixMinutesAgoStage
      )
    )
    assertEquals(
      ComputationToken(
        localId = 66, globalId = 6, stage = FakeProtocolStages.A,
        owner = "the-owner-of-the-lock", nextWorker = COMPUTATION_DETAILS.outgoingNodeId,
        role = DuchyRole.PRIMARY, attempt = 1, lastUpdateTime = testClock.last().toEpochMilli()
      ),
      database.claimTask("the-owner-of-the-lock")
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT Attempt, BeginTime, EndTime, Details FROM ComputationStageAttempts
      WHERE ComputationId = 66 AND ComputationStage = 0
      """.trimIndent(),
      Struct.newBuilder()
        .set("Attempt").to(1)
        .set("BeginTime").to(testClock.last().toGcpTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(ComputationStageAttemptDetails.getDefaultInstance())
        .build()
    )

    assertEquals(
      ComputationToken(
        localId = 555, globalId = 55, stage = FakeProtocolStages.A,
        owner = "the-owner-of-the-lock", nextWorker = COMPUTATION_DETAILS.outgoingNodeId,
        role = DuchyRole.PRIMARY, attempt = 1, lastUpdateTime = testClock.last().toEpochMilli()
      ),
      database.claimTask("the-owner-of-the-lock")
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT Attempt, BeginTime, EndTime, Details FROM ComputationStageAttempts
      WHERE ComputationId = 555 AND ComputationStage = 0
      """.trimIndent(),
      Struct.newBuilder()
        .set("Attempt").to(1)
        .set("BeginTime").to(testClock.last().toGcpTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(ComputationStageAttemptDetails.getDefaultInstance())
        .build()
    )

    // No tasks to claim anymore
    assertNull(database.claimTask("the-owner-of-the-lock"))
  }

  @Test
  fun `claim locked tasks`() = runBlocking<Unit> {
    testClock.tickSeconds("5_minutes_ago", 60)
    testClock.tickSeconds("TimeOfTest", 300)
    val fiveMinutesAgo = testClock["5_minutes_ago"].toGcpTimestamp()
    val fiveMinutesFromNow = testClock.last().plusSeconds(300).toGcpTimestamp()
    val expiredClaim = computationMutations.insertComputation(
      localId = 111L,
      stage = FakeProtocolStages.A,
      updateTime = testClock["start"].toGcpTimestamp(),
      globalId = 11,
      lockOwner = "owner-of-the-lock",
      lockExpirationTime = fiveMinutesAgo,
      details = COMPUTATION_DETAILS
    )
    val expiredClaimStage = computationMutations.insertComputationStage(
      localId = 111,
      stage = FakeProtocolStages.A,
      nextAttempt = 2,
      creationTime = fiveMinutesAgo,
      details = computationMutations.detailsFor(FakeProtocolStages.A)
    )
    val expiredClaimAttempt = computationMutations.insertComputationStageAttempt(
      localId = 111,
      stage = FakeProtocolStages.A,
      attempt = 1,
      beginTime = fiveMinutesAgo,
      details = ComputationStageAttemptDetails.getDefaultInstance()
    )
    databaseClient.write(listOf(expiredClaim, expiredClaimStage, expiredClaimAttempt))

    val claimed = computationMutations.insertComputation(
      localId = 333,
      stage = FakeProtocolStages.A,
      updateTime = testClock["start"].toGcpTimestamp(),
      globalId = 33,
      lockOwner = "owner-of-the-lock",
      lockExpirationTime = fiveMinutesFromNow,
      details = COMPUTATION_DETAILS
    )
    val claimedStage = computationMutations.insertComputationStage(
      localId = 333,
      stage = FakeProtocolStages.A,
      nextAttempt = 2,
      creationTime = fiveMinutesAgo,
      details = computationMutations.detailsFor(FakeProtocolStages.A)
    )
    val claimedAttempt = computationMutations.insertComputationStageAttempt(
      localId = 333,
      stage = FakeProtocolStages.A,
      attempt = 1,
      beginTime = fiveMinutesAgo,
      details = ComputationStageAttemptDetails.getDefaultInstance()
    )
    databaseClient.write(listOf(claimed, claimedStage, claimedAttempt))

    // Claim a task that is owned but the lock expired.
    assertEquals(
      ComputationToken(
        localId = 111, globalId = 11, stage = FakeProtocolStages.A,
        owner = "new-owner", nextWorker = COMPUTATION_DETAILS.outgoingNodeId,
        role = DuchyRole.PRIMARY, attempt = 2, lastUpdateTime = testClock.last().toEpochMilli()
      ),
      database.claimTask("new-owner")
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT Attempt, BeginTime, EndTime, Details FROM ComputationStageAttempts
      WHERE ComputationId = 111 AND ComputationStage = 0
      ORDER BY Attempt
      """.trimIndent(),
      Struct.newBuilder()
        .set("Attempt").to(1)
        .set("BeginTime").to(fiveMinutesAgo)
        .set("EndTime").to(testClock.last().toGcpTimestamp())
        .set("Details").toProtoBytes(
          ComputationStageAttemptDetails.newBuilder().apply {
            reasonEnded = ComputationStageAttemptDetails.EndReason.LOCK_OVERWRITTEN
          }.build()
        )
        .build(),
      Struct.newBuilder()
        .set("Attempt").to(2)
        .set("BeginTime").to(testClock.last().toGcpTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(ComputationStageAttemptDetails.getDefaultInstance())
        .build()
    )

    // No task may be claimed at this point
    assertNull(database.claimTask("new-owner"))
  }

  @Test
  fun renewTask() = runBlocking<Unit> {
    val lastUpdated = TEST_INSTANT.minusSeconds(355)
    val token = ComputationToken(
      localId = 4315, globalId = 55, stage = FakeProtocolStages.E,
      owner = "the-owner-of-the-lock", nextWorker = COMPUTATION_DETAILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 2, lastUpdateTime = lastUpdated.toEpochMilli()
    )
    val fiveSecondsFromNow = Instant.now().plusSeconds(5).toGcpTimestamp()
    val computation = computationMutations.insertComputation(
      token.localId,
      lastUpdated.toGcpTimestamp(),
      token.globalId,
      stage = FakeProtocolStages.E,
      lockOwner = token.owner!!,
      lockExpirationTime = fiveSecondsFromNow,
      details = COMPUTATION_DETAILS
    )
    val stage = computationMutations.insertComputationStage(
      localId = token.localId,
      stage = FakeProtocolStages.E,
      nextAttempt = 3,
      creationTime = Instant.ofEpochMilli(3456789L).toGcpTimestamp(),
      details = computationMutations.detailsFor(FakeProtocolStages.E)
    )
    databaseClient.write(listOf(computation, stage))
    assertEquals(
      token.copy(lastUpdateTime = TEST_INSTANT.toEpochMilli()),
      database.renewTask(token)
    )

    assertQueryReturns(
      databaseClient,
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
  fun `renewTask for missing computation fails`() = runBlocking<Unit> {
    val token = ComputationToken(
      localId = 4315, globalId = 55, stage = FakeProtocolStages.E,
      owner = "the-owner-of-the-lock", nextWorker = COMPUTATION_DETAILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 2, lastUpdateTime = 12345L
    )
    assertFailsWith<SpannerException> { database.renewTask(token) }
  }

  @Test
  fun `renewTask for token with no owner fails`() = runBlocking<Unit> {
    val token = ComputationToken(
      localId = 4315, globalId = 55, stage = FakeProtocolStages.E,
      owner = null, nextWorker = COMPUTATION_DETAILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 2, lastUpdateTime = 12345L
    )
    assertFailsWith<IllegalStateException> { database.renewTask(token) }
  }

  private fun testTransitionOfStageWhere(
    afterTransition: AfterTransition
  ): ComputationToken<FakeProtocolStages> = runBlocking {
    testClock.tickSeconds("stage_b_created")
    testClock.tickSeconds("last_updated")
    testClock.tickSeconds("lock_expires", 100)
    val token = ComputationToken(
      localId = 4315, globalId = 55, stage = FakeProtocolStages.B,
      owner = "the-owner-of-the-lock", nextWorker = COMPUTATION_DETAILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 2,
      lastUpdateTime = testClock["last_updated"].toEpochMilli()
    )
    val computation = computationMutations.insertComputation(
      localId = token.localId,
      updateTime = testClock["last_updated"].toGcpTimestamp(),
      globalId = token.globalId,
      stage = FakeProtocolStages.B,
      lockOwner = token.owner!!,
      lockExpirationTime = testClock["lock_expires"].toGcpTimestamp(),
      details = COMPUTATION_DETAILS
    )
    val stage = computationMutations.insertComputationStage(
      localId = token.localId,
      stage = FakeProtocolStages.B,
      nextAttempt = 3,
      creationTime = testClock["stage_b_created"].toGcpTimestamp(),
      details = computationMutations.detailsFor(FakeProtocolStages.B)
    )
    val attempt =
      computationMutations.insertComputationStageAttempt(
        localId = token.localId,
        stage = FakeProtocolStages.B,
        attempt = 2,
        beginTime = testClock["stage_b_created"].toGcpTimestamp(),
        details = ComputationStageAttemptDetails.getDefaultInstance()
      )
    databaseClient.write(listOf(computation, stage, attempt))
    testClock.tickSeconds("update_stage", 100)
    assertEquals(
      database.updateComputationStage(
        token = token,
        to = FakeProtocolStages.D,
        inputBlobPaths = listOf(),
        outputBlobs = 0,
        afterTransition = afterTransition
      ),
      database.getToken(token.globalId)
    )
    // Ensure the Computation and ComputationStage were updated. This does not check if the
    // lock is held or the exact configurations of the ComputationStageAttempts because they
    // differ depending on what to do after the transition.
    assertStageTransitioned(token)
    return@runBlocking token
  }

  private fun assertStageTransitioned(token: ComputationToken<FakeProtocolStages>) {
    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, UpdateTime, GlobalComputationId
      FROM Computations
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(FakeProtocolStages.D))
        .set("UpdateTime").to(testClock["update_stage"].toGcpTimestamp())
        .set("GlobalComputationId").to(token.globalId)
        .build()
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, CreationTime, EndTime, PreviousStage, FollowingStage, 
             Details
      FROM ComputationStages
      ORDER BY ComputationStage
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(FakeProtocolStages.B))
        .set("CreationTime").to(testClock["stage_b_created"].toGcpTimestamp())
        .set("EndTime").to(testClock["update_stage"].toGcpTimestamp())
        .set("PreviousStage").to(null as Long?)
        .set("FollowingStage").to(ProtocolStages.enumToLong(FakeProtocolStages.D))
        .set("Details").toProtoBytes(computationMutations.detailsFor(FakeProtocolStages.B))
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(FakeProtocolStages.D))
        .set("CreationTime").to(testClock["update_stage"].toGcpTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .set("PreviousStage").to(ProtocolStages.enumToLong(FakeProtocolStages.B))
        .set("FollowingStage").to(null as Long?)
        .set("Details").toProtoBytes(computationMutations.detailsFor(FakeProtocolStages.D))
        .build()
    )
  }

  @Test
  fun `updateComputationStage and continue working`() = runBlocking<Unit> {
    val token = testTransitionOfStageWhere(AfterTransition.CONTINUE_WORKING)

    assertQueryReturns(
      databaseClient,
      "SELECT ComputationId, LockOwner, LockExpirationTime FROM Computations",
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("LockOwner").to(token.owner)
        .set("LockExpirationTime").to(testClock.last().plusSeconds(300).toGcpTimestamp())
        .build()
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, Attempt, BeginTime, EndTime, Details
      FROM ComputationStageAttempts
      ORDER BY ComputationStage, Attempt
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(FakeProtocolStages.B))
        .set("Attempt").to(2)
        .set("BeginTime").to(testClock["stage_b_created"].toGcpTimestamp())
        .set("EndTime").to(testClock.last().toGcpTimestamp())
        .set("Details").toProtoBytes(
          ComputationStageAttemptDetails.newBuilder().apply {
            reasonEnded = ComputationStageAttemptDetails.EndReason.SUCCEEDED
          }.build()
        )
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(FakeProtocolStages.D))
        .set("Attempt").to(1)
        .set("BeginTime").to(testClock.last().toGcpTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(ComputationStageAttemptDetails.getDefaultInstance())
        .build()
    )
  }

  @Test
  fun `updateComputationStage and add to queue`() {
    val token = testTransitionOfStageWhere(AfterTransition.ADD_UNCLAIMED_TO_QUEUE)

    assertQueryReturns(
      databaseClient,
      "SELECT ComputationId, LockOwner, LockExpirationTime FROM Computations",
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("LockOwner").to(null as String?)
        .set("LockExpirationTime").to(testClock.last().toGcpTimestamp())
        .build()
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, Attempt, BeginTime, EndTime, Details
      FROM ComputationStageAttempts
      ORDER BY ComputationStage, Attempt
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(FakeProtocolStages.B))
        .set("Attempt").to(2)
        .set("BeginTime").to(testClock["stage_b_created"].toGcpTimestamp())
        .set("EndTime").to(testClock.last().toGcpTimestamp())
        .set("Details").toProtoBytes(
          ComputationStageAttemptDetails.newBuilder().apply {
            reasonEnded = ComputationStageAttemptDetails.EndReason.SUCCEEDED
          }.build()
        )
        .build()
    )
  }

  @Test
  fun `updateComputationStage and do not add to queue`() {
    val token = testTransitionOfStageWhere(AfterTransition.DO_NOT_ADD_TO_QUEUE)

    assertQueryReturns(
      databaseClient,
      "SELECT ComputationId, LockOwner, LockExpirationTime FROM Computations",
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("LockOwner").to(null as String?)
        .set("LockExpirationTime").to(null as Timestamp?)
        .build()
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, Attempt, BeginTime, EndTime, Details
      FROM ComputationStageAttempts
      ORDER BY ComputationStage, Attempt
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(FakeProtocolStages.B))
        .set("Attempt").to(2)
        .set("BeginTime").to(testClock["stage_b_created"].toGcpTimestamp())
        .set("EndTime").to(testClock.last().toGcpTimestamp())
        .set("Details").toProtoBytes(
          ComputationStageAttemptDetails.newBuilder().apply {
            reasonEnded = ComputationStageAttemptDetails.EndReason.SUCCEEDED
          }.build()
        )
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(FakeProtocolStages.D))
        .set("Attempt").to(1)
        .set("BeginTime").to(testClock.last().toGcpTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(ComputationStageAttemptDetails.getDefaultInstance())
        .build()
    )
  }

  @Test
  fun `many stage transitions`() = runBlocking<Unit> {
    testClock.tickSeconds("insert_computation", 20)
    var token = database.insertComputation(12345, FakeProtocolStages.A)
    assertNull(database.claimTask("TestJob"))
    database.writeOutputBlobReference(token, BlobRef(0L, "/path/to/output/for/stage/A"))
    testClock.tickSeconds("move_to_B", 21)
    token = database.updateComputationStage(
      token = token,
      to = FakeProtocolStages.B,
      inputBlobPaths = listOf("/path/to/inputs/123"),
      outputBlobs = 1,
      afterTransition = AfterTransition.CONTINUE_WORKING
    )
    testClock.tickSeconds("write_output_B")
    database.writeOutputBlobReference(token, BlobRef(1L, "/path/to/output/for/stage/B"))
    testClock.tickSeconds("move_to_C", 3)
    token = database.updateComputationStage(
      token = token,
      to = FakeProtocolStages.C,
      inputBlobPaths = listOf(),
      outputBlobs = 1,
      afterTransition = AfterTransition.ADD_UNCLAIMED_TO_QUEUE
    )
    testClock.tickSeconds("write_output_C")
    database.writeOutputBlobReference(token, BlobRef(0L, "/path/to/output/for/stage/C"))
    database.enqueue(token)
    testClock.tickSeconds("claimed_in_C", 3)
    token = assertNotNull(database.claimTask("a-second-worker"))
    testClock.tickSeconds("move_to_E", 3)
    database.endComputation(
      token = token,
      endingStage = FakeProtocolStages.E,
      endComputationReason = EndComputationReason.SUCCEEDED
    )
    assertNull(database.claimTask("nothing-to-claim"))

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, UpdateTime, GlobalComputationId, LockOwner,
          LockExpirationTime
      FROM Computations
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(FakeProtocolStages.E))
        .set("UpdateTime").to(testClock.last().toGcpTimestamp())
        .set("GlobalComputationId").to(12345)
        .set("LockOwner").to(null as String?)
        .set("LockExpirationTime").to(null as Timestamp?)
        .build()
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, CreationTime, NextAttempt, EndTime, PreviousStage,
             FollowingStage, Details
      FROM ComputationStages
      ORDER BY ComputationStage
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(FakeProtocolStages.A))
        .set("CreationTime").to(testClock["insert_computation"].toGcpTimestamp())
        .set("NextAttempt").to(2)
        .set("EndTime").to(testClock["move_to_B"].toGcpTimestamp())
        .set("PreviousStage").to(null as Long?)
        .set("FollowingStage").to(ProtocolStages.enumToLong(FakeProtocolStages.B))
        .set("Details").toProtoBytes(computationMutations.detailsFor(FakeProtocolStages.A))
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(FakeProtocolStages.B))
        .set("CreationTime").to(testClock["move_to_B"].toGcpTimestamp())
        .set("NextAttempt").to(2)
        .set("EndTime").to(testClock["move_to_C"].toGcpTimestamp())
        .set("PreviousStage").to(ProtocolStages.enumToLong(FakeProtocolStages.A))
        .set("FollowingStage").to(ProtocolStages.enumToLong(FakeProtocolStages.C))
        .set("Details").toProtoBytes(computationMutations.detailsFor(FakeProtocolStages.B))
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(FakeProtocolStages.C))
        .set("CreationTime").to(testClock["move_to_C"].toGcpTimestamp())
        .set("NextAttempt").to(2)
        .set("EndTime").to(testClock["move_to_E"].toGcpTimestamp())
        .set("PreviousStage").to(ProtocolStages.enumToLong(FakeProtocolStages.B))
        .set("FollowingStage").to(ProtocolStages.enumToLong(FakeProtocolStages.E))
        .set("Details").toProtoBytes(computationMutations.detailsFor(FakeProtocolStages.C))
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(FakeProtocolStages.E))
        .set("CreationTime").to(testClock["move_to_E"].toGcpTimestamp())
        .set("NextAttempt").to(1)
        .set("EndTime").to(null as Timestamp?)
        .set("PreviousStage").to(ProtocolStages.enumToLong(FakeProtocolStages.C))
        .set("FollowingStage").to(null as Long?)
        .set("Details").toProtoBytes(computationMutations.detailsFor(FakeProtocolStages.E))
        .build()
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, BlobId, PathToBlob, DependencyType
      FROM ComputationBlobReferences
      ORDER BY ComputationStage, BlobId
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(FakeProtocolStages.A))
        .set("BlobId").to(0L)
        .set("PathToBlob").to("/path/to/output/for/stage/A")
        .set("DependencyType").to(ComputationBlobDependency.OUTPUT.ordinal.toLong())
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(FakeProtocolStages.B))
        .set("BlobId").to(0L)
        .set("PathToBlob").to("/path/to/inputs/123")
        .set("DependencyType").to(ComputationBlobDependency.INPUT.ordinal.toLong())
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(FakeProtocolStages.B))
        .set("BlobId").to(1L)
        .set("PathToBlob").to("/path/to/output/for/stage/B")
        .set("DependencyType").to(ComputationBlobDependency.OUTPUT.ordinal.toLong())
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(FakeProtocolStages.C))
        .set("BlobId").to(0L)
        .set("PathToBlob").to("/path/to/output/for/stage/C")
        .set("DependencyType").to(ComputationBlobDependency.OUTPUT.ordinal.toLong())
        .build()
    )
  }

  @Test
  fun `updateComputationStage illegal stage transition fails`() = runBlocking<Unit> {
    val token = ComputationToken(
      localId = 1, nextWorker = "", role = DuchyRole.PRIMARY,
      owner = null, attempt = 1, stage = FakeProtocolStages.A,
      globalId = 0, lastUpdateTime = 0
    )
    assertFailsWith<IllegalArgumentException> {
      database.updateComputationStage(
        token = token,
        to = FakeProtocolStages.D,
        inputBlobPaths = listOf(),
        outputBlobs = 0,
        afterTransition = AfterTransition.DO_NOT_ADD_TO_QUEUE
      )
    }
  }

  @Test
  fun readBlobReferences() = runBlocking<Unit> {
    val token = ComputationToken(
      localId = 4315, globalId = 55, stage = FakeProtocolStages.B,
      owner = null, nextWorker = COMPUTATION_DETAILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 1, lastUpdateTime = testClock.last().toEpochMilli()
    )
    val computation = computationMutations.insertComputation(
      localId = token.localId,
      updateTime = testClock.last().toGcpTimestamp(),
      stage = FakeProtocolStages.B,
      globalId = token.globalId,
      lockOwner = WRITE_NULL_STRING,
      lockExpirationTime = testClock.last().toGcpTimestamp(),
      details = COMPUTATION_DETAILS
    )
    val stage = computationMutations.insertComputationStage(
      localId = token.localId,
      stage = FakeProtocolStages.B,
      nextAttempt = 3,
      creationTime = testClock.last().toGcpTimestamp(),
      details = computationMutations.detailsFor(FakeProtocolStages.B)
    )
    val inputBlobA = computationMutations.insertComputationBlobReference(
      localId = token.localId,
      stage = FakeProtocolStages.B,
      blobId = 0,
      pathToBlob = "/path/to/blob/A",
      dependencyType = ComputationBlobDependency.INPUT
    )
    val inputBlobB = computationMutations.insertComputationBlobReference(
      localId = token.localId,
      stage = FakeProtocolStages.B,
      blobId = 1,
      pathToBlob = "/path/to/blob/B",
      dependencyType = ComputationBlobDependency.INPUT
    )
    val outputBlobC = computationMutations.insertComputationBlobReference(
      localId = token.localId,
      stage = FakeProtocolStages.B,
      blobId = 2,
      pathToBlob = "/path/to/blob/C",
      dependencyType = ComputationBlobDependency.OUTPUT
    )
    val outputBlobD = computationMutations.insertComputationBlobReference(
      localId = token.localId,
      stage = FakeProtocolStages.B,
      blobId = 3,
      dependencyType = ComputationBlobDependency.OUTPUT
    )
    databaseClient.write(listOf(computation, stage))
    databaseClient.write(listOf(inputBlobA, inputBlobB, outputBlobC, outputBlobD))
    assertEquals(
      mapOf(
        0L to "/path/to/blob/A",
        1L to "/path/to/blob/B",
        2L to "/path/to/blob/C",
        3L to null
      ),
      database.readBlobReferences(token, BlobDependencyType.ANY)
    )
    assertEquals(
      mapOf(
        2L to "/path/to/blob/C",
        3L to null
      ),
      database.readBlobReferences(token, BlobDependencyType.OUTPUT)
    )
    assertEquals(
      mapOf(
        0L to "/path/to/blob/A",
        1L to "/path/to/blob/B"
      ),
      database.readBlobReferences(token, BlobDependencyType.INPUT)
    )
  }

  @Test
  fun readStageSpecificDetails() = runBlocking<Unit> {
    val token = ComputationToken(
      globalId = 21231,
      localId = 100,
      stage = FakeProtocolStages.D,
      owner = null,
      role = DuchyRole.PRIMARY,
      nextWorker = COMPUTATION_DETAILS.outgoingNodeId,
      attempt = 1,
      lastUpdateTime = testClock.last().toEpochMilli()
    )
    val computation = computationMutations.insertComputation(
      localId = token.localId,
      stage = FakeProtocolStages.D,
      updateTime = testClock.last().toGcpTimestamp(),
      globalId = token.globalId,
      lockOwner = WRITE_NULL_STRING,
      lockExpirationTime = testClock.last().toGcpTimestamp(),
      details = COMPUTATION_DETAILS
    )
    val stage = computationMutations.insertComputationStage(
      localId = token.localId,
      stage = FakeProtocolStages.D,
      nextAttempt = 1,
      creationTime = testClock.last().toGcpTimestamp(),
      details = FakeProtocolStageDetails.getDefaultInstance()
    )
    databaseClient.write(listOf(computation, stage))
    ProtoTruth.assertThat(database.readStageSpecificDetails(token)).isEqualToDefaultInstance()

    val newDetails = FakeProtocolStageDetails.newBuilder().setName("AnotherName").build()
    val stageWithSpecificDetails = computationMutations.updateComputationStage(
      localId = token.localId,
      stage = FakeProtocolStages.D,
      details = newDetails
    )
    databaseClient.write(listOf(stageWithSpecificDetails))
    assertEquals(
      newDetails,
      database.readStageSpecificDetails(token)
    )
  }

  @Test
  fun writeOutputBlobReference() = runBlocking<Unit> {
    val token = ComputationToken(
      localId = 4315, globalId = 55, stage = FakeProtocolStages.B,
      owner = null, nextWorker = COMPUTATION_DETAILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 1, lastUpdateTime = testClock.last().toEpochMilli()
    )
    val computation = computationMutations.insertComputation(
      localId = token.localId,
      stage = FakeProtocolStages.B,
      updateTime = testClock.last().toGcpTimestamp(),
      globalId = token.globalId,
      lockOwner = WRITE_NULL_STRING,
      lockExpirationTime = WRITE_NULL_TIMESTAMP,
      details = COMPUTATION_DETAILS
    )
    val stage = computationMutations.insertComputationStage(
      localId = token.localId,
      stage = FakeProtocolStages.B,
      nextAttempt = 3,
      creationTime = testClock.last().toGcpTimestamp(),
      details = FakeProtocolStageDetails.getDefaultInstance()
    )
    val outputRef = computationMutations.insertComputationBlobReference(
      localId = token.localId,
      stage = FakeProtocolStages.B,
      blobId = 1234L,
      dependencyType = ComputationBlobDependency.OUTPUT
    )
    val inputRef = computationMutations.insertComputationBlobReference(
      localId = token.localId,
      stage = FakeProtocolStages.B,
      blobId = 5678L,
      pathToBlob = "/path/to/input/blob",
      dependencyType = ComputationBlobDependency.INPUT
    )
    databaseClient.write(listOf(computation, stage, outputRef, inputRef))
    database.writeOutputBlobReference(token, BlobRef(1234L, "/wrote/something/there"))
    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, BlobId, PathToBlob, DependencyType
      FROM ComputationBlobReferences
      ORDER BY ComputationStage, BlobId
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(token.stage))
        .set("BlobId").to(1234L)
        .set("PathToBlob").to("/wrote/something/there")
        .set("DependencyType").toProtoEnum(ComputationBlobDependency.OUTPUT)
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(token.stage))
        .set("BlobId").to(5678L)
        .set("PathToBlob").to("/path/to/input/blob")
        .set("DependencyType").toProtoEnum(ComputationBlobDependency.INPUT)
        .build()
    )

    // Can't update a blob with blank path.
    assertFailsWith<IllegalArgumentException> {
      database.writeOutputBlobReference(token, BlobRef(1234L, ""))
    }
    // Can't update an input blob
    assertFailsWith<SpannerException> {
      database.writeOutputBlobReference(token, BlobRef(5678L, "/wrote/something/there"))
    }
    // Blob id doesn't exist
    assertFailsWith<SpannerException> {
      database.writeOutputBlobReference(token, BlobRef(223344L, "/wrote/something/there"))
    }
  }

  @Test
  fun `end successful computation`() = runBlocking<Unit> {
    val token = ComputationToken(
      localId = 4315, globalId = 55, stage = FakeProtocolStages.C,
      owner = null, nextWorker = COMPUTATION_DETAILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 1, lastUpdateTime = testClock.last().toEpochMilli()
    )
    val computation = computationMutations.insertComputation(
      localId = token.localId,
      updateTime = testClock.last().toGcpTimestamp(),
      stage = FakeProtocolStages.C,
      globalId = token.globalId,
      lockOwner = WRITE_NULL_STRING,
      lockExpirationTime = testClock.last().toGcpTimestamp(),
      details = COMPUTATION_DETAILS
    )
    val stage = computationMutations.insertComputationStage(
      localId = token.localId,
      stage = FakeProtocolStages.C,
      nextAttempt = 3,
      creationTime = testClock.last().toGcpTimestamp(),
      details = computationMutations.detailsFor(FakeProtocolStages.C)
    )
    testClock.tickSeconds("time-ended")
    databaseClient.write(listOf(computation, stage))
    val expectedDetails =
      COMPUTATION_DETAILS.toBuilder().setEndingState(ComputationDetails.CompletedReason.SUCCEEDED)
        .build()
    database.endComputation(token, FakeProtocolStages.E, EndComputationReason.SUCCEEDED)
    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, UpdateTime, GlobalComputationId, LockOwner, 
             LockExpirationTime, ComputationDetails, ComputationDetailsJSON
      FROM Computations
      ORDER BY ComputationId DESC
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(FakeProtocolStages.E.ordinal.toLong())
        .set("UpdateTime").to(testClock.last().toGcpTimestamp())
        .set("GlobalComputationId").to(token.globalId)
        .set("LockOwner").to(null as String?)
        .set("LockExpirationTime").to(null as Timestamp?)
        .set("ComputationDetails").toProtoBytes(expectedDetails)
        .set("ComputationDetailsJSON").toProtoJson(expectedDetails)
        .build()
    )
  }

  @Test
  fun `end failed computation`() = runBlocking<Unit> {
    val token = ComputationToken(
      localId = 4315, globalId = 55, stage = FakeProtocolStages.C,
      owner = "owner-of-lock", nextWorker = COMPUTATION_DETAILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 1, lastUpdateTime = testClock.last().toEpochMilli()
    )
    val computation = computationMutations.insertComputation(
      localId = token.localId,
      updateTime = testClock.last().toGcpTimestamp(),
      stage = FakeProtocolStages.C,
      globalId = token.globalId,
      lockOwner = token.owner!!,
      lockExpirationTime = testClock.last().toGcpTimestamp(),
      details = COMPUTATION_DETAILS
    )
    val stage = computationMutations.insertComputationStage(
      localId = token.localId,
      stage = FakeProtocolStages.C,
      nextAttempt = 3,
      creationTime = testClock.last().toGcpTimestamp(),
      details = computationMutations.detailsFor(FakeProtocolStages.C)
    )
    val attempt = computationMutations.insertComputationStageAttempt(
      localId = token.localId,
      stage = FakeProtocolStages.C,
      attempt = 2,
      beginTime = testClock.last().toGcpTimestamp(),
      details = ComputationStageAttemptDetails.getDefaultInstance()
    )
    testClock.tickSeconds("time-failed")
    databaseClient.write(listOf(computation, stage, attempt))
    val expectedDetails =
      COMPUTATION_DETAILS.toBuilder().setEndingState(ComputationDetails.CompletedReason.FAILED)
        .build()
    database.endComputation(token, FakeProtocolStages.E, EndComputationReason.FAILED)
    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, UpdateTime, GlobalComputationId, LockOwner, 
             LockExpirationTime, ComputationDetails, ComputationDetailsJSON
      FROM Computations
      ORDER BY ComputationId DESC
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(FakeProtocolStages.E.ordinal.toLong())
        .set("UpdateTime").to(testClock.last().toGcpTimestamp())
        .set("GlobalComputationId").to(token.globalId)
        .set("LockOwner").to(null as String?)
        .set("LockExpirationTime").to(null as Timestamp?)
        .set("ComputationDetails").toProtoBytes(expectedDetails)
        .set("ComputationDetailsJSON").toProtoJson(expectedDetails)
        .build()
    )
    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, Attempt, BeginTime, EndTime, Details
      FROM ComputationStageAttempts
      ORDER BY ComputationStage, Attempt
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(FakeProtocolStages.C))
        .set("Attempt").to(2)
        .set("BeginTime").to(testClock["start"].toGcpTimestamp())
        .set("EndTime").to(testClock.last().toGcpTimestamp())
        .set("Details").toProtoBytes(
          ComputationStageAttemptDetails.newBuilder().apply {
            reasonEnded = ComputationStageAttemptDetails.EndReason.CANCELLED
          }.build()
        )
        .build()
    )
  }

  @Test
  fun `endComputation throws for non-ending state`() = runBlocking<Unit> {
    val token = ComputationToken(
      localId = 4315, globalId = 55, stage = FakeProtocolStages.C,
      owner = "owner-of-lock", nextWorker = COMPUTATION_DETAILS.outgoingNodeId,
      role = DuchyRole.PRIMARY, attempt = 1, lastUpdateTime = testClock.last().toEpochMilli()
    )
    assertFailsWith(IllegalArgumentException::class, "Invalid initial stage") {
      database.endComputation(token, FakeProtocolStages.B, EndComputationReason.CANCELED)
    }
  }
}
