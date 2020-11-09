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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation

import com.google.cloud.Timestamp
import com.google.cloud.spanner.SpannerException
import com.google.cloud.spanner.Struct
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.Duchy
import org.wfanet.measurement.common.DuchyOrder
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.duchy.db.computation.AfterTransition
import org.wfanet.measurement.duchy.db.computation.BlobRef
import org.wfanet.measurement.duchy.db.computation.ComputationStatMetric
import org.wfanet.measurement.duchy.db.computation.ComputationStorageEditToken
import org.wfanet.measurement.duchy.db.computation.EndComputationReason
import org.wfanet.measurement.duchy.db.computation.ProtocolStageDetails
import org.wfanet.measurement.duchy.db.computation.ProtocolStageEnumHelper
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.FakeProtocolStages.A
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.FakeProtocolStages.B
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.FakeProtocolStages.C
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.FakeProtocolStages.D
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.FakeProtocolStages.E
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.testing.COMPUTATIONS_SCHEMA
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.testing.UsingSpannerEmulator
import org.wfanet.measurement.gcloud.spanner.testing.assertQueryReturns
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.gcloud.spanner.toProtoJson
import org.wfanet.measurement.internal.db.gcp.FakeProtocolStageDetails
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationDetails
import org.wfanet.measurement.internal.duchy.ComputationStageAttemptDetails

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
  override val validInitialStages = setOf(A)
  override val validTerminalStages = setOf(E)
  override val validSuccessors = mapOf(
    A to setOf(B),
    B to setOf(C, D)
  )

  override fun enumToLong(value: FakeProtocolStages): Long {
    return value.ordinal.toLong()
  }

  override fun longToEnum(value: Long): FakeProtocolStages {
    return when (value) {
      0L -> A
      1L -> B
      2L -> C
      3L -> D
      4L -> E
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
class GcpSpannerComputationsDbTest : UsingSpannerEmulator(COMPUTATIONS_SCHEMA) {

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
    val idGenerator = GlobalBitsPlusTimeStampIdGenerator(testClock)
    val globalId1 = "12345"
    val localId1 = idGenerator.localId(globalId1)
    database.insertComputation(globalId1, A, computationMutations.detailsFor(A))
    val globalId2 = "5678"
    val localId2 = idGenerator.localId(globalId2)
    database.insertComputation(globalId2, A, computationMutations.detailsFor(A))

    val expectedDetails123 = ComputationDetails.newBuilder().apply {
      role = ComputationDetails.RoleInComputation.PRIMARY
      incomingNodeId = "SALZBURG"
      outgoingNodeId = "BOHEMIA"
      primaryNodeId = "AUSTRIA"
      blobsStoragePrefix = "mill-computation-stage-storage/$localId1"
    }.build()

    val expectedDetails220 = ComputationDetails.newBuilder().apply {
      role = ComputationDetails.RoleInComputation.SECONDARY
      incomingNodeId = "SALZBURG"
      outgoingNodeId = "BOHEMIA"
      primaryNodeId = "SALZBURG"
      blobsStoragePrefix = "mill-computation-stage-storage/$localId2"
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
        .set("ComputationId").to(localId1)
        .set("ComputationStage").to(A.ordinal.toLong())
        .set("UpdateTime").to(TEST_INSTANT.toGcloudTimestamp())
        .set("GlobalComputationId").to(globalId1)
        .set("LockOwner").to(null as String?)
        .set("LockExpirationTime").to(TEST_INSTANT.toGcloudTimestamp())
        .set("ComputationDetails").toProtoBytes(expectedDetails123)
        .set("ComputationDetailsJSON").toProtoJson(expectedDetails123)
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(localId2)
        .set("ComputationStage").to(A.ordinal.toLong())
        .set("UpdateTime").to(TEST_INSTANT.toGcloudTimestamp())
        .set("GlobalComputationId").to(globalId2)
        .set("LockOwner").to(null as String?)
        .set("LockExpirationTime").to(TEST_INSTANT.toGcloudTimestamp())
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
        .set("ComputationId").to(localId1)
        .set("ComputationStage").to(A.ordinal.toLong())
        .set("CreationTime").to(TEST_INSTANT.toGcloudTimestamp())
        .set("NextAttempt").to(1L)
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(computationMutations.detailsFor(A))
        .set("DetailsJSON").toProtoJson(computationMutations.detailsFor(A))
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(localId2)
        .set("ComputationStage").to(A.ordinal.toLong())
        .set("CreationTime").to(TEST_INSTANT.toGcloudTimestamp())
        .set("NextAttempt").to(1L)
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(computationMutations.detailsFor(A))
        .set("DetailsJSON").toProtoJson(computationMutations.detailsFor(A))
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
        .set("ComputationId").to(localId1)
        .set("N").to(1)
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(localId2)
        .set("N").to(1)
        .build()
    )
  }

  @Test
  fun `insert computations with same global id fails`() = runBlocking<Unit> {
    database.insertComputation("123", A, computationMutations.detailsFor(A))
    // This one fails because the same local id is used.
    assertFailsWith(SpannerException::class, "ALREADY_EXISTS") {
      database.insertComputation("123", A, computationMutations.detailsFor(A))
    }
  }

  @Test
  fun `insert computation with bad initial stage fails`() = runBlocking<Unit> {
    assertFailsWith(IllegalArgumentException::class, "Invalid initial stage") {
      database.insertComputation("123", B, computationMutations.detailsFor(B))
    }
    assertFailsWith(IllegalArgumentException::class, "Invalid initial stage") {
      database.insertComputation("123", C, computationMutations.detailsFor(C))
    }
    assertFailsWith(IllegalArgumentException::class, "Invalid initial stage") {
      database.insertComputation("123", D, computationMutations.detailsFor(D))
    }
    assertFailsWith(IllegalArgumentException::class, "Invalid initial stage") {
      database.insertComputation("123", E, computationMutations.detailsFor(E))
    }
  }

  @Test
  fun enqueue() = runBlocking<Unit> {
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val lockExpires = Instant.now().plusSeconds(300)
    val token = ComputationStorageEditToken(
      localId = 1,
      stage = C,
      attempt = 1,
      editVersion = lastUpdated.toEpochMilli()
    )

    val computation = computationMutations.insertComputation(
      localId = token.localId,
      updateTime = lastUpdated.toGcloudTimestamp(),
      globalId = "0",
      stage = token.stage,
      lockOwner = "PeterSpacemen",
      lockExpirationTime = lockExpires.toGcloudTimestamp(),
      details = COMPUTATION_DETAILS
    )
    val differentComputation = computationMutations.insertComputation(
      localId = 456789,
      updateTime = lastUpdated.toGcloudTimestamp(),
      globalId = "10111213",
      stage = B,
      lockOwner = "PeterSpacemen",
      lockExpirationTime = lockExpires.toGcloudTimestamp(),
      details = COMPUTATION_DETAILS
    )

    databaseClient.write(listOf(computation, differentComputation))
    database.enqueue(token, 2)

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, LockOwner, LockExpirationTime,
             ComputationDetails, ComputationDetailsJSON
      FROM Computations
      ORDER BY ComputationId
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(token.stage))
        .set("LockOwner").to(null as String?)
        .set("LockExpirationTime").to(TEST_INSTANT.plusSeconds(2).toGcloudTimestamp())
        .set("ComputationDetails").toProtoBytes(COMPUTATION_DETAILS)
        .set("ComputationDetailsJSON").toProtoJson(COMPUTATION_DETAILS)
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(456789)
        .set("ComputationStage").to(1)
        .set("LockOwner").to("PeterSpacemen")
        .set("LockExpirationTime").to(lockExpires.toGcloudTimestamp())
        .set("ComputationDetails").toProtoBytes(COMPUTATION_DETAILS)
        .set("ComputationDetailsJSON").toProtoJson(COMPUTATION_DETAILS)
        .build()
    )
  }

  @Test
  fun `enqueue deteleted computation fails`() = runBlocking<Unit> {
    val token = ComputationStorageEditToken(
      localId = 1,
      stage = C,
      attempt = 1,
      editVersion = 0
    )
    assertFailsWith<SpannerException> { database.enqueue(token, 0) }
  }

  @Test
  fun `enqueue with old token fails`() = runBlocking<Unit> {
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val lockExpires = lastUpdated.plusSeconds(1)
    val token = ComputationStorageEditToken(
      localId = 1,
      stage = C,
      attempt = 1,
      editVersion = lastUpdated.minusSeconds(200).toEpochMilli()
    )

    val computation = computationMutations.insertComputation(
      localId = token.localId,
      updateTime = lastUpdated.toGcloudTimestamp(),
      globalId = "1234",
      stage = token.stage,
      lockOwner = "AnOwnedLock",
      lockExpirationTime = lockExpires.toGcloudTimestamp(),
      details = COMPUTATION_DETAILS
    )
    databaseClient.write(listOf(computation))
    assertFailsWith<SpannerException> { database.enqueue(token, 0) }
  }

  @Test
  fun claimTask() = runBlocking<Unit> {
    testClock.tickSeconds("6_minutes_ago")
    testClock.tickSeconds("5_minutes_ago", 60)
    testClock.tickSeconds("TimeOfTest", 300)
    val fiveMinutesAgo = testClock["5_minutes_ago"].toGcloudTimestamp()
    val sixMinutesAgo = testClock["6_minutes_ago"].toGcloudTimestamp()
    val enqueuedFiveMinutesAgo = computationMutations.insertComputation(
      localId = 555,
      updateTime = fiveMinutesAgo,
      globalId = "55",
      stage = A,
      lockOwner = WRITE_NULL_STRING,
      lockExpirationTime = fiveMinutesAgo,
      details = COMPUTATION_DETAILS
    )
    val enqueuedFiveMinutesAgoStage = computationMutations.insertComputationStage(
      localId = 555,
      stage = A,
      nextAttempt = 1,
      creationTime = Instant.ofEpochMilli(3456789L).toGcloudTimestamp(),
      details = computationMutations.detailsFor(A)
    )

    val enqueuedSixMinutesAgo = computationMutations.insertComputation(
      localId = 66,
      stage = A,
      updateTime = sixMinutesAgo,
      globalId = "6",
      lockOwner = WRITE_NULL_STRING,
      lockExpirationTime = sixMinutesAgo,
      details = COMPUTATION_DETAILS
    )
    val enqueuedSixMinutesAgoStage = computationMutations.insertComputationStage(
      localId = 66,
      stage = A,
      nextAttempt = 1,
      creationTime = Instant.ofEpochMilli(3456789L).toGcloudTimestamp(),
      details = computationMutations.detailsFor(A)
    )
    databaseClient.write(
      listOf(
        enqueuedFiveMinutesAgo,
        enqueuedFiveMinutesAgoStage,
        enqueuedSixMinutesAgo,
        enqueuedSixMinutesAgoStage
      )
    )
    assertEquals("6", database.claimTask("the-owner-of-the-lock"))

    assertQueryReturns(
      databaseClient,
      """
      SELECT Attempt, BeginTime, EndTime, Details FROM ComputationStageAttempts
      WHERE ComputationId = 66 AND ComputationStage = 0
      """.trimIndent(),
      Struct.newBuilder()
        .set("Attempt").to(1)
        .set("BeginTime").to(testClock.last().toGcloudTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(ComputationStageAttemptDetails.getDefaultInstance())
        .build()
    )

    assertEquals("55", database.claimTask("the-owner-of-the-lock"))

    assertQueryReturns(
      databaseClient,
      """
      SELECT Attempt, BeginTime, EndTime, Details FROM ComputationStageAttempts
      WHERE ComputationId = 555 AND ComputationStage = 0
      """.trimIndent(),
      Struct.newBuilder()
        .set("Attempt").to(1)
        .set("BeginTime").to(testClock.last().toGcloudTimestamp())
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
    val fiveMinutesAgo = testClock["5_minutes_ago"].toGcloudTimestamp()
    val fiveMinutesFromNow = testClock.last().plusSeconds(300).toGcloudTimestamp()
    val expiredClaim = computationMutations.insertComputation(
      localId = 111L,
      stage = A,
      updateTime = testClock["start"].toGcloudTimestamp(),
      globalId = "11",
      lockOwner = "owner-of-the-lock",
      lockExpirationTime = fiveMinutesAgo,
      details = COMPUTATION_DETAILS
    )
    val expiredClaimStage = computationMutations.insertComputationStage(
      localId = 111,
      stage = A,
      nextAttempt = 2,
      creationTime = fiveMinutesAgo,
      details = computationMutations.detailsFor(A)
    )
    val expiredClaimAttempt = computationMutations.insertComputationStageAttempt(
      localId = 111,
      stage = A,
      attempt = 1,
      beginTime = fiveMinutesAgo,
      details = ComputationStageAttemptDetails.getDefaultInstance()
    )
    databaseClient.write(listOf(expiredClaim, expiredClaimStage, expiredClaimAttempt))

    val claimed = computationMutations.insertComputation(
      localId = 333,
      stage = A,
      updateTime = testClock["start"].toGcloudTimestamp(),
      globalId = "33",
      lockOwner = "owner-of-the-lock",
      lockExpirationTime = fiveMinutesFromNow,
      details = COMPUTATION_DETAILS
    )
    val claimedStage = computationMutations.insertComputationStage(
      localId = 333,
      stage = A,
      nextAttempt = 2,
      creationTime = fiveMinutesAgo,
      details = computationMutations.detailsFor(A)
    )
    val claimedAttempt = computationMutations.insertComputationStageAttempt(
      localId = 333,
      stage = A,
      attempt = 1,
      beginTime = fiveMinutesAgo,
      details = ComputationStageAttemptDetails.getDefaultInstance()
    )
    databaseClient.write(listOf(claimed, claimedStage, claimedAttempt))

    // Claim a task that is owned but the lock expired.
    assertEquals("11", database.claimTask("new-owner"))

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
        .set("EndTime").to(testClock.last().toGcloudTimestamp())
        .set("Details").toProtoBytes(
          ComputationStageAttemptDetails.newBuilder().apply {
            reasonEnded = ComputationStageAttemptDetails.EndReason.LOCK_OVERWRITTEN
          }.build()
        )
        .build(),
      Struct.newBuilder()
        .set("Attempt").to(2)
        .set("BeginTime").to(testClock.last().toGcloudTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(ComputationStageAttemptDetails.getDefaultInstance())
        .build()
    )

    // No task may be claimed at this point
    assertNull(database.claimTask("new-owner"))
  }

  private fun testTransitionOfStageWhere(
    afterTransition: AfterTransition
  ): ComputationStorageEditToken<FakeProtocolStages> = runBlocking {
    testClock.tickSeconds("stage_b_created")
    testClock.tickSeconds("last_updated")
    testClock.tickSeconds("lock_expires", 100)
    val globalId = "55"
    val token = ComputationStorageEditToken(
      localId = 4315,
      stage = B,
      attempt = 2,
      editVersion = testClock["last_updated"].toEpochMilli()
    )
    val computation = computationMutations.insertComputation(
      localId = token.localId,
      updateTime = testClock["last_updated"].toGcloudTimestamp(),
      globalId = globalId,
      stage = B,
      lockOwner = "the-owner-of-the-lock",
      lockExpirationTime = testClock["lock_expires"].toGcloudTimestamp(),
      details = COMPUTATION_DETAILS
    )
    val stage = computationMutations.insertComputationStage(
      localId = token.localId,
      stage = B,
      nextAttempt = 3,
      creationTime = testClock["stage_b_created"].toGcloudTimestamp(),
      details = computationMutations.detailsFor(B)
    )
    val attempt =
      computationMutations.insertComputationStageAttempt(
        localId = token.localId,
        stage = B,
        attempt = 2,
        beginTime = testClock["stage_b_created"].toGcloudTimestamp(),
        details = ComputationStageAttemptDetails.getDefaultInstance()
      )
    databaseClient.write(listOf(computation, stage, attempt))
    testClock.tickSeconds("update_stage", 100)
    database.updateComputationStage(
      token = token,
      nextStage = D,
      inputBlobPaths = listOf(),
      outputBlobs = 0,
      afterTransition = afterTransition,
      nextStageDetails = computationMutations.detailsFor(D)
    )
    // Ensure the Computation and ComputationStage were updated. This does not check if the
    // lock is held or the exact configurations of the ComputationStageAttempts because they
    // differ depending on what to do after the transition.
    assertStageTransitioned(token)
    return@runBlocking token
  }

  private suspend fun assertStageTransitioned(
    token: ComputationStorageEditToken<FakeProtocolStages>
  ) {
    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, UpdateTime
      FROM Computations
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(D))
        .set("UpdateTime").to(testClock["update_stage"].toGcloudTimestamp())
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
        .set("ComputationStage").to(ProtocolStages.enumToLong(B))
        .set("CreationTime").to(testClock["stage_b_created"].toGcloudTimestamp())
        .set("EndTime").to(testClock["update_stage"].toGcloudTimestamp())
        .set("PreviousStage").to(null as Long?)
        .set("FollowingStage").to(ProtocolStages.enumToLong(D))
        .set("Details").toProtoBytes(computationMutations.detailsFor(B))
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(D))
        .set("CreationTime").to(testClock["update_stage"].toGcloudTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .set("PreviousStage").to(ProtocolStages.enumToLong(B))
        .set("FollowingStage").to(null as Long?)
        .set("Details").toProtoBytes(computationMutations.detailsFor(D))
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
        .set("LockOwner").to("the-owner-of-the-lock")
        .set("LockExpirationTime").to(testClock.last().plusSeconds(300).toGcloudTimestamp())
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
        .set("ComputationStage").to(ProtocolStages.enumToLong(B))
        .set("Attempt").to(2)
        .set("BeginTime").to(testClock["stage_b_created"].toGcloudTimestamp())
        .set("EndTime").to(testClock.last().toGcloudTimestamp())
        .set("Details").toProtoBytes(
          ComputationStageAttemptDetails.newBuilder().apply {
            reasonEnded = ComputationStageAttemptDetails.EndReason.SUCCEEDED
          }.build()
        )
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(D))
        .set("Attempt").to(1)
        .set("BeginTime").to(testClock.last().toGcloudTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(ComputationStageAttemptDetails.getDefaultInstance())
        .build()
    )
  }

  @Test
  fun `updateComputationStage and add to queue`() = runBlocking {
    val token = testTransitionOfStageWhere(AfterTransition.ADD_UNCLAIMED_TO_QUEUE)

    assertQueryReturns(
      databaseClient,
      "SELECT ComputationId, LockOwner, LockExpirationTime FROM Computations",
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("LockOwner").to(null as String?)
        .set("LockExpirationTime").to(testClock.last().toGcloudTimestamp())
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
        .set("ComputationStage").to(ProtocolStages.enumToLong(B))
        .set("Attempt").to(2)
        .set("BeginTime").to(testClock["stage_b_created"].toGcloudTimestamp())
        .set("EndTime").to(testClock.last().toGcloudTimestamp())
        .set("Details").toProtoBytes(
          ComputationStageAttemptDetails.newBuilder().apply {
            reasonEnded = ComputationStageAttemptDetails.EndReason.SUCCEEDED
          }.build()
        )
        .build()
    )
  }

  @Test
  fun `updateComputationStage and do not add to queue`() = runBlocking {
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
        .set("ComputationStage").to(ProtocolStages.enumToLong(B))
        .set("Attempt").to(2)
        .set("BeginTime").to(testClock["stage_b_created"].toGcloudTimestamp())
        .set("EndTime").to(testClock.last().toGcloudTimestamp())
        .set("Details").toProtoBytes(
          ComputationStageAttemptDetails.newBuilder().apply {
            reasonEnded = ComputationStageAttemptDetails.EndReason.SUCCEEDED
          }.build()
        )
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(D))
        .set("Attempt").to(1)
        .set("BeginTime").to(testClock.last().toGcloudTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(ComputationStageAttemptDetails.getDefaultInstance())
        .build()
    )
  }

  @Test
  fun `updateComputationStage illegal stage transition fails`() = runBlocking<Unit> {
    val token = ComputationStorageEditToken(
      localId = 1,
      attempt = 1,
      stage = A,
      editVersion = 0
    )
    assertFailsWith<IllegalArgumentException> {
      database.updateComputationStage(
        token = token,
        nextStage = D,
        inputBlobPaths = listOf(),
        outputBlobs = 0,
        afterTransition = AfterTransition.DO_NOT_ADD_TO_QUEUE,
        nextStageDetails = computationMutations.detailsFor(D)
      )
    }
  }

  @Test
  fun writeOutputBlobReference() = runBlocking<Unit> {
    val token = ComputationStorageEditToken(
      localId = 4315,
      stage = B,
      attempt = 1,
      editVersion = testClock.last().toEpochMilli()
    )
    val computation = computationMutations.insertComputation(
      localId = token.localId,
      stage = B,
      updateTime = testClock.last().toGcloudTimestamp(),
      globalId = "2002",
      lockOwner = WRITE_NULL_STRING,
      lockExpirationTime = WRITE_NULL_TIMESTAMP,
      details = COMPUTATION_DETAILS
    )
    val stage = computationMutations.insertComputationStage(
      localId = token.localId,
      stage = B,
      nextAttempt = 3,
      creationTime = testClock.last().toGcloudTimestamp(),
      details = FakeProtocolStageDetails.getDefaultInstance()
    )
    val outputRef = computationMutations.insertComputationBlobReference(
      localId = token.localId,
      stage = B,
      blobId = 1234L,
      dependencyType = ComputationBlobDependency.OUTPUT
    )
    val inputRef = computationMutations.insertComputationBlobReference(
      localId = token.localId,
      stage = B,
      blobId = 5678L,
      pathToBlob = "/path/to/input/blob",
      dependencyType = ComputationBlobDependency.INPUT
    )
    databaseClient.write(listOf(computation, stage, outputRef, inputRef))
    testClock.tickSeconds("write-blob-ref")
    database.writeOutputBlobReference(token, BlobRef(1234L, "/wrote/something/there"))
    assertQueryReturns(
      databaseClient,
      """
      SELECT b.ComputationId, b.ComputationStage, b.BlobId, b.PathToBlob, b.DependencyType,
             c.UpdateTime
      FROM ComputationBlobReferences AS b
      JOIN Computations AS c USING(ComputationId)
      ORDER BY ComputationStage, BlobId
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(token.stage))
        .set("BlobId").to(1234L)
        .set("PathToBlob").to("/wrote/something/there")
        .set("DependencyType").toProtoEnum(ComputationBlobDependency.OUTPUT)
        .set("UpdateTime").to(testClock.get("write-blob-ref").toGcloudTimestamp())
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").to(ProtocolStages.enumToLong(token.stage))
        .set("BlobId").to(5678L)
        .set("PathToBlob").to("/path/to/input/blob")
        .set("DependencyType").toProtoEnum(ComputationBlobDependency.INPUT)
        .set("UpdateTime").to(testClock.get("write-blob-ref").toGcloudTimestamp())
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
    val token = ComputationStorageEditToken(
      localId = 4315,
      stage = C,
      attempt = 1,
      editVersion = testClock.last().toEpochMilli()
    )
    val computation = computationMutations.insertComputation(
      localId = token.localId,
      updateTime = testClock.last().toGcloudTimestamp(),
      stage = C,
      globalId = "55",
      lockOwner = WRITE_NULL_STRING,
      lockExpirationTime = testClock.last().toGcloudTimestamp(),
      details = COMPUTATION_DETAILS
    )
    val stage = computationMutations.insertComputationStage(
      localId = token.localId,
      stage = C,
      nextAttempt = 3,
      creationTime = testClock.last().toGcloudTimestamp(),
      details = computationMutations.detailsFor(C)
    )
    testClock.tickSeconds("time-ended")
    databaseClient.write(listOf(computation, stage))
    val expectedDetails =
      COMPUTATION_DETAILS.toBuilder().setEndingState(ComputationDetails.CompletedReason.SUCCEEDED)
        .build()
    database.endComputation(token, E, EndComputationReason.SUCCEEDED)
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
        .set("ComputationStage").to(E.ordinal.toLong())
        .set("UpdateTime").to(testClock.last().toGcloudTimestamp())
        .set("GlobalComputationId").to("55")
        .set("LockOwner").to(null as String?)
        .set("LockExpirationTime").to(null as Timestamp?)
        .set("ComputationDetails").toProtoBytes(expectedDetails)
        .set("ComputationDetailsJSON").toProtoJson(expectedDetails)
        .build()
    )
  }

  @Test
  fun `end failed computation`() = runBlocking<Unit> {
    val globalId = "474747"
    val token = ComputationStorageEditToken(
      localId = 4315,
      stage = C,
      attempt = 1,
      editVersion = testClock.last().toEpochMilli()
    )
    val computation = computationMutations.insertComputation(
      localId = token.localId,
      updateTime = testClock.last().toGcloudTimestamp(),
      stage = C,
      globalId = globalId,
      lockOwner = "lock-owner",
      lockExpirationTime = testClock.last().toGcloudTimestamp(),
      details = COMPUTATION_DETAILS
    )
    val stage = computationMutations.insertComputationStage(
      localId = token.localId,
      stage = C,
      nextAttempt = 3,
      creationTime = testClock.last().toGcloudTimestamp(),
      details = computationMutations.detailsFor(C)
    )
    val attempt = computationMutations.insertComputationStageAttempt(
      localId = token.localId,
      stage = C,
      attempt = 2,
      beginTime = testClock.last().toGcloudTimestamp(),
      details = ComputationStageAttemptDetails.getDefaultInstance()
    )
    testClock.tickSeconds("time-failed")
    databaseClient.write(listOf(computation, stage, attempt))
    val expectedDetails =
      COMPUTATION_DETAILS.toBuilder().setEndingState(ComputationDetails.CompletedReason.FAILED)
        .build()
    database.endComputation(token, E, EndComputationReason.FAILED)
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
        .set("ComputationStage").to(E.ordinal.toLong())
        .set("UpdateTime").to(testClock.last().toGcloudTimestamp())
        .set("GlobalComputationId").to(globalId)
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
        .set("ComputationStage").to(ProtocolStages.enumToLong(C))
        .set("Attempt").to(2)
        .set("BeginTime").to(testClock["start"].toGcloudTimestamp())
        .set("EndTime").to(testClock.last().toGcloudTimestamp())
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
    val token = ComputationStorageEditToken(
      localId = 4315,
      stage = C,
      attempt = 1,
      editVersion = testClock.last().toEpochMilli()
    )
    assertFailsWith(IllegalArgumentException::class, "Invalid initial stage") {
      database.endComputation(token, B, EndComputationReason.CANCELED)
    }
  }

  @Test
  fun `insert computation stat succeeds`() = runBlocking<Unit> {
    val globalId = "474747"
    val localId = 4315L
    val computation = computationMutations.insertComputation(
      localId = localId,
      updateTime = testClock.last().toGcloudTimestamp(),
      stage = C,
      globalId = globalId,
      lockOwner = "lock-owner",
      lockExpirationTime = testClock.last().toGcloudTimestamp(),
      details = COMPUTATION_DETAILS
    )
    val stage = computationMutations.insertComputationStage(
      localId = localId,
      stage = C,
      nextAttempt = 3,
      creationTime = testClock.last().toGcloudTimestamp(),
      details = computationMutations.detailsFor(C)
    )
    val attempt = computationMutations.insertComputationStageAttempt(
      localId = localId,
      stage = C,
      attempt = 2,
      beginTime = testClock.last().toGcloudTimestamp(),
      details = ComputationStageAttemptDetails.getDefaultInstance()
    )
    testClock.tickSeconds("time-failed")
    databaseClient.write(listOf(computation, stage, attempt))
    database.insertComputationStat(
      localId = localId,
      stage = C,
      attempt = 2,
      metric = ComputationStatMetric("crypto_cpu_time_millis", 3125)
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, Attempt, MetricName, MetricValue
      FROM ComputationStats
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(localId)
        .set("ComputationStage").to(C.ordinal.toLong())
        .set("Attempt").to(2)
        .set("MetricName").to("crypto_cpu_time_millis")
        .set("MetricValue").to(3125)
        .build()
    )
  }
}
