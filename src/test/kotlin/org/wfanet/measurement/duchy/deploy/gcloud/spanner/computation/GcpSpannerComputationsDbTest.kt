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

import com.google.cloud.Timestamp
import com.google.cloud.spanner.SpannerException
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.ValueBinder
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.duchy.db.computation.AfterTransition
import org.wfanet.measurement.duchy.db.computation.BlobRef
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetailsHelper
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationStageLongValues
import org.wfanet.measurement.duchy.db.computation.ComputationStatMetric
import org.wfanet.measurement.duchy.db.computation.ComputationStorageEditToken
import org.wfanet.measurement.duchy.db.computation.ComputationTypeEnumHelper
import org.wfanet.measurement.duchy.db.computation.EndComputationReason
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.FakeProtocolStages.A
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.FakeProtocolStages.B
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.FakeProtocolStages.C
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.FakeProtocolStages.D
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.FakeProtocolStages.E
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.FakeProtocolStages.X
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.FakeProtocolStages.Y
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation.FakeProtocolStages.Z
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.testing.COMPUTATIONS_SCHEMA
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.testing.UsingSpannerEmulator
import org.wfanet.measurement.gcloud.spanner.testing.assertQueryReturns
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.gcloud.spanner.toProtoJson
import org.wfanet.measurement.internal.db.gcp.FakeComputationDetails
import org.wfanet.measurement.internal.db.gcp.FakeProtocolStageDetails
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationStageAttemptDetails

/**
 * Protocol Zero:
 * +--------------+
 * |              |
 * |              v
 * A -> B -> C -> E
 *      |         ^
 *      v         |
 *      D --------+
 *
 * Protocol One:
 * X -> Y -> Z
 */
enum class FakeProtocolStages {
  A, B, C, D, E, X, Y, Z;
}

enum class FakeProtocol {
  ZERO,
  ONE;

  object Helper : ComputationTypeEnumHelper<FakeProtocol> {
    override fun protocolEnumToLong(value: FakeProtocol): Long {
      return value.ordinal.toLong()
    }

    override fun longToProtocolEnum(value: Long): FakeProtocol {
      require(value == 100L)
      return when (value) {
        0L -> ZERO
        1L -> ONE
        else -> error("Bad value")
      }
    }
  }
}

object ComputationProtocolStages :
  ComputationProtocolStagesEnumHelper<FakeProtocol, FakeProtocolStages> {
  private val validInitialStages = mapOf(
    FakeProtocol.ZERO to setOf(A),
    FakeProtocol.ONE to setOf(X)
  )
  private val validTerminalStages = mapOf(
    FakeProtocol.ZERO to setOf(E),
    FakeProtocol.ONE to setOf(Z)
  )
  private val validSuccessors = mapOf(
    A to setOf(B),
    B to setOf(C, D),
    X to setOf(Y)
  )

  override fun stageToProtocol(stage: FakeProtocolStages): FakeProtocol {
    return when (stage) {
      A, B, C, D, E -> FakeProtocol.ZERO
      X, Y, Z -> FakeProtocol.ONE
    }
  }

  override fun computationStageEnumToLongValues(value: FakeProtocolStages):
    ComputationStageLongValues {
      return when (value) {
        A -> ComputationStageLongValues(0, 0)
        B -> ComputationStageLongValues(0, 1)
        C -> ComputationStageLongValues(0, 2)
        D -> ComputationStageLongValues(0, 3)
        E -> ComputationStageLongValues(0, 4)
        X -> ComputationStageLongValues(1, 0)
        Y -> ComputationStageLongValues(1, 1)
        Z -> ComputationStageLongValues(1, 2)
      }
    }

  override fun longValuesToComputationStageEnum(value: ComputationStageLongValues):
    FakeProtocolStages {
      return when (value) {
        ComputationStageLongValues(0, 0) -> A
        ComputationStageLongValues(0, 1) -> B
        ComputationStageLongValues(0, 2) -> C
        ComputationStageLongValues(0, 3) -> D
        ComputationStageLongValues(0, 4) -> E
        ComputationStageLongValues(1, 0) -> X
        ComputationStageLongValues(1, 1) -> Y
        ComputationStageLongValues(1, 2) -> Z
        else -> error("Bad value")
      }
    }

  override fun getValidInitialStage(protocol: FakeProtocol): Set<FakeProtocolStages> =
    validInitialStages[protocol] ?: error("bad protocol")

  override fun getValidTerminalStages(protocol: FakeProtocol): Set<FakeProtocolStages> =
    validTerminalStages[protocol] ?: error("bad protocol")

  override fun validInitialStage(protocol: FakeProtocol, stage: FakeProtocolStages): Boolean {
    return stage in getValidInitialStage(protocol)
  }

  override fun validTerminalStage(protocol: FakeProtocol, stage: FakeProtocolStages): Boolean {
    return stage in getValidTerminalStages(protocol)
  }

  override fun validTransition(
    currentStage: FakeProtocolStages,
    nextStage: FakeProtocolStages
  ): Boolean {
    return nextStage in validSuccessors[currentStage] ?: error("bad stage")
  }
}

class ProtocolStageDetailsHelper :
  ComputationProtocolStageDetailsHelper<
    FakeProtocol, FakeProtocolStages, FakeProtocolStageDetails, FakeComputationDetails> {

  override fun setEndingState(
    details: FakeComputationDetails,
    reason: EndComputationReason
  ): FakeComputationDetails {
    return details.toBuilder().setEndReason(reason.toString()).build()
  }

  override fun parseComputationDetails(bytes: ByteArray): FakeComputationDetails {
    return FakeComputationDetails.parseFrom(bytes)
  }

  override fun validateRoleForStage(
    stage: FakeProtocolStages,
    computationDetails: FakeComputationDetails
  ):
    Boolean {
      return true // the value doesn't matter in this test
    }

  override fun afterTransitionForStage(stage: FakeProtocolStages): AfterTransition {
    return AfterTransition.ADD_UNCLAIMED_TO_QUEUE // the value doesn't matter in this test
  }

  override fun outputBlobNumbersForStage(stage: FakeProtocolStages): Int {
    return 1 // the value doesn't matter in this test
  }

  override fun detailsFor(stage: FakeProtocolStages): FakeProtocolStageDetails {
    return FakeProtocolStageDetails.newBuilder()
      .setName(stage.name)
      .build()
  }

  override fun parseDetails(protocol: FakeProtocol, bytes: ByteArray): FakeProtocolStageDetails =
    FakeProtocolStageDetails.parseFrom(bytes)
}

@RunWith(JUnit4::class)
class GcpSpannerComputationsDbTest : UsingSpannerEmulator(COMPUTATIONS_SCHEMA) {

  companion object {
    val FAKE_COMPUTATION_DETAILS = FakeComputationDetails.newBuilder().apply {
      role = "foo"
    }.build()

    val TEST_INSTANT: Instant = Instant.ofEpochMilli(123456789L)
  }

  private val testClock = TestClockWithNamedInstants(TEST_INSTANT)
  private val computationMutations =
    ComputationMutations(
      FakeProtocol.Helper,
      ComputationProtocolStages,
      ProtocolStageDetailsHelper()
    )

  private lateinit var database:
    GcpSpannerComputationsDb<
      FakeProtocol,
      FakeProtocolStages,
      FakeProtocolStageDetails,
      FakeComputationDetails
      >

  @Before
  fun initDatabase() {
    database = GcpSpannerComputationsDb(
      databaseClient,
      clock = testClock,
      computationMutations = computationMutations
    )
  }

  @Test
  fun `insert two computations`() = runBlocking<Unit> {
    val idGenerator = GlobalBitsPlusTimeStampIdGenerator(testClock)
    val globalId1 = "12345"
    val localId1 = idGenerator.localId(globalId1)
    database.insertComputation(
      globalId1, FakeProtocol.ZERO, A, computationMutations.detailsFor(A), FAKE_COMPUTATION_DETAILS
    )
    val globalId2 = "5678"
    val localId2 = idGenerator.localId(globalId2)
    database.insertComputation(
      globalId2, FakeProtocol.ZERO, A, computationMutations.detailsFor(A), FAKE_COMPUTATION_DETAILS
    )

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
        .set("ComputationStage").toFakeStage(A)
        .set("UpdateTime").to(TEST_INSTANT.toGcloudTimestamp())
        .set("GlobalComputationId").to(globalId1)
        .set("LockOwner").to(null as String?)
        .set("LockExpirationTime").to(TEST_INSTANT.toGcloudTimestamp())
        .set("ComputationDetails").toProtoBytes(FAKE_COMPUTATION_DETAILS)
        .set("ComputationDetailsJSON").toProtoJson(FAKE_COMPUTATION_DETAILS)
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(localId2)
        .set("ComputationStage").toFakeStage(A)
        .set("UpdateTime").to(TEST_INSTANT.toGcloudTimestamp())
        .set("GlobalComputationId").to(globalId2)
        .set("LockOwner").to(null as String?)
        .set("LockExpirationTime").to(TEST_INSTANT.toGcloudTimestamp())
        .set("ComputationDetails").toProtoBytes(FAKE_COMPUTATION_DETAILS)
        .set("ComputationDetailsJSON").toProtoJson(FAKE_COMPUTATION_DETAILS)
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
        .set("ComputationStage").toFakeStage(A)
        .set("CreationTime").to(TEST_INSTANT.toGcloudTimestamp())
        .set("NextAttempt").to(1L)
        .set("EndTime").to(null as Timestamp?)
        .set("Details").toProtoBytes(computationMutations.detailsFor(A))
        .set("DetailsJSON").toProtoJson(computationMutations.detailsFor(A))
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(localId2)
        .set("ComputationStage").toFakeStage(A)
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
    database.insertComputation(
      "123", FakeProtocol.ZERO, A, computationMutations.detailsFor(A), FAKE_COMPUTATION_DETAILS
    )
    // This one fails because the same local id is used.
    assertFailsWith(SpannerException::class, "ALREADY_EXISTS") {
      database.insertComputation(
        "123", FakeProtocol.ZERO, A, computationMutations.detailsFor(A), FAKE_COMPUTATION_DETAILS
      )
    }
  }

  @Test
  fun `insert computation with bad initial stage fails`() = runBlocking<Unit> {
    assertFailsWith(IllegalArgumentException::class, "Invalid initial stage") {
      database.insertComputation(
        "123", FakeProtocol.ONE, B, computationMutations.detailsFor(B), FAKE_COMPUTATION_DETAILS
      )
    }
    assertFailsWith(IllegalArgumentException::class, "Invalid initial stage") {
      database.insertComputation(
        "123", FakeProtocol.ONE, C, computationMutations.detailsFor(C), FAKE_COMPUTATION_DETAILS
      )
    }
    assertFailsWith(IllegalArgumentException::class, "Invalid initial stage") {
      database.insertComputation(
        "123", FakeProtocol.ZERO, D, computationMutations.detailsFor(D), FAKE_COMPUTATION_DETAILS
      )
    }
    assertFailsWith(IllegalArgumentException::class, "Invalid initial stage") {
      database.insertComputation(
        "123", FakeProtocol.ONE, E, computationMutations.detailsFor(E), FAKE_COMPUTATION_DETAILS
      )
    }
  }

  @Test
  fun enqueue() = runBlocking<Unit> {
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val lockExpires = Instant.now().plusSeconds(300)
    val token = ComputationStorageEditToken(
      localId = 1,
      protocol = FakeProtocol.ZERO,
      stage = C,
      attempt = 1,
      editVersion = lastUpdated.toEpochMilli()
    )

    val computation = computationMutations.insertComputation(
      localId = token.localId,
      updateTime = lastUpdated.toGcloudTimestamp(),
      globalId = "0",
      protocol = token.protocol,
      stage = token.stage,
      lockOwner = "PeterSpacemen",
      lockExpirationTime = lockExpires.toGcloudTimestamp(),
      details = FAKE_COMPUTATION_DETAILS
    )
    val differentComputation = computationMutations.insertComputation(
      localId = 456789,
      updateTime = lastUpdated.toGcloudTimestamp(),
      globalId = "10111213",
      protocol = token.protocol,
      stage = B,
      lockOwner = "PeterSpacemen",
      lockExpirationTime = lockExpires.toGcloudTimestamp(),
      details = FAKE_COMPUTATION_DETAILS
    )

    databaseClient.write(listOf(computation, differentComputation))
    database.enqueue(token, 2)

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, Protocol, ComputationStage, LockOwner, LockExpirationTime,
             ComputationDetails, ComputationDetailsJSON
      FROM Computations
      ORDER BY ComputationId
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("Protocol").to(0)
        .set("ComputationStage").toFakeStage(token.stage)
        .set("LockOwner").to(null as String?)
        .set("LockExpirationTime").to(TEST_INSTANT.plusSeconds(2).toGcloudTimestamp())
        .set("ComputationDetails").toProtoBytes(FAKE_COMPUTATION_DETAILS)
        .set("ComputationDetailsJSON").toProtoJson(FAKE_COMPUTATION_DETAILS)
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(456789)
        .set("Protocol").to(0)
        .set("ComputationStage").to(1)
        .set("LockOwner").to("PeterSpacemen")
        .set("LockExpirationTime").to(lockExpires.toGcloudTimestamp())
        .set("ComputationDetails").toProtoBytes(FAKE_COMPUTATION_DETAILS)
        .set("ComputationDetailsJSON").toProtoJson(FAKE_COMPUTATION_DETAILS)
        .build()
    )
  }

  @Test
  fun `enqueue deleted computation fails`() = runBlocking<Unit> {
    val token = ComputationStorageEditToken(
      localId = 1,
      protocol = FakeProtocol.ZERO,
      stage = C,
      attempt = 1,
      editVersion = 0
    )
    assertFailsWith<IllegalStateException> { database.enqueue(token, 0) }
  }

  @Test
  fun `enqueue with old token fails`() = runBlocking<Unit> {
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val lockExpires = lastUpdated.plusSeconds(1)
    val token = ComputationStorageEditToken(
      localId = 1,
      protocol = FakeProtocol.ZERO,
      stage = C,
      attempt = 1,
      editVersion = lastUpdated.minusSeconds(200).toEpochMilli()
    )

    val computation = computationMutations.insertComputation(
      localId = token.localId,
      updateTime = lastUpdated.toGcloudTimestamp(),
      globalId = "1234",
      protocol = FakeProtocol.ONE,
      stage = token.stage,
      lockOwner = "AnOwnedLock",
      lockExpirationTime = lockExpires.toGcloudTimestamp(),
      details = FAKE_COMPUTATION_DETAILS
    )
    databaseClient.write(listOf(computation))
    assertFailsWith<IllegalStateException> { database.enqueue(token, 0) }
  }

  @Test
  fun claimTask() = runBlocking<Unit> {
    testClock.tickSeconds("7_minutes_ago")
    testClock.tickSeconds("6_minutes_ago", 60)
    testClock.tickSeconds("5_minutes_ago", 60)
    testClock.tickSeconds("TimeOfTest", 300)
    val fiveMinutesAgo = testClock["5_minutes_ago"].toGcloudTimestamp()
    val sixMinutesAgo = testClock["6_minutes_ago"].toGcloudTimestamp()
    val sevenMinutesAgo = testClock["7_minutes_ago"].toGcloudTimestamp()
    val enqueuedSevenMinutesAgoForOtherProtocol = computationMutations.insertComputation(
      localId = 111,
      updateTime = fiveMinutesAgo,
      globalId = "11",
      protocol = FakeProtocol.ZERO,
      stage = A,
      lockOwner = WRITE_NULL_STRING,
      lockExpirationTime = sevenMinutesAgo,
      details = FAKE_COMPUTATION_DETAILS
    )
    val enqueuedSevenMinutesAgoForOtherProtocolStage = computationMutations.insertComputationStage(
      localId = 111,
      stage = A,
      nextAttempt = 1,
      creationTime = Instant.ofEpochMilli(3456789L).toGcloudTimestamp(),
      details = computationMutations.detailsFor(A)
    )

    val enqueuedFiveMinutesAgo = computationMutations.insertComputation(
      localId = 555,
      updateTime = fiveMinutesAgo,
      globalId = "55",
      protocol = FakeProtocol.ONE,
      stage = A,
      lockOwner = WRITE_NULL_STRING,
      lockExpirationTime = fiveMinutesAgo,
      details = FAKE_COMPUTATION_DETAILS
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
      protocol = FakeProtocol.ONE,
      stage = A,
      updateTime = sixMinutesAgo,
      globalId = "6",
      lockOwner = WRITE_NULL_STRING,
      lockExpirationTime = sixMinutesAgo,
      details = FAKE_COMPUTATION_DETAILS
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
        enqueuedSixMinutesAgoStage,
        enqueuedSevenMinutesAgoForOtherProtocol,
        enqueuedSevenMinutesAgoForOtherProtocolStage
      )
    )
    assertEquals("6", database.claimTask(FakeProtocol.ONE, "the-owner-of-the-lock"))

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

    assertEquals("55", database.claimTask(FakeProtocol.ONE, "the-owner-of-the-lock"))

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
    assertNull(database.claimTask(FakeProtocol.ONE, "the-owner-of-the-lock"))
  }

  @Test
  fun `claim locked tasks`() = runBlocking<Unit> {
    testClock.tickSeconds("5_minutes_ago", 60)
    testClock.tickSeconds("TimeOfTest", 300)
    val fiveMinutesAgo = testClock["5_minutes_ago"].toGcloudTimestamp()
    val fiveMinutesFromNow = testClock.last().plusSeconds(300).toGcloudTimestamp()
    val expiredClaim = computationMutations.insertComputation(
      localId = 111L,
      protocol = FakeProtocol.ZERO,
      stage = A,
      updateTime = testClock["start"].toGcloudTimestamp(),
      globalId = "11",
      lockOwner = "owner-of-the-lock",
      lockExpirationTime = fiveMinutesAgo,
      details = FAKE_COMPUTATION_DETAILS
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
      protocol = FakeProtocol.ZERO,
      stage = A,
      updateTime = testClock["start"].toGcloudTimestamp(),
      globalId = "33",
      lockOwner = "owner-of-the-lock",
      lockExpirationTime = fiveMinutesFromNow,
      details = FAKE_COMPUTATION_DETAILS
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
    assertEquals("11", database.claimTask(FakeProtocol.ZERO, "new-owner"))

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
    assertNull(database.claimTask(FakeProtocol.ZERO, "new-owner"))
  }

  private fun testTransitionOfStageWhere(
    afterTransition: AfterTransition
  ): ComputationStorageEditToken<FakeProtocol, FakeProtocolStages> = runBlocking {
    testClock.tickSeconds("stage_b_created")
    testClock.tickSeconds("last_updated")
    testClock.tickSeconds("lock_expires", 100)
    val globalId = "55"
    val token = ComputationStorageEditToken(
      localId = 4315,
      protocol = FakeProtocol.ONE,
      stage = B,
      attempt = 2,
      editVersion = testClock["last_updated"].toEpochMilli()
    )
    val computation = computationMutations.insertComputation(
      localId = token.localId,
      updateTime = testClock["last_updated"].toGcloudTimestamp(),
      globalId = globalId,
      protocol = token.protocol,
      stage = B,
      lockOwner = "the-owner-of-the-lock",
      lockExpirationTime = testClock["lock_expires"].toGcloudTimestamp(),
      details = FAKE_COMPUTATION_DETAILS
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
      passThroughBlobPaths = listOf(),
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
    token: ComputationStorageEditToken<FakeProtocol, FakeProtocolStages>
  ) {
    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, UpdateTime
      FROM Computations
      """.trimIndent(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").toFakeStage(D)
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
        .set("ComputationStage").toFakeStage(B)
        .set("CreationTime").to(testClock["stage_b_created"].toGcloudTimestamp())
        .set("EndTime").to(testClock["update_stage"].toGcloudTimestamp())
        .set("PreviousStage").to(null as Long?)
        .set("FollowingStage").to(
          ComputationProtocolStages.computationStageEnumToLongValues(D).stage
        )
        .set("Details").toProtoBytes(computationMutations.detailsFor(B))
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").toFakeStage(D)
        .set("CreationTime").to(testClock["update_stage"].toGcloudTimestamp())
        .set("EndTime").to(null as Timestamp?)
        .set("PreviousStage").to(
          ComputationProtocolStages.computationStageEnumToLongValues(B).stage
        )
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
        .set("ComputationStage").toFakeStage(B)
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
        .set("ComputationStage").toFakeStage(D)
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
        .set("ComputationStage").toFakeStage(B)
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
        .set("ComputationStage").toFakeStage(B)
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
        .set("ComputationStage").toFakeStage(D)
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
      protocol = FakeProtocol.ZERO,
      stage = A,
      editVersion = 0
    )
    assertFailsWith<IllegalArgumentException> {
      database.updateComputationStage(
        token = token,
        nextStage = D,
        inputBlobPaths = listOf(),
        passThroughBlobPaths = listOf(),
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
      protocol = FakeProtocol.ZERO,
      stage = B,
      attempt = 1,
      editVersion = testClock.last().toEpochMilli()
    )
    val computation = computationMutations.insertComputation(
      localId = token.localId,
      protocol = token.protocol,
      stage = B,
      updateTime = testClock.last().toGcloudTimestamp(),
      globalId = "2002",
      lockOwner = WRITE_NULL_STRING,
      lockExpirationTime = WRITE_NULL_TIMESTAMP,
      details = FAKE_COMPUTATION_DETAILS
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
        .set("ComputationStage").toFakeStage(token.stage)
        .set("BlobId").to(1234L)
        .set("PathToBlob").to("/wrote/something/there")
        .set("DependencyType").toProtoEnum(ComputationBlobDependency.OUTPUT)
        .set("UpdateTime").to(testClock.get("write-blob-ref").toGcloudTimestamp())
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").toFakeStage(token.stage)
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
    assertFailsWith<IllegalStateException> {
      database.writeOutputBlobReference(token, BlobRef(5678L, "/wrote/something/there"))
    }
    // Blob id doesn't exist
    assertFailsWith<IllegalStateException> {
      database.writeOutputBlobReference(token, BlobRef(223344L, "/wrote/something/there"))
    }
  }

  @Test
  fun `update computation details`() = runBlocking<Unit> {
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val lockExpires = Instant.now().plusSeconds(300)
    val token = ComputationStorageEditToken(
      localId = 4315,
      protocol = FakeProtocol.ZERO,
      stage = C,
      attempt = 2,
      editVersion = lastUpdated.toEpochMilli()
    )
    val computation = computationMutations.insertComputation(
      localId = token.localId,
      updateTime = lastUpdated.toGcloudTimestamp(),
      protocol = token.protocol,
      stage = token.stage,
      globalId = "55",
      lockOwner = "PeterSpacemen",
      lockExpirationTime = lockExpires.toGcloudTimestamp(),
      details = FAKE_COMPUTATION_DETAILS
    )

    testClock.tickSeconds("time-ended")
    databaseClient.write(listOf(computation))
    val newComputationDetails = FakeComputationDetails.newBuilder().apply {
      role = "something different"
    }.build()
    database.updateComputationDetails(token, newComputationDetails)
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
        .set("ComputationStage").toFakeStage(C)
        .set("UpdateTime").to(testClock.last().toGcloudTimestamp())
        .set("GlobalComputationId").to("55")
        .set("LockOwner").to("PeterSpacemen")
        .set("LockExpirationTime").to(lockExpires.toGcloudTimestamp())
        .set("ComputationDetails").toProtoBytes(newComputationDetails)
        .set("ComputationDetailsJSON").toProtoJson(newComputationDetails)
        .build()
    )
  }

  @Test
  fun `end successful computation`() = runBlocking<Unit> {
    val token = ComputationStorageEditToken(
      localId = 4315,
      protocol = FakeProtocol.ZERO,
      stage = C,
      attempt = 2,
      editVersion = testClock.last().toEpochMilli()
    )
    val computation = computationMutations.insertComputation(
      localId = token.localId,
      updateTime = testClock.last().toGcloudTimestamp(),
      protocol = token.protocol,
      stage = C,
      globalId = "55",
      lockOwner = WRITE_NULL_STRING,
      lockExpirationTime = testClock.last().toGcloudTimestamp(),
      details = FAKE_COMPUTATION_DETAILS
    )
    val stage = computationMutations.insertComputationStage(
      localId = token.localId,
      stage = C,
      nextAttempt = 3,
      creationTime = testClock.last().toGcloudTimestamp(),
      details = computationMutations.detailsFor(C)
    )
    val unfinishedPreviousAttempt = computationMutations.insertComputationStageAttempt(
      localId = token.localId,
      stage = token.stage,
      attempt = token.attempt.toLong() - 1,
      beginTime = testClock.last().toGcloudTimestamp(),
      details = ComputationStageAttemptDetails.getDefaultInstance()
    )
    val attempt = computationMutations.insertComputationStageAttempt(
      localId = token.localId,
      stage = token.stage,
      attempt = token.attempt.toLong(),
      beginTime = testClock.last().toGcloudTimestamp(),
      details = ComputationStageAttemptDetails.getDefaultInstance()
    )
    testClock.tickSeconds("time-ended")
    databaseClient.write(listOf(computation, stage, unfinishedPreviousAttempt, attempt))
    val expectedDetails =
      FAKE_COMPUTATION_DETAILS.toBuilder().setEndReason(EndComputationReason.SUCCEEDED.toString())
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
        .set("ComputationStage").toFakeStage(E)
        .set("UpdateTime").to(testClock.last().toGcloudTimestamp())
        .set("GlobalComputationId").to("55")
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
        .set("ComputationStage").toFakeStage(C)
        .set("Attempt").to(1)
        .set("BeginTime").to(testClock["start"].toGcloudTimestamp())
        .set("EndTime").to(testClock.last().toGcloudTimestamp())
        .set("Details").toProtoBytes(
          ComputationStageAttemptDetails.newBuilder().apply {
            reasonEnded = ComputationStageAttemptDetails.EndReason.CANCELLED
          }.build()
        )
        .build(),
      Struct.newBuilder()
        .set("ComputationId").to(token.localId)
        .set("ComputationStage").toFakeStage(C)
        .set("Attempt").to(2)
        .set("BeginTime").to(testClock["start"].toGcloudTimestamp())
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
  fun `end failed computation`() = runBlocking<Unit> {
    val globalId = "474747"
    val token = ComputationStorageEditToken(
      localId = 4315,
      protocol = FakeProtocol.ZERO,
      stage = C,
      attempt = 1,
      editVersion = testClock.last().toEpochMilli()
    )
    val computation = computationMutations.insertComputation(
      localId = token.localId,
      updateTime = testClock.last().toGcloudTimestamp(),
      protocol = token.protocol,
      stage = C,
      globalId = globalId,
      lockOwner = "lock-owner",
      lockExpirationTime = testClock.last().toGcloudTimestamp(),
      details = FAKE_COMPUTATION_DETAILS
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
      attempt = 1,
      beginTime = testClock.last().toGcloudTimestamp(),
      details = ComputationStageAttemptDetails.getDefaultInstance()
    )
    testClock.tickSeconds("time-failed")
    databaseClient.write(listOf(computation, stage, attempt))
    val expectedDetails =
      FAKE_COMPUTATION_DETAILS.toBuilder().setEndReason(EndComputationReason.FAILED.toString())
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
        .set("ComputationStage").toFakeStage(E)
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
        .set("ComputationStage").toFakeStage(C)
        .set("Attempt").to(1)
        .set("BeginTime").to(testClock["start"].toGcloudTimestamp())
        .set("EndTime").to(testClock.last().toGcloudTimestamp())
        .set("Details").toProtoBytes(
          ComputationStageAttemptDetails.newBuilder().apply {
            reasonEnded = ComputationStageAttemptDetails.EndReason.ERROR
          }.build()
        )
        .build()
    )
  }

  @Test
  fun `endComputation throws for non-ending state`() = runBlocking<Unit> {
    val token = ComputationStorageEditToken(
      localId = 4315,
      protocol = FakeProtocol.ZERO,
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
      protocol = FakeProtocol.ZERO,
      stage = C,
      globalId = globalId,
      lockOwner = "lock-owner",
      lockExpirationTime = testClock.last().toGcloudTimestamp(),
      details = FAKE_COMPUTATION_DETAILS
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
        .set("ComputationStage").toFakeStage(C)
        .set("Attempt").to(2)
        .set("MetricName").to("crypto_cpu_time_millis")
        .set("MetricValue").to(3125)
        .build()
    )
  }

  private fun <T> ValueBinder<T>.toFakeStage(value: FakeProtocolStages): T =
    to(ComputationProtocolStages.computationStageEnumToLongValues(value).stage)
}
