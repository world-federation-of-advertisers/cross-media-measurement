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
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.kotlin.toByteStringUtf8
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.duchy.db.computation.AfterTransition
import org.wfanet.measurement.duchy.db.computation.BlobRef
import org.wfanet.measurement.duchy.db.computation.ComputationEditToken
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetailsHelper
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationStageLongValues
import org.wfanet.measurement.duchy.db.computation.ComputationStatMetric
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
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.duchy.service.internal.ComputationNotFoundException
import org.wfanet.measurement.duchy.service.internal.ComputationTokenVersionMismatchException
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.struct
import org.wfanet.measurement.gcloud.spanner.testing.UsingSpannerEmulator
import org.wfanet.measurement.gcloud.spanner.testing.assertQueryReturns
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.gcloud.spanner.toProtoBytes
import org.wfanet.measurement.gcloud.spanner.toProtoJson
import org.wfanet.measurement.internal.db.gcp.FakeComputationDetails
import org.wfanet.measurement.internal.db.gcp.FakeProtocolStageDetails
import org.wfanet.measurement.internal.duchy.ComputationBlobDependency
import org.wfanet.measurement.internal.duchy.ComputationStageAttemptDetails
import org.wfanet.measurement.internal.duchy.RequisitionDetails
import org.wfanet.measurement.internal.duchy.copy
import org.wfanet.measurement.internal.duchy.externalRequisitionKey
import org.wfanet.measurement.internal.duchy.requisitionEntry

/**
 * Protocol Zero: +--------------+ | | | v A -> B -> C -> E
 *
 * ```
 *      |         ^
 *      v         |
 *      D --------+
 * ```
 *
 * Protocol One: X -> Y -> Z
 */
enum class FakeProtocolStages {
  A,
  B,
  C,
  D,
  E,
  X,
  Y,
  Z,
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
  private val validInitialStages =
    mapOf(FakeProtocol.ZERO to setOf(A), FakeProtocol.ONE to setOf(X))
  private val validTerminalStages =
    mapOf(FakeProtocol.ZERO to setOf(E), FakeProtocol.ONE to setOf(Z))
  private val validSuccessors = mapOf(A to setOf(B), B to setOf(C, D), X to setOf(Y))

  override fun stageToProtocol(stage: FakeProtocolStages): FakeProtocol {
    return when (stage) {
      A,
      B,
      C,
      D,
      E -> FakeProtocol.ZERO
      X,
      Y,
      Z -> FakeProtocol.ONE
    }
  }

  override fun computationStageEnumToLongValues(
    value: FakeProtocolStages
  ): ComputationStageLongValues {
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

  override fun longValuesToComputationStageEnum(
    value: ComputationStageLongValues
  ): FakeProtocolStages {
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
    nextStage: FakeProtocolStages,
  ): Boolean {
    return nextStage in validSuccessors[currentStage] ?: error("bad stage")
  }
}

class ProtocolStageDetailsHelper :
  ComputationProtocolStageDetailsHelper<
    FakeProtocol,
    FakeProtocolStages,
    FakeProtocolStageDetails,
    FakeComputationDetails,
  > {

  override fun setEndingState(
    details: FakeComputationDetails,
    reason: EndComputationReason,
  ): FakeComputationDetails {
    return details.toBuilder().setEndReason(reason.toString()).build()
  }

  override fun parseComputationDetails(bytes: ByteArray): FakeComputationDetails {
    return FakeComputationDetails.parseFrom(bytes)
  }

  override fun validateRoleForStage(
    stage: FakeProtocolStages,
    computationDetails: FakeComputationDetails,
  ): Boolean {
    return true // the value doesn't matter in this test
  }

  override fun afterTransitionForStage(stage: FakeProtocolStages): AfterTransition {
    return AfterTransition.ADD_UNCLAIMED_TO_QUEUE // the value doesn't matter in this test
  }

  override fun outputBlobNumbersForStage(
    stage: FakeProtocolStages,
    computationDetails: FakeComputationDetails,
  ): Int {
    return 1 // the value doesn't matter in this test
  }

  override fun detailsFor(
    stage: FakeProtocolStages,
    computationDetails: FakeComputationDetails,
  ): FakeProtocolStageDetails {
    return FakeProtocolStageDetails.newBuilder().setName(stage.name).build()
  }

  override fun parseDetails(protocol: FakeProtocol, bytes: ByteArray): FakeProtocolStageDetails =
    FakeProtocolStageDetails.parseFrom(bytes)
}

@RunWith(JUnit4::class)
class GcpSpannerComputationsDatabaseTransactorTest :
  UsingSpannerEmulator(Schemata.DUCHY_CHANGELOG_PATH) {

  companion object {
    val FAKE_COMPUTATION_DETAILS: FakeComputationDetails =
      FakeComputationDetails.newBuilder().apply { role = "foo" }.build()

    val TEST_INSTANT: Instant = Instant.ofEpochMilli(123456789L)

    private val DEFAULT_LOCK_DURATION = Duration.ofMinutes(5)
  }

  private val testClock = TestClockWithNamedInstants(TEST_INSTANT)
  private val computationMutations =
    ComputationMutations(
      FakeProtocol.Helper,
      ComputationProtocolStages,
      ProtocolStageDetailsHelper(),
    )

  private lateinit var database:
    GcpSpannerComputationsDatabaseTransactor<
      FakeProtocol,
      FakeProtocolStages,
      FakeProtocolStageDetails,
      FakeComputationDetails,
    >

  class TestIdGenerator() : IdGenerator {
    var next = AtomicLong(1)

    override fun generateId(): Long {
      return next.getAndIncrement()
    }
  }

  @Before
  fun initDatabase() {
    database =
      GcpSpannerComputationsDatabaseTransactor(
        databaseClient,
        computationMutations = computationMutations,
        clock = testClock,
        TestIdGenerator(),
      )
  }

  @Test
  fun `insert two computations`() = runBlocking {
    val globalId1 = "12345"
    val localId1 = 1L
    database.insertComputation(
      globalId1,
      FakeProtocol.ZERO,
      A,
      computationMutations.detailsFor(A, FAKE_COMPUTATION_DETAILS),
      FAKE_COMPUTATION_DETAILS,
    )
    val globalId2 = "5678"
    val localId2 = 2L
    database.insertComputation(
      globalId2,
      FakeProtocol.ZERO,
      A,
      computationMutations.detailsFor(A, FAKE_COMPUTATION_DETAILS),
      FAKE_COMPUTATION_DETAILS,
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, UpdateTime, GlobalComputationId, LockOwner,
             LockExpirationTime, ComputationDetails, ComputationDetailsJSON
      FROM Computations
      ORDER BY ComputationId ASC
      """
        .trimIndent(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(localId1)
          set("ComputationStage").toFakeStage(A)
          set("UpdateTime").to(TEST_INSTANT.toGcloudTimestamp())
          set("GlobalComputationId").to(globalId1)
          set("LockOwner").to(null as String?)
          set("LockExpirationTime").to(TEST_INSTANT.toGcloudTimestamp())
          set("ComputationDetails").toProtoBytes(FAKE_COMPUTATION_DETAILS)
          set("ComputationDetailsJSON").toProtoJson(FAKE_COMPUTATION_DETAILS)
        }
        .build(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(localId2)
          set("ComputationStage").toFakeStage(A)
          set("UpdateTime").to(TEST_INSTANT.toGcloudTimestamp())
          set("GlobalComputationId").to(globalId2)
          set("LockOwner").to(null as String?)
          set("LockExpirationTime").to(TEST_INSTANT.toGcloudTimestamp())
          set("ComputationDetails").toProtoBytes(FAKE_COMPUTATION_DETAILS)
          set("ComputationDetailsJSON").toProtoJson(FAKE_COMPUTATION_DETAILS)
        }
        .build(),
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, CreationTime, NextAttempt,
             EndTime, Details, DetailsJSON
      FROM ComputationStages
      ORDER BY ComputationId ASC
      """
        .trimIndent(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(localId1)
          set("ComputationStage").toFakeStage(A)
          set("CreationTime").to(TEST_INSTANT.toGcloudTimestamp())
          set("NextAttempt").to(1L)
          set("EndTime").to(null as Timestamp?)
          set("Details").toProtoBytes(computationMutations.detailsFor(A, FAKE_COMPUTATION_DETAILS))
          set("DetailsJSON")
            .toProtoJson(computationMutations.detailsFor(A, FAKE_COMPUTATION_DETAILS))
        }
        .build(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(localId2)
          set("ComputationStage").toFakeStage(A)
          set("CreationTime").to(TEST_INSTANT.toGcloudTimestamp())
          set("NextAttempt").to(1L)
          set("EndTime").to(null as Timestamp?)
          set("Details").toProtoBytes(computationMutations.detailsFor(A, FAKE_COMPUTATION_DETAILS))
          set("DetailsJSON")
            .toProtoJson(computationMutations.detailsFor(A, FAKE_COMPUTATION_DETAILS))
        }
        .build(),
    )
  }

  @Test
  fun `insert computations with same global id fails`() =
    runBlocking<Unit> {
      database.insertComputation(
        "123",
        FakeProtocol.ZERO,
        A,
        computationMutations.detailsFor(A, FAKE_COMPUTATION_DETAILS),
        FAKE_COMPUTATION_DETAILS,
      )
      // This one fails because the same local id is used.
      assertFailsWith(SpannerException::class, "ALREADY_EXISTS") {
        database.insertComputation(
          "123",
          FakeProtocol.ZERO,
          A,
          computationMutations.detailsFor(A, FAKE_COMPUTATION_DETAILS),
          FAKE_COMPUTATION_DETAILS,
        )
      }
    }

  @Test
  fun `insert computation with bad initial stage fails`() =
    runBlocking<Unit> {
      assertFailsWith(IllegalArgumentException::class, "Invalid initial stage") {
        database.insertComputation(
          "123",
          FakeProtocol.ONE,
          B,
          computationMutations.detailsFor(B, FAKE_COMPUTATION_DETAILS),
          FAKE_COMPUTATION_DETAILS,
        )
      }
      assertFailsWith(IllegalArgumentException::class, "Invalid initial stage") {
        database.insertComputation(
          "123",
          FakeProtocol.ONE,
          C,
          computationMutations.detailsFor(C, FAKE_COMPUTATION_DETAILS),
          FAKE_COMPUTATION_DETAILS,
        )
      }
      assertFailsWith(IllegalArgumentException::class, "Invalid initial stage") {
        database.insertComputation(
          "123",
          FakeProtocol.ZERO,
          D,
          computationMutations.detailsFor(D, FAKE_COMPUTATION_DETAILS),
          FAKE_COMPUTATION_DETAILS,
        )
      }
      assertFailsWith(IllegalArgumentException::class, "Invalid initial stage") {
        database.insertComputation(
          "123",
          FakeProtocol.ONE,
          E,
          computationMutations.detailsFor(E, FAKE_COMPUTATION_DETAILS),
          FAKE_COMPUTATION_DETAILS,
        )
      }
    }

  @Test
  fun enqueue() = runBlocking {
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val lockExpires = Instant.now().plusSeconds(300)
    val token =
      ComputationEditToken(
        localId = 1,
        protocol = FakeProtocol.ZERO,
        stage = C,
        attempt = 1,
        editVersion = lastUpdated.toEpochMilli(),
        globalId = "0",
      )

    val computation =
      computationMutations.insertComputation(
        localId = token.localId,
        creationTime = lastUpdated.toGcloudTimestamp(),
        updateTime = lastUpdated.toGcloudTimestamp(),
        globalId = token.globalId,
        protocol = token.protocol,
        stage = token.stage,
        lockOwner = "PeterSpacemen",
        lockExpirationTime = lockExpires.toGcloudTimestamp(),
        details = FAKE_COMPUTATION_DETAILS,
      )
    val differentComputation =
      computationMutations.insertComputation(
        localId = 456789,
        creationTime = lastUpdated.toGcloudTimestamp(),
        updateTime = lastUpdated.toGcloudTimestamp(),
        globalId = "10111213",
        protocol = token.protocol,
        stage = B,
        lockOwner = "PeterSpacemen",
        lockExpirationTime = lockExpires.toGcloudTimestamp(),
        details = FAKE_COMPUTATION_DETAILS,
      )

    databaseClient.write(listOf(computation, differentComputation))
    database.enqueue(token, 2, "PeterSpacemen")

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, Protocol, ComputationStage, LockOwner, LockExpirationTime,
             ComputationDetails, ComputationDetailsJSON
      FROM Computations
      ORDER BY ComputationId
      """
        .trimIndent(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(token.localId)
          set("Protocol").to(0)
          set("ComputationStage").toFakeStage(token.stage)
          set("LockOwner").to(null as String?)
          set("LockExpirationTime").to(TEST_INSTANT.plusSeconds(2).toGcloudTimestamp())
          set("ComputationDetails").toProtoBytes(FAKE_COMPUTATION_DETAILS)
          set("ComputationDetailsJSON").toProtoJson(FAKE_COMPUTATION_DETAILS)
        }
        .build(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(456789)
          set("Protocol").to(0)
          set("ComputationStage").to(1)
          set("LockOwner").to("PeterSpacemen")
          set("LockExpirationTime").to(lockExpires.toGcloudTimestamp())
          set("ComputationDetails").toProtoBytes(FAKE_COMPUTATION_DETAILS)
          set("ComputationDetailsJSON").toProtoJson(FAKE_COMPUTATION_DETAILS)
        }
        .build(),
    )
  }

  @Test
  fun `enqueue deleted computation fails`() =
    runBlocking<Unit> {
      val token =
        ComputationEditToken(
          localId = 1,
          protocol = FakeProtocol.ZERO,
          stage = C,
          attempt = 1,
          editVersion = 0,
          globalId = "0",
        )
      assertFailsWith<ComputationNotFoundException> {
        database.enqueue(token, 0, "the-owner-of-the-lock")
      }
    }

  @Test
  fun `enqueue with old token fails`() =
    runBlocking<Unit> {
      val lastUpdated = Instant.ofEpochMilli(12345678910L)
      val lockExpires = lastUpdated.plusSeconds(1)
      val token =
        ComputationEditToken(
          localId = 1,
          protocol = FakeProtocol.ZERO,
          stage = C,
          attempt = 1,
          editVersion = lastUpdated.minusSeconds(200).toEpochMilli(),
          globalId = "1234",
        )

      val computation =
        computationMutations.insertComputation(
          localId = token.localId,
          creationTime = lastUpdated.toGcloudTimestamp(),
          updateTime = lastUpdated.toGcloudTimestamp(),
          globalId = token.globalId,
          protocol = FakeProtocol.ONE,
          stage = token.stage,
          lockOwner = "AnOwnedLock",
          lockExpirationTime = lockExpires.toGcloudTimestamp(),
          details = FAKE_COMPUTATION_DETAILS,
        )
      databaseClient.write(listOf(computation))
      assertFailsWith<ComputationTokenVersionMismatchException> {
        database.enqueue(token, 0, "AnOwnedLock")
      }
    }

  @Test
  fun `enqueue with unmatched owner fails`() =
    runBlocking<Unit> {
      val lastUpdated = Instant.ofEpochMilli(12345678910L)
      val lockExpires = lastUpdated.plusSeconds(1)
      val token =
        ComputationEditToken(
          localId = 1,
          protocol = FakeProtocol.ZERO,
          stage = C,
          attempt = 1,
          editVersion = lastUpdated.minusSeconds(200).toEpochMilli(),
          globalId = "1234",
        )

      val computation =
        computationMutations.insertComputation(
          localId = token.localId,
          creationTime = lastUpdated.toGcloudTimestamp(),
          updateTime = lastUpdated.toGcloudTimestamp(),
          globalId = token.globalId,
          protocol = FakeProtocol.ONE,
          stage = token.stage,
          lockOwner = "AnOwnedLock",
          lockExpirationTime = lockExpires.toGcloudTimestamp(),
          details = FAKE_COMPUTATION_DETAILS,
        )
      databaseClient.write(listOf(computation))
      assertFailsWith<ComputationTokenVersionMismatchException> {
        database.enqueue(token, 0, "WrongOwner")
      }
    }

  @Test
  fun claimTask() = runBlocking {
    testClock.tickSeconds("7_minutes_ago")
    testClock.tickSeconds("6_minutes_ago", 60)
    testClock.tickSeconds("5_minutes_ago", 60)
    testClock.tickSeconds("TimeOfTest", 300)
    val fiveMinutesAgo = testClock["5_minutes_ago"].toGcloudTimestamp()
    val sixMinutesAgo = testClock["6_minutes_ago"].toGcloudTimestamp()
    val sevenMinutesAgo = testClock["7_minutes_ago"].toGcloudTimestamp()
    val enqueuedSevenMinutesAgoForOtherProtocol =
      computationMutations.insertComputation(
        localId = 111,
        creationTime = fiveMinutesAgo,
        updateTime = fiveMinutesAgo,
        globalId = "11",
        protocol = FakeProtocol.ZERO,
        stage = A,
        lockOwner = WRITE_NULL_STRING,
        lockExpirationTime = sevenMinutesAgo,
        details = FAKE_COMPUTATION_DETAILS,
      )
    val enqueuedSevenMinutesAgoForOtherProtocolStage =
      computationMutations.insertComputationStage(
        localId = 111,
        stage = A,
        nextAttempt = 1,
        creationTime = Instant.ofEpochMilli(3456789L).toGcloudTimestamp(),
        details = computationMutations.detailsFor(A, FAKE_COMPUTATION_DETAILS),
      )

    val enqueuedFiveMinutesAgo =
      computationMutations.insertComputation(
        localId = 555,
        creationTime = fiveMinutesAgo,
        updateTime = fiveMinutesAgo,
        globalId = "55",
        protocol = FakeProtocol.ONE,
        stage = A,
        lockOwner = WRITE_NULL_STRING,
        lockExpirationTime = fiveMinutesAgo,
        details = FAKE_COMPUTATION_DETAILS,
      )
    val enqueuedFiveMinutesAgoStage =
      computationMutations.insertComputationStage(
        localId = 555,
        stage = A,
        nextAttempt = 1,
        creationTime = Instant.ofEpochMilli(3456789L).toGcloudTimestamp(),
        details = computationMutations.detailsFor(A, FAKE_COMPUTATION_DETAILS),
      )

    val enqueuedSixMinutesAgo =
      computationMutations.insertComputation(
        localId = 66,
        protocol = FakeProtocol.ONE,
        stage = A,
        creationTime = sixMinutesAgo,
        updateTime = sixMinutesAgo,
        globalId = "6",
        lockOwner = WRITE_NULL_STRING,
        lockExpirationTime = sixMinutesAgo,
        details = FAKE_COMPUTATION_DETAILS,
      )
    val enqueuedSixMinutesAgoStage =
      computationMutations.insertComputationStage(
        localId = 66,
        stage = A,
        nextAttempt = 1,
        creationTime = Instant.ofEpochMilli(3456789L).toGcloudTimestamp(),
        details = computationMutations.detailsFor(A, FAKE_COMPUTATION_DETAILS),
      )
    databaseClient.write(
      listOf(
        enqueuedFiveMinutesAgo,
        enqueuedFiveMinutesAgoStage,
        enqueuedSixMinutesAgo,
        enqueuedSixMinutesAgoStage,
        enqueuedSevenMinutesAgoForOtherProtocol,
        enqueuedSevenMinutesAgoForOtherProtocolStage,
      )
    )
    assertEquals(
      "6",
      database.claimTask(FakeProtocol.ONE, "the-owner-of-the-lock", DEFAULT_LOCK_DURATION),
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT Attempt, BeginTime, EndTime, Details FROM ComputationStageAttempts
      WHERE ComputationId = 66 AND ComputationStage = 0
      """
        .trimIndent(),
      Struct.newBuilder()
        .apply {
          set("Attempt").to(1)
          set("BeginTime").to(testClock.last().toGcloudTimestamp())
          set("EndTime").to(null as Timestamp?)
          set("Details").toProtoBytes(ComputationStageAttemptDetails.getDefaultInstance())
        }
        .build(),
    )

    assertEquals(
      "55",
      database.claimTask(FakeProtocol.ONE, "the-owner-of-the-lock", DEFAULT_LOCK_DURATION),
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT Attempt, BeginTime, EndTime, Details FROM ComputationStageAttempts
      WHERE ComputationId = 555 AND ComputationStage = 0
      """
        .trimIndent(),
      Struct.newBuilder()
        .apply {
          set("Attempt").to(1)
          set("BeginTime").to(testClock.last().toGcloudTimestamp())
          set("EndTime").to(null as Timestamp?)
          set("Details").toProtoBytes(ComputationStageAttemptDetails.getDefaultInstance())
        }
        .build(),
    )

    // No tasks to claim anymore
    assertNull(database.claimTask(FakeProtocol.ONE, "the-owner-of-the-lock", DEFAULT_LOCK_DURATION))
  }

  @Test
  fun `claimTask returns prioritized computation`() = runBlocking {
    testClock.tickSeconds("7_minutes_ago")
    testClock.tickSeconds("6_minutes_ago", 60)
    testClock.tickSeconds("5_minutes_ago", 60)
    testClock.tickSeconds("TimeOfTest", 300)
    val timestamp1 = testClock["7_minutes_ago"].toGcloudTimestamp()
    val timestamp2 = testClock["6_minutes_ago"].toGcloudTimestamp()
    val timestamp3 = testClock["5_minutes_ago"].toGcloudTimestamp()

    val computation1 =
      computationMutations.insertComputation(
        localId = 1,
        creationTime = timestamp1,
        updateTime = timestamp1,
        globalId = "1",
        protocol = FakeProtocol.ZERO,
        stage = C,
        lockOwner = WRITE_NULL_STRING,
        lockExpirationTime = timestamp1,
        details = FAKE_COMPUTATION_DETAILS,
      )
    val computation1Stage =
      computationMutations.insertComputationStage(
        localId = 1,
        stage = C,
        nextAttempt = 1,
        creationTime = timestamp1,
        details = computationMutations.detailsFor(A, FAKE_COMPUTATION_DETAILS),
      )
    val computation2 =
      computationMutations.insertComputation(
        localId = 2,
        protocol = FakeProtocol.ZERO,
        stage = E,
        creationTime = timestamp2,
        updateTime = timestamp2,
        globalId = "2",
        lockOwner = WRITE_NULL_STRING,
        lockExpirationTime = timestamp2,
        details = FAKE_COMPUTATION_DETAILS,
      )
    val computation2Stage =
      computationMutations.insertComputationStage(
        localId = 2,
        stage = E,
        nextAttempt = 1,
        creationTime = timestamp2,
        details = computationMutations.detailsFor(A, FAKE_COMPUTATION_DETAILS),
      )
    // This is the computation with prioritized stage.
    val computation3 =
      computationMutations.insertComputation(
        localId = 3,
        creationTime = timestamp3,
        updateTime = timestamp3,
        globalId = "3",
        protocol = FakeProtocol.ZERO,
        stage = A,
        lockOwner = WRITE_NULL_STRING,
        lockExpirationTime = timestamp3,
        details = FAKE_COMPUTATION_DETAILS,
      )
    val computation3Stage =
      computationMutations.insertComputationStage(
        localId = 3,
        stage = A,
        nextAttempt = 1,
        creationTime = timestamp3,
        details = computationMutations.detailsFor(A, FAKE_COMPUTATION_DETAILS),
      )
    databaseClient.write(
      listOf(
        computation1,
        computation1Stage,
        computation2,
        computation2Stage,
        computation3,
        computation3Stage,
      )
    )

    assertEquals(
      "3",
      database.claimTask(
        FakeProtocol.ZERO,
        "the-owner-of-the-lock",
        DEFAULT_LOCK_DURATION,
        listOf(A),
      ),
    )
    assertEquals(
      "1",
      database.claimTask(
        FakeProtocol.ZERO,
        "the-owner-of-the-lock",
        DEFAULT_LOCK_DURATION,
        listOf(A),
      ),
    )
    assertEquals(
      "2",
      database.claimTask(
        FakeProtocol.ZERO,
        "the-owner-of-the-lock",
        DEFAULT_LOCK_DURATION,
        listOf(A),
      ),
    )
  }

  @Test
  fun `claim locked tasks`() = runBlocking {
    testClock.tickSeconds("5_minutes_ago", 60)
    testClock.tickSeconds("TimeOfTest", 300)
    val fiveMinutesAgo = testClock["5_minutes_ago"].toGcloudTimestamp()
    val fiveMinutesFromNow = testClock.last().plusSeconds(300).toGcloudTimestamp()
    val expiredClaim =
      computationMutations.insertComputation(
        localId = 111L,
        protocol = FakeProtocol.ZERO,
        stage = A,
        creationTime = testClock["start"].toGcloudTimestamp(),
        updateTime = testClock["start"].toGcloudTimestamp(),
        globalId = "11",
        lockOwner = "owner-of-the-lock",
        lockExpirationTime = fiveMinutesAgo,
        details = FAKE_COMPUTATION_DETAILS,
      )
    val expiredClaimStage =
      computationMutations.insertComputationStage(
        localId = 111,
        stage = A,
        nextAttempt = 2,
        creationTime = fiveMinutesAgo,
        details = computationMutations.detailsFor(A, FAKE_COMPUTATION_DETAILS),
      )
    val expiredClaimAttempt =
      computationMutations.insertComputationStageAttempt(
        localId = 111,
        stage = A,
        attempt = 1,
        beginTime = fiveMinutesAgo,
        details = ComputationStageAttemptDetails.getDefaultInstance(),
      )
    databaseClient.write(listOf(expiredClaim, expiredClaimStage, expiredClaimAttempt))

    val claimed =
      computationMutations.insertComputation(
        localId = 333,
        protocol = FakeProtocol.ZERO,
        stage = A,
        creationTime = testClock["start"].toGcloudTimestamp(),
        updateTime = testClock["start"].toGcloudTimestamp(),
        globalId = "33",
        lockOwner = "owner-of-the-lock",
        lockExpirationTime = fiveMinutesFromNow,
        details = FAKE_COMPUTATION_DETAILS,
      )
    val claimedStage =
      computationMutations.insertComputationStage(
        localId = 333,
        stage = A,
        nextAttempt = 2,
        creationTime = fiveMinutesAgo,
        details = computationMutations.detailsFor(A, FAKE_COMPUTATION_DETAILS),
      )
    val claimedAttempt =
      computationMutations.insertComputationStageAttempt(
        localId = 333,
        stage = A,
        attempt = 1,
        beginTime = fiveMinutesAgo,
        details = ComputationStageAttemptDetails.getDefaultInstance(),
      )
    databaseClient.write(listOf(claimed, claimedStage, claimedAttempt))

    // Claim a task that is owned but the lock expired.
    assertEquals("11", database.claimTask(FakeProtocol.ZERO, "new-owner", DEFAULT_LOCK_DURATION))

    assertQueryReturns(
      databaseClient,
      """
      SELECT Attempt, BeginTime, EndTime, Details FROM ComputationStageAttempts
      WHERE ComputationId = 111 AND ComputationStage = 0
      ORDER BY Attempt
      """
        .trimIndent(),
      Struct.newBuilder()
        .apply {
          set("Attempt").to(1)
          set("BeginTime").to(fiveMinutesAgo)
          set("EndTime").to(testClock.last().toGcloudTimestamp())
          set("Details")
            .toProtoBytes(
              ComputationStageAttemptDetails.newBuilder()
                .apply { reasonEnded = ComputationStageAttemptDetails.EndReason.LOCK_OVERWRITTEN }
                .build()
            )
        }
        .build(),
      Struct.newBuilder()
        .apply {
          set("Attempt").to(2)
          set("BeginTime").to(testClock.last().toGcloudTimestamp())
          set("EndTime").to(null as Timestamp?)
          set("Details").toProtoBytes(ComputationStageAttemptDetails.getDefaultInstance())
        }
        .build(),
    )

    // No task may be claimed at this point
    assertNull(database.claimTask(FakeProtocol.ZERO, "new-owner", DEFAULT_LOCK_DURATION))
  }

  private fun testTransitionOfStageWhere(
    afterTransition: AfterTransition
  ): ComputationEditToken<FakeProtocol, FakeProtocolStages> = runBlocking {
    testClock.tickSeconds("stage_b_created")
    testClock.tickSeconds("last_updated")
    testClock.tickSeconds("lock_expires", 100)
    val globalId = "55"
    val token =
      ComputationEditToken(
        localId = 4315,
        protocol = FakeProtocol.ONE,
        stage = B,
        attempt = 2,
        editVersion = testClock["last_updated"].toEpochMilli(),
        globalId = globalId,
      )
    val computation =
      computationMutations.insertComputation(
        localId = token.localId,
        creationTime = testClock["last_updated"].toGcloudTimestamp(),
        updateTime = testClock["last_updated"].toGcloudTimestamp(),
        globalId = globalId,
        protocol = token.protocol,
        stage = B,
        lockOwner = "the-owner-of-the-lock",
        lockExpirationTime = testClock["lock_expires"].toGcloudTimestamp(),
        details = FAKE_COMPUTATION_DETAILS,
      )
    val stage =
      computationMutations.insertComputationStage(
        localId = token.localId,
        stage = B,
        nextAttempt = 3,
        creationTime = testClock["stage_b_created"].toGcloudTimestamp(),
        details = computationMutations.detailsFor(B, FAKE_COMPUTATION_DETAILS),
      )
    val attempt =
      computationMutations.insertComputationStageAttempt(
        localId = token.localId,
        stage = B,
        attempt = 2,
        beginTime = testClock["stage_b_created"].toGcloudTimestamp(),
        details = ComputationStageAttemptDetails.getDefaultInstance(),
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
      nextStageDetails = computationMutations.detailsFor(D, FAKE_COMPUTATION_DETAILS),
      lockExtension = DEFAULT_LOCK_DURATION,
    )
    // Ensure the Computation and ComputationStage were updated. This does not check if the
    // lock is held or the exact configurations of the ComputationStageAttempts because they
    // differ depending on what to do after the transition.
    assertStageTransitioned(token)
    return@runBlocking token
  }

  private suspend fun assertStageTransitioned(
    token: ComputationEditToken<FakeProtocol, FakeProtocolStages>
  ) {
    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, UpdateTime
      FROM Computations
      """
        .trimIndent(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(token.localId)
          set("ComputationStage").toFakeStage(D)
          set("UpdateTime").to(testClock["update_stage"].toGcloudTimestamp())
        }
        .build(),
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, CreationTime, EndTime, PreviousStage, FollowingStage,
             Details
      FROM ComputationStages
      ORDER BY ComputationStage
      """
        .trimIndent(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(token.localId)
          set("ComputationStage").toFakeStage(B)
          set("CreationTime").to(testClock["stage_b_created"].toGcloudTimestamp())
          set("EndTime").to(testClock["update_stage"].toGcloudTimestamp())
          set("PreviousStage").to(null as Long?)
          set("FollowingStage")
            .to(ComputationProtocolStages.computationStageEnumToLongValues(D).stage)
          set("Details").toProtoBytes(computationMutations.detailsFor(B, FAKE_COMPUTATION_DETAILS))
        }
        .build(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(token.localId)
          set("ComputationStage").toFakeStage(D)
          set("CreationTime").to(testClock["update_stage"].toGcloudTimestamp())
          set("EndTime").to(null as Timestamp?)
          set("PreviousStage")
            .to(ComputationProtocolStages.computationStageEnumToLongValues(B).stage)
          set("FollowingStage").to(null as Long?)
          set("Details").toProtoBytes(computationMutations.detailsFor(D, FAKE_COMPUTATION_DETAILS))
        }
        .build(),
    )
  }

  @Test
  fun `updateComputationStage and continue working`() = runBlocking {
    val token = testTransitionOfStageWhere(AfterTransition.CONTINUE_WORKING)

    assertQueryReturns(
      databaseClient,
      "SELECT ComputationId, LockOwner, LockExpirationTime FROM Computations",
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(token.localId)
          set("LockOwner").to("the-owner-of-the-lock")
          set("LockExpirationTime").to(testClock.last().plusSeconds(300).toGcloudTimestamp())
        }
        .build(),
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, Attempt, BeginTime, EndTime, Details
      FROM ComputationStageAttempts
      ORDER BY ComputationStage, Attempt
      """
        .trimIndent(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(token.localId)
          set("ComputationStage").toFakeStage(B)
          set("Attempt").to(2)
          set("BeginTime").to(testClock["stage_b_created"].toGcloudTimestamp())
          set("EndTime").to(testClock.last().toGcloudTimestamp())
          set("Details")
            .toProtoBytes(
              ComputationStageAttemptDetails.newBuilder()
                .apply { reasonEnded = ComputationStageAttemptDetails.EndReason.SUCCEEDED }
                .build()
            )
        }
        .build(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(token.localId)
          set("ComputationStage").toFakeStage(D)
          set("Attempt").to(1)
          set("BeginTime").to(testClock.last().toGcloudTimestamp())
          set("EndTime").to(null as Timestamp?)
          set("Details").toProtoBytes(ComputationStageAttemptDetails.getDefaultInstance())
        }
        .build(),
    )
  }

  @Test
  fun `updateComputationStage and add to queue`() = runBlocking {
    val token = testTransitionOfStageWhere(AfterTransition.ADD_UNCLAIMED_TO_QUEUE)

    assertQueryReturns(
      databaseClient,
      "SELECT ComputationId, LockOwner, LockExpirationTime FROM Computations",
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(token.localId)
          set("LockOwner").to(null as String?)
          set("LockExpirationTime").to(testClock.last().toGcloudTimestamp())
        }
        .build(),
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, Attempt, BeginTime, EndTime, Details
      FROM ComputationStageAttempts
      ORDER BY ComputationStage, Attempt
      """
        .trimIndent(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(token.localId)
          set("ComputationStage").toFakeStage(B)
          set("Attempt").to(2)
          set("BeginTime").to(testClock["stage_b_created"].toGcloudTimestamp())
          set("EndTime").to(testClock.last().toGcloudTimestamp())
          set("Details")
            .toProtoBytes(
              ComputationStageAttemptDetails.newBuilder()
                .apply { reasonEnded = ComputationStageAttemptDetails.EndReason.SUCCEEDED }
                .build()
            )
        }
        .build(),
    )
  }

  @Test
  fun `updateComputationStage and do not add to queue`() = runBlocking {
    val token = testTransitionOfStageWhere(AfterTransition.DO_NOT_ADD_TO_QUEUE)

    assertQueryReturns(
      databaseClient,
      "SELECT ComputationId, LockOwner, LockExpirationTime FROM Computations",
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(token.localId)
          set("LockOwner").to(null as String?)
          set("LockExpirationTime").to(null as Timestamp?)
        }
        .build(),
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, Attempt, BeginTime, EndTime, Details
      FROM ComputationStageAttempts
      ORDER BY ComputationStage, Attempt
      """
        .trimIndent(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(token.localId)
          set("ComputationStage").toFakeStage(B)
          set("Attempt").to(2)
          set("BeginTime").to(testClock["stage_b_created"].toGcloudTimestamp())
          set("EndTime").to(testClock.last().toGcloudTimestamp())
          set("Details")
            .toProtoBytes(
              ComputationStageAttemptDetails.newBuilder()
                .apply { reasonEnded = ComputationStageAttemptDetails.EndReason.SUCCEEDED }
                .build()
            )
        }
        .build(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(token.localId)
          set("ComputationStage").toFakeStage(D)
          set("Attempt").to(1)
          set("BeginTime").to(testClock.last().toGcloudTimestamp())
          set("EndTime").to(null as Timestamp?)
          set("Details").toProtoBytes(ComputationStageAttemptDetails.getDefaultInstance())
        }
        .build(),
    )
  }

  @Test
  fun `updateComputationStage illegal stage transition fails`() =
    runBlocking<Unit> {
      val token =
        ComputationEditToken(
          localId = 1,
          attempt = 1,
          protocol = FakeProtocol.ZERO,
          stage = A,
          editVersion = 0,
          globalId = "0",
        )
      assertFailsWith<IllegalArgumentException> {
        database.updateComputationStage(
          token = token,
          nextStage = D,
          inputBlobPaths = listOf(),
          passThroughBlobPaths = listOf(),
          outputBlobs = 0,
          afterTransition = AfterTransition.DO_NOT_ADD_TO_QUEUE,
          nextStageDetails = computationMutations.detailsFor(D, FAKE_COMPUTATION_DETAILS),
          lockExtension = DEFAULT_LOCK_DURATION,
        )
      }
    }

  @Test
  fun writeOutputBlobReference() =
    runBlocking<Unit> {
      var token =
        ComputationEditToken(
          localId = 4315,
          protocol = FakeProtocol.ZERO,
          stage = B,
          attempt = 1,
          editVersion = testClock.last().toEpochMilli(),
          globalId = "2002",
        )
      val computation =
        computationMutations.insertComputation(
          localId = token.localId,
          protocol = token.protocol,
          stage = B,
          creationTime = testClock.last().toGcloudTimestamp(),
          updateTime = testClock.last().toGcloudTimestamp(),
          globalId = token.globalId,
          lockOwner = WRITE_NULL_STRING,
          lockExpirationTime = WRITE_NULL_TIMESTAMP,
          details = FAKE_COMPUTATION_DETAILS,
        )
      val stage =
        computationMutations.insertComputationStage(
          localId = token.localId,
          stage = B,
          nextAttempt = 3,
          creationTime = testClock.last().toGcloudTimestamp(),
          details = FakeProtocolStageDetails.getDefaultInstance(),
        )
      val outputRef =
        computationMutations.insertComputationBlobReference(
          localId = token.localId,
          stage = B,
          blobId = 1234L,
          dependencyType = ComputationBlobDependency.OUTPUT,
        )
      val inputRef =
        computationMutations.insertComputationBlobReference(
          localId = token.localId,
          stage = B,
          blobId = 5678L,
          pathToBlob = "/path/to/input/blob",
          dependencyType = ComputationBlobDependency.INPUT,
        )
      databaseClient.write(listOf(computation, stage, outputRef, inputRef))
      testClock.tickSeconds("write-blob-ref")
      database.writeOutputBlobReference(token, BlobRef(1234L, "/wrote/something/there"))
      token = token.withEditVersion(testClock["write-blob-ref"].toEpochMilli())
      assertQueryReturns(
        databaseClient,
        """
      SELECT b.ComputationId, b.ComputationStage, b.BlobId, b.PathToBlob, b.DependencyType,
             c.UpdateTime
      FROM ComputationBlobReferences AS b
      JOIN Computations AS c USING(ComputationId)
      ORDER BY ComputationStage, BlobId
      """
          .trimIndent(),
        Struct.newBuilder()
          .apply {
            set("ComputationId").to(token.localId)
            set("ComputationStage").toFakeStage(token.stage)
            set("BlobId").to(1234L)
            set("PathToBlob").to("/wrote/something/there")
            set("DependencyType").toInt64(ComputationBlobDependency.OUTPUT)
            set("UpdateTime").to(testClock["write-blob-ref"].toGcloudTimestamp())
          }
          .build(),
        Struct.newBuilder()
          .apply {
            set("ComputationId").to(token.localId)
            set("ComputationStage").toFakeStage(token.stage)
            set("BlobId").to(5678L)
            set("PathToBlob").to("/path/to/input/blob")
            set("DependencyType").toInt64(ComputationBlobDependency.INPUT)
            set("UpdateTime").to(testClock["write-blob-ref"].toGcloudTimestamp())
          }
          .build(),
      )

      // Can't update a blob with blank path.
      val blankPathException =
        assertFailsWith<IllegalArgumentException> {
          database.writeOutputBlobReference(token, BlobRef(1234L, ""))
        }
      assertThat(blankPathException).hasMessageThat().ignoringCase().contains("path")
      // Can't update an input blob
      val inputBlobException =
        assertFailsWith<IllegalStateException> {
          database.writeOutputBlobReference(token, BlobRef(5678L, "/wrote/something/there"))
        }
      assertThat(inputBlobException).hasMessageThat().ignoringCase().contains("input")
      // Blob id doesn't exist
      val blobNotFoundException =
        assertFailsWith<IllegalStateException> {
          database.writeOutputBlobReference(token, BlobRef(404L, "/wrote/something/there"))
        }
      assertThat(blobNotFoundException).hasMessageThat().ignoringCase().contains("404")
    }

  @Test
  fun `update computation details`() = runBlocking {
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val lockExpires = Instant.now().plusSeconds(300)
    val token =
      ComputationEditToken(
        localId = 4315,
        protocol = FakeProtocol.ZERO,
        stage = C,
        attempt = 2,
        editVersion = lastUpdated.toEpochMilli(),
        globalId = "55",
      )
    val computation =
      computationMutations.insertComputation(
        localId = token.localId,
        creationTime = lastUpdated.toGcloudTimestamp(),
        updateTime = lastUpdated.toGcloudTimestamp(),
        protocol = token.protocol,
        stage = token.stage,
        globalId = token.globalId,
        lockOwner = "PeterSpacemen",
        lockExpirationTime = lockExpires.toGcloudTimestamp(),
        details = FAKE_COMPUTATION_DETAILS,
      )

    testClock.tickSeconds("time-ended")
    databaseClient.write(listOf(computation))
    val newComputationDetails =
      FakeComputationDetails.newBuilder().apply { role = "something different" }.build()
    database.updateComputationDetails(token, newComputationDetails)
    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, UpdateTime, GlobalComputationId, LockOwner,
             LockExpirationTime, ComputationDetails, ComputationDetailsJSON
      FROM Computations
      ORDER BY ComputationId DESC
      """
        .trimIndent(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(token.localId)
          set("ComputationStage").toFakeStage(C)
          set("UpdateTime").to(testClock.last().toGcloudTimestamp())
          set("GlobalComputationId").to("55")
          set("LockOwner").to("PeterSpacemen")
          set("LockExpirationTime").to(lockExpires.toGcloudTimestamp())
          set("ComputationDetails").toProtoBytes(newComputationDetails)
          set("ComputationDetailsJSON").toProtoJson(newComputationDetails)
        }
        .build(),
    )
  }

  @Test
  fun `update computation details and requisition details`() = runBlocking {
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val lockExpires = Instant.now().plusSeconds(300)
    val requisitionKey1 = externalRequisitionKey {
      externalRequisitionId = "11111"
      requisitionFingerprint = "A".toByteStringUtf8()
    }
    val requisitionKey2 = externalRequisitionKey {
      externalRequisitionId = "22222"
      requisitionFingerprint = "B".toByteStringUtf8()
    }
    val token =
      ComputationEditToken(
        localId = 4315,
        protocol = FakeProtocol.ZERO,
        stage = C,
        attempt = 2,
        editVersion = lastUpdated.toEpochMilli(),
        globalId = "55",
      )
    val computation =
      computationMutations.insertComputation(
        localId = token.localId,
        creationTime = lastUpdated.toGcloudTimestamp(),
        updateTime = lastUpdated.toGcloudTimestamp(),
        protocol = token.protocol,
        stage = token.stage,
        globalId = token.globalId,
        lockOwner = "PeterSpacemen",
        lockExpirationTime = lockExpires.toGcloudTimestamp(),
        details = FAKE_COMPUTATION_DETAILS,
      )
    val requisition1 =
      computationMutations.insertRequisition(
        localComputationId = token.localId,
        requisitionId = 1L,
        externalRequisitionId = requisitionKey1.externalRequisitionId,
        requisitionFingerprint = requisitionKey1.requisitionFingerprint,
      )
    val requisition2 =
      computationMutations.insertRequisition(
        localComputationId = token.localId,
        requisitionId = 2L,
        externalRequisitionId = requisitionKey2.externalRequisitionId,
        requisitionFingerprint = requisitionKey2.requisitionFingerprint,
        pathToBlob = "foo/B",
      )
    val requisitionDetails1 =
      RequisitionDetails.newBuilder().apply { externalFulfillingDuchyId = "duchy-1" }.build()
    val requisitionDetails2 =
      RequisitionDetails.newBuilder().apply { externalFulfillingDuchyId = "duchy-2" }.build()

    testClock.tickSeconds("time-ended")
    databaseClient.write(listOf(computation, requisition1, requisition2))
    val newComputationDetails =
      FakeComputationDetails.newBuilder().apply { role = "something different" }.build()

    database.updateComputationDetails(
      token,
      newComputationDetails,
      listOf(
        requisitionEntry {
          key = requisitionKey1
          value = requisitionDetails1
        },
        requisitionEntry {
          key = requisitionKey2
          value = requisitionDetails2
        },
      ),
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, UpdateTime, GlobalComputationId, LockOwner,
             LockExpirationTime, ComputationDetails, ComputationDetailsJSON
      FROM Computations
      ORDER BY ComputationId DESC
      """
        .trimIndent(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(token.localId)
          set("ComputationStage").toFakeStage(C)
          set("UpdateTime").to(testClock.last().toGcloudTimestamp())
          set("GlobalComputationId").to("55")
          set("LockOwner").to("PeterSpacemen")
          set("LockExpirationTime").to(lockExpires.toGcloudTimestamp())
          set("ComputationDetails").toProtoBytes(newComputationDetails)
          set("ComputationDetailsJSON").toProtoJson(newComputationDetails)
        }
        .build(),
    )
    assertQueryReturns(
      databaseClient,
      """
      SELECT r.ComputationId, r.RequisitionId, r.ExternalRequisitionId, r.RequisitionFingerprint,
             r.PathToBlob, r.RequisitionDetails, c.UpdateTime
      FROM Requisitions AS r
      JOIN Computations AS c USING(ComputationId)
      ORDER BY RequisitionId
      """
        .trimIndent(),
      struct {
        set("ComputationId").to(token.localId)
        set("RequisitionId").to(1)
        set("ExternalRequisitionId").to(requisitionKey1.externalRequisitionId)
        set("RequisitionFingerprint").to(requisitionKey1.requisitionFingerprint.toGcloudByteArray())
        set("PathToBlob").to(null as String?)
        set("RequisitionDetails").toProtoBytes(requisitionDetails1)
        set("UpdateTime").to(testClock.last().toGcloudTimestamp())
      },
      struct {
        set("ComputationId").to(token.localId)
        set("RequisitionId").to(2)
        set("ExternalRequisitionId").to(requisitionKey2.externalRequisitionId)
        set("RequisitionFingerprint").to(requisitionKey2.requisitionFingerprint.toGcloudByteArray())
        set("PathToBlob").to("foo/B")
        set("RequisitionDetails").toProtoBytes(requisitionDetails2)
        set("UpdateTime").to(testClock.last().toGcloudTimestamp())
      },
    )
  }

  @Test
  fun `write requisition blob path`() = runBlocking {
    val lastUpdated = Instant.ofEpochMilli(12345678910L)
    val lockExpires = Instant.now().plusSeconds(300)
    val requisitionKey1 = externalRequisitionKey {
      externalRequisitionId = "11111"
      requisitionFingerprint = "A".toByteStringUtf8()
    }
    val requisitionKey2 = externalRequisitionKey {
      externalRequisitionId = "22222"
      requisitionFingerprint = "B".toByteStringUtf8()
    }
    val requisitionDetails1 =
      RequisitionDetails.newBuilder().apply { externalFulfillingDuchyId = "xxx" }.build()
    val token =
      ComputationEditToken(
        localId = 4315,
        protocol = FakeProtocol.ZERO,
        stage = C,
        attempt = 2,
        editVersion = lastUpdated.toEpochMilli(),
        globalId = "55",
      )
    val computation =
      computationMutations.insertComputation(
        localId = token.localId,
        creationTime = lastUpdated.toGcloudTimestamp(),
        updateTime = lastUpdated.toGcloudTimestamp(),
        protocol = token.protocol,
        stage = token.stage,
        globalId = token.globalId,
        lockOwner = "PeterSpacemen",
        lockExpirationTime = lockExpires.toGcloudTimestamp(),
        details = FAKE_COMPUTATION_DETAILS,
      )
    val requisition1 =
      computationMutations.insertRequisition(
        localComputationId = token.localId,
        requisitionId = 1L,
        externalRequisitionId = requisitionKey1.externalRequisitionId,
        requisitionFingerprint = requisitionKey1.requisitionFingerprint,
        requisitionDetails = requisitionDetails1,
      )
    val requisition2 =
      computationMutations.insertRequisition(
        localComputationId = token.localId,
        requisitionId = 2L,
        externalRequisitionId = requisitionKey2.externalRequisitionId,
        requisitionFingerprint = requisitionKey2.requisitionFingerprint,
        pathToBlob = "foo/B",
      )

    testClock.tickSeconds("time-ended")
    databaseClient.write(listOf(computation, requisition1, requisition2))

    assertQueryReturns(
      databaseClient,
      """
      SELECT r.ComputationId, r.RequisitionId, r.ExternalRequisitionId, r.RequisitionFingerprint,
             r.PathToBlob, r.RequisitionDetails, c.UpdateTime
      FROM Requisitions AS r
      JOIN Computations AS c USING(ComputationId)
      ORDER BY RequisitionId
      """
        .trimIndent(),
      struct {
        set("ComputationId").to(token.localId)
        set("RequisitionId").to(1)
        set("ExternalRequisitionId").to(requisitionKey1.externalRequisitionId)
        set("RequisitionFingerprint").to(requisitionKey1.requisitionFingerprint.toGcloudByteArray())
        set("PathToBlob").to(null as String?)
        set("RequisitionDetails").toProtoBytes(requisitionDetails1)
        set("UpdateTime").to(lastUpdated.toGcloudTimestamp())
      },
      struct {
        set("ComputationId").to(token.localId)
        set("RequisitionId").to(2)
        set("ExternalRequisitionId").to(requisitionKey2.externalRequisitionId)
        set("RequisitionFingerprint").to(requisitionKey2.requisitionFingerprint.toGcloudByteArray())
        set("PathToBlob").to("foo/B")
        set("RequisitionDetails").toProtoBytes(RequisitionDetails.getDefaultInstance())
        set("UpdateTime").to(lastUpdated.toGcloudTimestamp())
      },
    )

    database.writeRequisitionBlobPath(token, requisitionKey1, "this is a new path", "v2alpha")

    assertQueryReturns(
      databaseClient,
      """
      SELECT r.ComputationId, r.RequisitionId, r.ExternalRequisitionId, r.RequisitionFingerprint,
             r.PathToBlob, r.RequisitionDetails, c.UpdateTime
      FROM Requisitions AS r
      JOIN Computations AS c USING(ComputationId)
      ORDER BY RequisitionId
      """
        .trimIndent(),
      struct {
        set("ComputationId").to(token.localId)
        set("RequisitionId").to(1)
        set("ExternalRequisitionId").to(requisitionKey1.externalRequisitionId)
        set("RequisitionFingerprint").to(requisitionKey1.requisitionFingerprint.toGcloudByteArray())
        set("PathToBlob").to("this is a new path")
        set("RequisitionDetails")
          .toProtoBytes(requisitionDetails1.copy { publicApiVersion = "v2alpha" })
        set("UpdateTime").to(testClock.last().toGcloudTimestamp())
      },
      struct {
        set("ComputationId").to(token.localId)
        set("RequisitionId").to(2)
        set("ExternalRequisitionId").to(requisitionKey2.externalRequisitionId)
        set("RequisitionFingerprint").to(requisitionKey2.requisitionFingerprint.toGcloudByteArray())
        set("PathToBlob").to("foo/B")
        set("RequisitionDetails").toProtoBytes(RequisitionDetails.getDefaultInstance())
        set("UpdateTime").to(testClock.last().toGcloudTimestamp())
      },
    )
  }

  @Test
  fun `end successful computation`() = runBlocking {
    val token =
      ComputationEditToken(
        localId = 4315,
        protocol = FakeProtocol.ZERO,
        stage = C,
        attempt = 2,
        editVersion = testClock.last().toEpochMilli(),
        globalId = "55",
      )
    val computation =
      computationMutations.insertComputation(
        localId = token.localId,
        creationTime = testClock.last().toGcloudTimestamp(),
        updateTime = testClock.last().toGcloudTimestamp(),
        protocol = token.protocol,
        stage = C,
        globalId = token.globalId,
        lockOwner = WRITE_NULL_STRING,
        lockExpirationTime = testClock.last().toGcloudTimestamp(),
        details = FAKE_COMPUTATION_DETAILS,
      )
    val stage =
      computationMutations.insertComputationStage(
        localId = token.localId,
        stage = C,
        nextAttempt = 3,
        creationTime = testClock.last().toGcloudTimestamp(),
        details = computationMutations.detailsFor(C, FAKE_COMPUTATION_DETAILS),
      )
    val unfinishedPreviousAttempt =
      computationMutations.insertComputationStageAttempt(
        localId = token.localId,
        stage = token.stage,
        attempt = token.attempt.toLong() - 1,
        beginTime = testClock.last().toGcloudTimestamp(),
        details = ComputationStageAttemptDetails.getDefaultInstance(),
      )
    val attempt =
      computationMutations.insertComputationStageAttempt(
        localId = token.localId,
        stage = token.stage,
        attempt = token.attempt.toLong(),
        beginTime = testClock.last().toGcloudTimestamp(),
        details = ComputationStageAttemptDetails.getDefaultInstance(),
      )
    testClock.tickSeconds("time-ended")
    databaseClient.write(listOf(computation, stage, unfinishedPreviousAttempt, attempt))
    val expectedDetails =
      FAKE_COMPUTATION_DETAILS.toBuilder()
        .setEndReason(EndComputationReason.SUCCEEDED.toString())
        .build()
    database.endComputation(token, E, EndComputationReason.SUCCEEDED, FAKE_COMPUTATION_DETAILS)
    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, UpdateTime, GlobalComputationId, LockOwner,
             LockExpirationTime, ComputationDetails, ComputationDetailsJSON
      FROM Computations
      ORDER BY ComputationId DESC
      """
        .trimIndent(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(token.localId)
          set("ComputationStage").toFakeStage(E)
          set("UpdateTime").to(testClock.last().toGcloudTimestamp())
          set("GlobalComputationId").to("55")
          set("LockOwner").to(null as String?)
          set("LockExpirationTime").to(null as Timestamp?)
          set("ComputationDetails").toProtoBytes(expectedDetails)
          set("ComputationDetailsJSON").toProtoJson(expectedDetails)
        }
        .build(),
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, Attempt, BeginTime, EndTime, Details
      FROM ComputationStageAttempts
      ORDER BY ComputationStage, Attempt
      """
        .trimIndent(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(token.localId)
          set("ComputationStage").toFakeStage(C)
          set("Attempt").to(1)
          set("BeginTime").to(testClock["start"].toGcloudTimestamp())
          set("EndTime").to(testClock.last().toGcloudTimestamp())
          set("Details")
            .toProtoBytes(
              ComputationStageAttemptDetails.newBuilder()
                .apply { reasonEnded = ComputationStageAttemptDetails.EndReason.CANCELLED }
                .build()
            )
        }
        .build(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(token.localId)
          set("ComputationStage").toFakeStage(C)
          set("Attempt").to(2)
          set("BeginTime").to(testClock["start"].toGcloudTimestamp())
          set("EndTime").to(testClock.last().toGcloudTimestamp())
          set("Details")
            .toProtoBytes(
              ComputationStageAttemptDetails.newBuilder()
                .apply { reasonEnded = ComputationStageAttemptDetails.EndReason.SUCCEEDED }
                .build()
            )
        }
        .build(),
    )
  }

  @Test
  fun `end failed computation`() = runBlocking {
    val globalId = "474747"
    val token =
      ComputationEditToken(
        localId = 4315,
        protocol = FakeProtocol.ZERO,
        stage = C,
        attempt = 1,
        editVersion = testClock.last().toEpochMilli(),
        globalId = globalId,
      )
    val computation =
      computationMutations.insertComputation(
        localId = token.localId,
        creationTime = testClock.last().toGcloudTimestamp(),
        updateTime = testClock.last().toGcloudTimestamp(),
        protocol = token.protocol,
        stage = C,
        globalId = globalId,
        lockOwner = "lock-owner",
        lockExpirationTime = testClock.last().toGcloudTimestamp(),
        details = FAKE_COMPUTATION_DETAILS,
      )
    val stage =
      computationMutations.insertComputationStage(
        localId = token.localId,
        stage = C,
        nextAttempt = 3,
        creationTime = testClock.last().toGcloudTimestamp(),
        details = computationMutations.detailsFor(C, FAKE_COMPUTATION_DETAILS),
      )
    val attempt =
      computationMutations.insertComputationStageAttempt(
        localId = token.localId,
        stage = C,
        attempt = 1,
        beginTime = testClock.last().toGcloudTimestamp(),
        details = ComputationStageAttemptDetails.getDefaultInstance(),
      )
    testClock.tickSeconds("time-failed")
    databaseClient.write(listOf(computation, stage, attempt))
    val expectedDetails =
      FAKE_COMPUTATION_DETAILS.toBuilder()
        .setEndReason(EndComputationReason.FAILED.toString())
        .build()
    database.endComputation(token, E, EndComputationReason.FAILED, FAKE_COMPUTATION_DETAILS)
    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, UpdateTime, GlobalComputationId, LockOwner,
             LockExpirationTime, ComputationDetails, ComputationDetailsJSON
      FROM Computations
      ORDER BY ComputationId DESC
      """
        .trimIndent(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(token.localId)
          set("ComputationStage").toFakeStage(E)
          set("UpdateTime").to(testClock.last().toGcloudTimestamp())
          set("GlobalComputationId").to(globalId)
          set("LockOwner").to(null as String?)
          set("LockExpirationTime").to(null as Timestamp?)
          set("ComputationDetails").toProtoBytes(expectedDetails)
          set("ComputationDetailsJSON").toProtoJson(expectedDetails)
        }
        .build(),
    )
    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, Attempt, BeginTime, EndTime, Details
      FROM ComputationStageAttempts
      ORDER BY ComputationStage, Attempt
      """
        .trimIndent(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(token.localId)
          set("ComputationStage").toFakeStage(C)
          set("Attempt").to(1)
          set("BeginTime").to(testClock["start"].toGcloudTimestamp())
          set("EndTime").to(testClock.last().toGcloudTimestamp())
          set("Details")
            .toProtoBytes(
              ComputationStageAttemptDetails.newBuilder()
                .apply { reasonEnded = ComputationStageAttemptDetails.EndReason.ERROR }
                .build()
            )
        }
        .build(),
    )
  }

  @Test
  fun `endComputation throws for non-ending state`() =
    runBlocking<Unit> {
      val token =
        ComputationEditToken(
          localId = 4315,
          protocol = FakeProtocol.ZERO,
          stage = C,
          attempt = 1,
          editVersion = testClock.last().toEpochMilli(),
          globalId = "55",
        )
      assertFailsWith(IllegalArgumentException::class, "Invalid initial stage") {
        database.endComputation(token, B, EndComputationReason.CANCELED, FAKE_COMPUTATION_DETAILS)
      }
    }

  @Test
  fun `insert computation stat succeeds`() = runBlocking {
    val globalId = "474747"
    val localId = 4315L
    val computation =
      computationMutations.insertComputation(
        localId = localId,
        creationTime = testClock.last().toGcloudTimestamp(),
        updateTime = testClock.last().toGcloudTimestamp(),
        protocol = FakeProtocol.ZERO,
        stage = C,
        globalId = globalId,
        lockOwner = "lock-owner",
        lockExpirationTime = testClock.last().toGcloudTimestamp(),
        details = FAKE_COMPUTATION_DETAILS,
      )
    val stage =
      computationMutations.insertComputationStage(
        localId = localId,
        stage = C,
        nextAttempt = 3,
        creationTime = testClock.last().toGcloudTimestamp(),
        details = computationMutations.detailsFor(C, FAKE_COMPUTATION_DETAILS),
      )
    val attempt =
      computationMutations.insertComputationStageAttempt(
        localId = localId,
        stage = C,
        attempt = 2,
        beginTime = testClock.last().toGcloudTimestamp(),
        details = ComputationStageAttemptDetails.getDefaultInstance(),
      )
    testClock.tickSeconds("time-failed")
    databaseClient.write(listOf(computation, stage, attempt))
    database.insertComputationStat(
      localId = localId,
      stage = C,
      attempt = 2,
      metric = ComputationStatMetric("crypto_cpu_time_millis", 3125),
    )

    assertQueryReturns(
      databaseClient,
      """
      SELECT ComputationId, ComputationStage, Attempt, MetricName, MetricValue
      FROM ComputationStats
      """
        .trimIndent(),
      Struct.newBuilder()
        .apply {
          set("ComputationId").to(localId)
          set("ComputationStage").toFakeStage(C)
          set("Attempt").to(2)
          set("MetricName").to("crypto_cpu_time_millis")
          set("MetricValue").to(3125)
        }
        .build(),
    )
  }

  private fun <T> ValueBinder<T>.toFakeStage(value: FakeProtocolStages): T =
    to(ComputationProtocolStages.computationStageEnumToLongValues(value).stage)
}

private fun <ProtocolT, StageT> ComputationEditToken<ProtocolT, StageT>.withEditVersion(
  editVersion: Long
) = ComputationEditToken(localId, protocol, stage, attempt, editVersion, globalId)
