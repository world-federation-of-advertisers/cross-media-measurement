package org.wfanet.measurement.service.internal.kingdom

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.test.assertEquals
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.db.kingdom.testing.FakeKingdomRelationalDatabase
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportConfigScheduleStorageGrpcKt.ReportConfigScheduleStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamReadyReportConfigSchedulesRequest
import org.wfanet.measurement.service.testing.GrpcTestServerRule

@RunWith(JUnit4::class)
class ReportConfigScheduleStorageServiceTest {
  companion object {
    val SCHEDULE1 = ReportConfigSchedule.newBuilder().setExternalScheduleId(1).build()
    val SCHEDULE2 = ReportConfigSchedule.newBuilder().setExternalScheduleId(2).build()
  }

  private val fakeKingdomRelationalDatabase = FakeKingdomRelationalDatabase()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    listOf(ReportConfigScheduleStorageService(fakeKingdomRelationalDatabase))
  }

  private val stub: ReportConfigScheduleStorageCoroutineStub by lazy {
    ReportConfigScheduleStorageCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun streamReadySchedules() = runBlocking<Unit> {
    val limit = 123L
    val request = StreamReadyReportConfigSchedulesRequest.newBuilder().setLimit(limit).build()

    var capturedLimit: Long? = null
    fakeKingdomRelationalDatabase.streamReadySchedulesFn = {
      capturedLimit = it
      flowOf(SCHEDULE1, SCHEDULE2)
    }

    assertThat(stub.streamReadyReportConfigSchedules(request).toList())
      .containsExactly(SCHEDULE1, SCHEDULE2)

    assertEquals(limit, capturedLimit)
  }
}
