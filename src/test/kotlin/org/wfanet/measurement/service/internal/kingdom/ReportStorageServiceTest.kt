package org.wfanet.measurement.service.internal.kingdom

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.Instant
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.kingdom.StreamReportsFilter
import org.wfanet.measurement.db.kingdom.streamReportsFilter
import org.wfanet.measurement.db.kingdom.testing.FakeKingdomRelationalDatabase
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt
import org.wfanet.measurement.internal.kingdom.StreamReportsRequest
import org.wfanet.measurement.service.testing.GrpcTestServerRule

@RunWith(JUnit4::class)
class ReportStorageServiceTest {

  companion object {
    val REPORT: Report = Report.newBuilder().apply {
      externalAdvertiserId = 1
      externalReportConfigId = 2
      externalScheduleId = 3
      externalReportId = 4
      createTimeBuilder.seconds = 567
      state = Report.ReportState.FAILED
    }.build()
  }

  private val fakeKingdomRelationalDatabase = FakeKingdomRelationalDatabase()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    listOf(ReportStorageService(fakeKingdomRelationalDatabase))
  }

  private val stub: ReportStorageGrpcKt.ReportStorageCoroutineStub by lazy {
    ReportStorageGrpcKt.ReportStorageCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun streamReports() = runBlocking<Unit> {
    val request: StreamReportsRequest =
      StreamReportsRequest.newBuilder().apply {
        limit = 10
        filterBuilder.apply {
          addExternalAdvertiserIds(1)
          addExternalReportConfigIds(2)
          addExternalReportConfigIds(3)
          addExternalScheduleIds(4)
          addStates(Report.ReportState.AWAITING_REQUISITIONS)
          createdAfterBuilder.seconds = 12345
        }
      }.build()

    var capturedFilter: StreamReportsFilter? = null
    var capturedLimit: Long = 0

    fakeKingdomRelationalDatabase.streamReportsFn = { filter, limit ->
      capturedFilter = filter
      capturedLimit = limit
      flowOf(REPORT, REPORT)
    }

    assertThat(stub.streamReports(request).toList())
      .containsExactly(REPORT, REPORT)

    val expectedFilter = streamReportsFilter(
      externalAdvertiserIds = listOf(ExternalId(1)),
      externalReportConfigIds = listOf(ExternalId(2), ExternalId(3)),
      externalScheduleIds = listOf(ExternalId(4)),
      states = listOf(Report.ReportState.AWAITING_REQUISITIONS),
      createdAfter = Instant.ofEpochSecond(12345)
    )

    assertThat(capturedFilter?.clauses).containsExactlyElementsIn(expectedFilter.clauses)
    assertThat(capturedLimit).isEqualTo(10)
  }
}
