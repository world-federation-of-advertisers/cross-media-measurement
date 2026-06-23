/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.subpoolassigner

import com.google.common.truth.Truth.assertThat
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.data.LongPointData
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.rawimpressions.DigestedEvent
import org.wfanet.measurement.edpaggregator.rawimpressions.EventIdDigest
import org.wfanet.measurement.edpaggregator.rawimpressions.LabelerInputMapper
import org.wfanet.measurement.edpaggregator.rawimpressions.ParquetDigestedEvent
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.ActiveWindow
import org.wfanet.measurement.storage.ParquetValue
import org.wfanet.measurement.storage.parquetValue
import org.wfanet.virtualpeople.common.LabelerInput

@RunWith(JUnit4::class)
class SubpoolAssignmentSinkTest {
  private val mapper = LabelerInputMapper(mapOf("timestamp_usec" to "ts", "event_id.id" to "eid"))

  private lateinit var metricReader: InMemoryMetricReader
  private lateinit var testMetrics: SubpoolAssignerMetrics

  @Before
  fun setUp() {
    metricReader = InMemoryMetricReader.create()
    val meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build()
    testMetrics = SubpoolAssignerMetrics(meterProvider.get("test"))
  }

  private fun newSink(
    accumulator: SubpoolFingerprintsAccumulator,
    labeler: PoolEmitLabeler,
    activeWindow: ActiveWindow,
  ): SubpoolAssignmentSink =
    SubpoolAssignmentSink(mapper, labeler, accumulator, activeWindow, testMetrics)

  private fun event(eid: String, ts: Long, hi: Long, lo: Int): ParquetDigestedEvent =
    DigestedEvent(
      row =
        mapOf<String, ParquetValue>(
          "eid" to parquetValue { stringValue = eid },
          "ts" to parquetValue { int64Value = ts },
        ),
      digest = EventIdDigest(hi, lo),
    )

  /** Cumulative value of the long-sum counter named [name], or 0 if not recorded. */
  private fun counterValue(name: String): Long {
    val metric = metricReader.collectAllMetrics().find { it.name == name } ?: return 0L
    return (metric.longSumData.points.first() as LongPointData).value
  }

  @Test
  fun `routes labeled events to their subpools keyed by digest`() =
    runBlocking<Unit> {
      val accumulator = SubpoolFingerprintsAccumulator()
      // Subpool = timestamp_usec value, for deterministic routing.
      val labeler = FakePoolEmitLabeler { listOf(it.timestampUsec) }
      val sink = newSink(accumulator, labeler, ActiveWindow(0, Long.MAX_VALUE))

      sink.processBatch(
        listOf(event("a", ts = 1L, hi = 10L, lo = 1), event("b", ts = 2L, hi = 20L, lo = 2))
      )

      assertThat(sink.labeled).isEqualTo(2L)
      assertThat(accumulator.subpoolIds()).containsExactly(1L, 2L)
      assertThat(accumulator.size(1L)).isEqualTo(1L)
      assertThat(accumulator.size(2L)).isEqualTo(1L)
    }

  @Test
  fun `tracks the max event timestamp over labeled events only`() =
    runBlocking<Unit> {
      val accumulator = SubpoolFingerprintsAccumulator()
      // ts=300 routes to no subpool, so it must not raise the max past the labeled ts=250.
      val labeler = FakePoolEmitLabeler {
        if (it.timestampUsec == 300L) emptyList() else listOf(1L)
      }
      val sink = newSink(accumulator, labeler, ActiveWindow(0, Long.MAX_VALUE))

      sink.processBatch(
        listOf(
          event("a", ts = 100L, hi = 1L, lo = 1),
          event("c", ts = 250L, hi = 2L, lo = 2),
          event("b", ts = 200L, hi = 3L, lo = 3),
          event("unrouted", ts = 300L, hi = 4L, lo = 4),
        )
      )

      assertThat(sink.maxTimestampUsec).isEqualTo(250L)
    }

  @Test
  fun `max event timestamp is null when no event is labeled`() =
    runBlocking<Unit> {
      val accumulator = SubpoolFingerprintsAccumulator()
      val labeler = FakePoolEmitLabeler { emptyList() }
      val sink = newSink(accumulator, labeler, ActiveWindow(0, Long.MAX_VALUE))

      sink.processBatch(listOf(event("a", ts = 100L, hi = 1L, lo = 1)))

      assertThat(sink.maxTimestampUsec).isNull()
    }

  @Test
  fun `drops events outside the active window`() =
    runBlocking<Unit> {
      val accumulator = SubpoolFingerprintsAccumulator()
      val labeler = FakePoolEmitLabeler { listOf(0L) }
      // Window [100, 200): ts=50 and ts=200 are out; ts=150 is in.
      val sink = newSink(accumulator, labeler, ActiveWindow(100, 200))

      sink.processBatch(
        listOf(
          event("early", ts = 50L, hi = 1L, lo = 1),
          event("in", ts = 150L, hi = 2L, lo = 2),
          event("late", ts = 200L, hi = 3L, lo = 3),
        )
      )

      assertThat(sink.droppedOutsideWindow).isEqualTo(2L)
      assertThat(sink.labeled).isEqualTo(1L)
      assertThat(accumulator.size(0L)).isEqualTo(1L)
    }

  @Test
  fun `counts events the labeler routes to no subpool as unrouted`() =
    runBlocking<Unit> {
      val accumulator = SubpoolFingerprintsAccumulator()
      val labeler = FakePoolEmitLabeler { emptyList() }
      val sink = newSink(accumulator, labeler, ActiveWindow(0, Long.MAX_VALUE))

      sink.processBatch(listOf(event("a", ts = 1L, hi = 1L, lo = 1)))

      assertThat(sink.unrouted).isEqualTo(1L)
      assertThat(sink.labeled).isEqualTo(0L)
      assertThat(accumulator.subpoolIds()).isEmpty()
    }

  @Test
  fun `records a fanned-out fingerprint in every subpool it routes to`() =
    runBlocking<Unit> {
      val accumulator = SubpoolFingerprintsAccumulator()
      val labeler = FakePoolEmitLabeler { listOf(7L, 8L, 9L) }
      val sink = newSink(accumulator, labeler, ActiveWindow(0, Long.MAX_VALUE))

      sink.processBatch(listOf(event("a", ts = 1L, hi = 99L, lo = 99)))

      assertThat(sink.labeled).isEqualTo(1L)
      assertThat(accumulator.subpoolIds()).containsExactly(7L, 8L, 9L)
    }

  @Test
  fun `records OpenTelemetry counters for each outcome`() =
    runBlocking<Unit> {
      val accumulator = SubpoolFingerprintsAccumulator()
      // In-window event at ts=150 routes to no subpool (unrouted); other in-window events route
      // to 1.
      val labeler = FakePoolEmitLabeler {
        if (it.timestampUsec == 150L) emptyList() else listOf(1L)
      }
      val sink = newSink(accumulator, labeler, ActiveWindow(100, 200))

      sink.processBatch(
        listOf(
          event("labeled", ts = 160L, hi = 1L, lo = 1), // in-window, routes -> labeled
          event("unrouted", ts = 150L, hi = 2L, lo = 2), // in-window, empty -> unrouted
          event("early", ts = 50L, hi = 3L, lo = 3), // out of window -> dropped
          event("late", ts = 250L, hi = 4L, lo = 4), // out of window -> dropped
        )
      )

      assertThat(counterValue("edpa.subpool_assigner.events_labeled")).isEqualTo(1L)
      assertThat(counterValue("edpa.subpool_assigner.events_dropped_outside_window")).isEqualTo(2L)
      assertThat(counterValue("edpa.subpool_assigner.events_unrouted")).isEqualTo(1L)
      assertThat(sink.labeled).isEqualTo(1L)
      assertThat(sink.droppedOutsideWindow).isEqualTo(2L)
      assertThat(sink.unrouted).isEqualTo(1L)
    }

  private class FakePoolEmitLabeler(private val routing: (LabelerInput) -> List<Long>) :
    PoolEmitLabeler {
    override fun emit(input: LabelerInput): List<Long> = routing(input)
  }
}
