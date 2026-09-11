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

package org.wfanet.measurement.common.telemetry

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reportingMetadata
import org.wfanet.measurement.api.v2alpha.measurementSpec

@RunWith(JUnit4::class)
class ReportTraceAttributesTest {
  @Test
  fun `fromMeasurementSpec returns reporting resource attributes`() {
    val measurementSpec = measurementSpec {
      reportingMetadata = reportingMetadata {
        basicReport = "measurementConsumers/123/basicReports/456"
        report = "measurementConsumers/123/reports/789"
        metric = "measurementConsumers/123/metrics/012"
      }
    }

    val attributes = ReportTraceAttributes.fromMeasurementSpec(measurementSpec)

    assertThat(attributes.get(ReportTraceAttributes.BASIC_REPORT_NAME))
      .isEqualTo("measurementConsumers/123/basicReports/456")
    assertThat(attributes.get(ReportTraceAttributes.REPORT_NAME))
      .isEqualTo("measurementConsumers/123/reports/789")
    assertThat(attributes.get(ReportTraceAttributes.METRIC_NAME))
      .isEqualTo("measurementConsumers/123/metrics/012")
  }

  @Test
  fun `fromMeasurementSpec omits unset reporting resource attributes`() {
    val attributes = ReportTraceAttributes.fromMeasurementSpec(measurementSpec {})

    assertThat(attributes.asMap()).isEmpty()
  }

  @Test
  fun `errorType includes enclosing class for nested exception`() {
    assertThat(ReportTraceAttributes.errorType(TestException.Nested()))
      .isEqualTo("TestException.Nested")
  }
}

private sealed class TestException : Exception() {
  class Nested : TestException()
}
