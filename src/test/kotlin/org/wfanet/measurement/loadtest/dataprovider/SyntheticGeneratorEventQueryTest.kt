// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Descriptors
import com.google.type.interval
import java.nio.file.Paths
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.toProtoTime

@RunWith(JUnit4::class)
class SyntheticGeneratorEventQueryTest {
  private class EventQueryImpl(
    syntheticPopulationSpec: SyntheticPopulationSpec,
    private val syntheticEventGroupSpec: SyntheticEventGroupSpec,
    eventMessageDescriptor: Descriptors.Descriptor,
  ) : SyntheticGeneratorEventQuery(syntheticPopulationSpec, eventMessageDescriptor, ZONE_ID) {
    override fun getSyntheticDataSpec(eventGroup: EventGroup): SyntheticEventGroupSpec {
      return syntheticEventGroupSpec
    }
  }

  @Test
  fun `getLabeledEvents returns events distributed across date range`() {
    val syntheticPopulationSpec: SyntheticPopulationSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve("small_population_spec.textproto").toFile(),
        SyntheticPopulationSpec.getDefaultInstance(),
      )
    val syntheticEventGroupSpec: SyntheticEventGroupSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve("small_data_spec.textproto").toFile(),
        SyntheticEventGroupSpec.getDefaultInstance(),
      )
    val eventQuery: SyntheticGeneratorEventQuery =
      EventQueryImpl(syntheticPopulationSpec, syntheticEventGroupSpec, TestEvent.getDescriptor())
    val dateRange = syntheticEventGroupSpec.getDateSpecs(0).dateRange
    val startTime =
      LocalDate.of(dateRange.start.year, dateRange.start.month, dateRange.start.day)
        .atStartOfDay(ZONE_ID)
        .toInstant()
    val endTime =
      LocalDate.of(
          dateRange.endExclusive.year,
          dateRange.endExclusive.month,
          dateRange.endExclusive.day - 1,
        )
        .atTime(23, 59, 59)
        .atZone(ZONE_ID)
        .toInstant()
    val eventGroupSpec =
      EventQuery.EventGroupSpec(
        EventGroup.getDefaultInstance(),
        RequisitionSpecKt.EventGroupEntryKt.value {
          collectionInterval = interval {
            this.startTime = startTime.toProtoTime()
            this.endTime = endTime.toProtoTime()
          }
        },
      )
    val events = eventQuery.getLabeledEvents(eventGroupSpec)
    assertThat(events.toList()).hasSize(8001)
  }

  companion object {
    private const val REPO_NAME = "wfa_measurement_system"
    private val TEST_DATA_PATH =
      Paths.get(REPO_NAME, "src", "main", "proto", "wfa", "measurement", "loadtest", "dataprovider")
    private val TEST_DATA_RUNTIME_PATH = getRuntimePath(TEST_DATA_PATH)!!

    private val ZONE_ID: ZoneId = ZoneOffset.UTC
  }
}
