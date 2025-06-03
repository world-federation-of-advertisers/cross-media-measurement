/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.requisitionfetcher

import com.google.common.truth.Truth.assertThat
import java.time.Clock
import java.time.Duration
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.times
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupMapEntry

@RunWith(JUnit4::class)
class SingleRequisitionGrouperTest : AbstractRequisitionGrouperTest() {

  override val requisitionGrouper: RequisitionGrouper =
    SingleRequisitionGrouper(
      privateEncryptionKey = EDP_DATA.privateEncryptionKey,
      eventGroupsClient = eventGroupsStub,
      requisitionsClient = requisitionsStub,
      throttler = MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofSeconds(1L)),
    )

  @Test
  fun `able to map Requisition to GroupedRequisisions`() {
    val groupedRequisitions: List<GroupedRequisitions> =
      requisitionGrouper.groupRequisitions(listOf(REQUISITION, REQUISITION))
    assertThat(groupedRequisitions).hasSize(2)
    groupedRequisitions.forEach { groupedRequisition: GroupedRequisitions ->
      assertThat(groupedRequisition.eventGroupMapList.single())
        .isEqualTo(
          eventGroupMapEntry {
            eventGroup = "dataProviders/someDataProvider/eventGroups/name"
            eventGroupReferenceId = "some-event-group-reference-id"
          }
        )
      assertThat(
          groupedRequisition.collectionIntervals["some-event-group-reference-id"]!!.startTime
        )
        .isEqualTo(TIME_RANGE.start.toProtoTime())
      assertThat(groupedRequisition.collectionIntervals["some-event-group-reference-id"]!!.endTime)
        .isEqualTo(TIME_RANGE.endExclusive.toProtoTime())
      assertThat(
          groupedRequisition.requisitionsList
            .map { it.requisition.unpack(Requisition::class.java) }
            .single()
        )
        .isEqualTo(REQUISITION)
    }
  }
}
