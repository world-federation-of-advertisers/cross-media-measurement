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

package org.wfanet.measurement.integration.common

import java.time.ZoneOffset
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupKey
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.loadtest.dataprovider.SyntheticGeneratorEventQuery

/** [SyntheticGeneratorEventQuery] for a test environment with EDP simulators. */
class EventQuery(
  syntheticPopulationSpec: SyntheticPopulationSpec,
  val eventGroupSpecByDataProvider: Map<DataProviderKey, SyntheticEventGroupSpec>,
) :
  SyntheticGeneratorEventQuery(syntheticPopulationSpec, TestEvent.getDescriptor(), ZoneOffset.UTC) {
  /**
   * @param syntheticPopulationSpec Synthetic population spec used by the EDP simulators.
   * @param syntheticEventGroupSpecs Synthetic event groups specs used by the EDP simulators, in
   *   order.
   * @param dataProviderNames DataProvider resource names of the EDP simulators, in order.
   */
  constructor(
    syntheticPopulationSpec: SyntheticPopulationSpec,
    syntheticEventGroupSpecs: List<SyntheticEventGroupSpec>,
    dataProviderNames: Iterable<String>,
  ) : this(
    syntheticPopulationSpec,
    buildEventGroupSpecsByDataProvider(syntheticEventGroupSpecs, dataProviderNames),
  )

  override fun getSyntheticDataSpec(eventGroup: EventGroup): SyntheticEventGroupSpec {
    val eventGroupKey =
      requireNotNull(EventGroupKey.fromName(eventGroup.name)) {
        "Invalid EventGroup resource name ${eventGroup.name}"
      }
    return eventGroupSpecByDataProvider.getValue(eventGroupKey.parentKey)
  }

  companion object {
    private fun buildEventGroupSpecsByDataProvider(
      syntheticEventGroupSpecs: List<SyntheticEventGroupSpec>,
      dataProviderNames: Iterable<String>,
    ): Map<DataProviderKey, SyntheticEventGroupSpec> {
      return buildMap {
        dataProviderNames.forEachIndexed { index, dataProviderName ->
          val key =
            requireNotNull(DataProviderKey.fromName(dataProviderName)) {
              "Invalid DataProvider resource name $dataProviderName"
            }
          val specIndex = index % syntheticEventGroupSpecs.size
          put(key, syntheticEventGroupSpecs[specIndex])
        }
      }
    }
  }
}
