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

package org.wfanet.measurement.edpaggregator.dataavailability

import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.Meter

/** OpenTelemetry instruments used when publishing EventGroup activity. */
class EventGroupActivityPublisherMetrics(meter: Meter) {
  val mapRecordsMissingEntityKeyCounter: LongCounter =
    meter
      .counterBuilder("edpa.event_group_activity.map_records_missing_entity_key")
      .setDescription("Number of EventGroup map records skipped because entity_key is absent")
      .build()

  val unmappedEntityKeysCounter: LongCounter =
    meter
      .counterBuilder("edpa.event_group_activity.unmapped_entity_keys")
      .setDescription("Number of impression metadata entity keys absent from the EventGroup map")
      .build()

  val eventGroupsNotFoundCounter: LongCounter =
    meter
      .counterBuilder("edpa.event_group_activity.event_groups_not_found")
      .setDescription(
        "Number of stale EventGroup mappings skipped because the resource was not found"
      )
      .build()

  companion object {
    internal val DATA_PROVIDER_NAME_ATTR: AttributeKey<String> =
      AttributeKey.stringKey("data_provider_name")
  }
}
