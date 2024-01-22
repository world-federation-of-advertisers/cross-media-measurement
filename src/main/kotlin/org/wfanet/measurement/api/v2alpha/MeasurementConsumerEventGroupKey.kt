/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.api.v2alpha

import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ChildResourceKey
import org.wfanet.measurement.common.api.ResourceKey

/** [ResourceKey] of an EventGroup with a MeasurementConsumer as the parent. */
data class MeasurementConsumerEventGroupKey(
  val measurementConsumerId: String,
  val eventGroupId: String,
) : ChildResourceKey {
  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.MEASUREMENT_CONSUMER to measurementConsumerId,
        IdVariable.EVENT_GROUP to eventGroupId,
      )
    )
  }

  override val parentKey = MeasurementConsumerKey(measurementConsumerId)

  companion object FACTORY : ResourceKey.Factory<MeasurementConsumerEventGroupKey> {
    private val parser =
      ResourceNameParser("measurementConsumers/{measurement_consumer}/eventGroups/{event_group}")

    val defaultValue = MeasurementConsumerEventGroupKey("", "")

    override fun fromName(resourceName: String): MeasurementConsumerEventGroupKey? {
      return parser.parseIdVars(resourceName)?.let {
        MeasurementConsumerEventGroupKey(
          it.getValue(IdVariable.MEASUREMENT_CONSUMER),
          it.getValue(IdVariable.EVENT_GROUP),
        )
      }
    }
  }
}
