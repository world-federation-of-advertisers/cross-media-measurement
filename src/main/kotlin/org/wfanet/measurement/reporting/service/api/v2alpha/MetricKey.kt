/*
 * Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ChildResourceKey
import org.wfanet.measurement.common.api.ResourceKey

/** [ResourceKey] of a Metric. */
data class MetricKey(override val parentKey: MeasurementConsumerKey, val metricId: String) :
  ChildResourceKey {
  constructor(
    cmmsMeasurementConsumerId: String,
    metricId: String,
  ) : this(MeasurementConsumerKey(cmmsMeasurementConsumerId), metricId)

  val cmmsMeasurementConsumerId: String
    get() = parentKey.measurementConsumerId

  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.MEASUREMENT_CONSUMER to cmmsMeasurementConsumerId,
        IdVariable.METRIC to metricId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<MetricKey> {
    const val PATTERN = "${MeasurementConsumerKey.PATTERN}/metrics/{metric}"
    private val parser = ResourceNameParser(PATTERN)

    override fun fromName(resourceName: String): MetricKey? {
      val idVars: Map<IdVariable, String> = parser.parseIdVars(resourceName) ?: return null
      return MetricKey(
        idVars.getValue(IdVariable.MEASUREMENT_CONSUMER),
        idVars.getValue(IdVariable.METRIC),
      )
    }
  }
}
