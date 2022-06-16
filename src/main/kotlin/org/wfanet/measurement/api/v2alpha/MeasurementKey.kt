// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.api.v2alpha

import org.wfanet.measurement.common.ResourceNameParser
import org.wfanet.measurement.common.api.ResourceKey

private val parser =
  ResourceNameParser("measurementConsumers/{measurement_consumer}/measurements/{measurement}")

/** [ResourceKey] of a Measurement. */
data class MeasurementKey(val measurementConsumerId: String, val measurementId: String) :
  ResourceKey {
  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.MEASUREMENT_CONSUMER to measurementConsumerId,
        IdVariable.MEASUREMENT to measurementId
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<MeasurementKey> {
    val defaultValue = MeasurementKey("", "")

    override fun fromName(resourceName: String): MeasurementKey? {
      return parser.parseIdVars(resourceName)?.let {
        MeasurementKey(
          it.getValue(IdVariable.MEASUREMENT_CONSUMER),
          it.getValue(IdVariable.MEASUREMENT)
        )
      }
    }
  }
}
