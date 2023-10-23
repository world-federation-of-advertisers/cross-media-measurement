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

private val parser = ResourceNameParser("measurementConsumers/{measurement_consumer}/publicKey")

/** [ResourceKey] of a MeasurementConsumer PublicKey. */
data class MeasurementConsumerPublicKeyKey(override val parentKey: MeasurementConsumerKey) :
  PublicKeyKey {
  constructor(measurementConsumerId: String) : this(MeasurementConsumerKey(measurementConsumerId))

  val measurementConsumerId: String
    get() = parentKey.measurementConsumerId

  override fun toName(): String {
    return parser.assembleName(mapOf(IdVariable.MEASUREMENT_CONSUMER to measurementConsumerId))
  }

  companion object FACTORY : ResourceKey.Factory<MeasurementConsumerPublicKeyKey> {
    val defaultValue = MeasurementConsumerPublicKeyKey("")

    override fun fromName(resourceName: String): MeasurementConsumerPublicKeyKey? {
      return parser.parseIdVars(resourceName)?.let {
        MeasurementConsumerPublicKeyKey(it.getValue(IdVariable.MEASUREMENT_CONSUMER))
      }
    }
  }
}
