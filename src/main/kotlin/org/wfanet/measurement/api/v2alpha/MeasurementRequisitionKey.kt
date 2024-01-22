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
import org.wfanet.measurement.common.api.ResourceKey

/** [ResourceKey] of a Requisition with a Measurement parent. */
data class MeasurementRequisitionKey(
  override val parentKey: MeasurementKey,
  override val requisitionId: String,
) : RequisitionKey {
  constructor(
    measurementConsumerId: String,
    measurementId: String,
    requisitionId: String,
  ) : this(MeasurementKey(measurementConsumerId, measurementId), requisitionId)

  val measurementConsumerId: String
    get() = parentKey.measurementConsumerId

  val measurementId: String
    get() = parentKey.measurementId

  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.MEASUREMENT_CONSUMER to measurementConsumerId,
        IdVariable.MEASUREMENT to measurementId,
        IdVariable.REQUISITION to requisitionId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<MeasurementRequisitionKey> {
    private val parser =
      ResourceNameParser(
        "measurementConsumers/{measurement_consumer}/measurements/{measurement}/requisitions/{requisition}"
      )

    val defaultValue = MeasurementRequisitionKey("", "", "")

    override fun fromName(resourceName: String): MeasurementRequisitionKey? {
      val idVars = parser.parseIdVars(resourceName) ?: return null
      return MeasurementRequisitionKey(
        idVars.getValue(IdVariable.MEASUREMENT_CONSUMER),
        idVars.getValue(IdVariable.MEASUREMENT),
        idVars.getValue(IdVariable.REQUISITION),
      )
    }
  }
}
