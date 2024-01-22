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

private val parser =
  ResourceNameParser(
    "modelProviders/{model_provider}/modelSuites/{model_suite}/modelLines/{model_line}/modelOutages/{model_outage}"
  )

/** [ResourceKey] of a Model Outage. */
data class ModelOutageKey(
  val modelProviderId: String,
  val modelSuiteId: String,
  val modelLineId: String,
  val modelOutageId: String,
) : ResourceKey {
  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.MODEL_PROVIDER to modelProviderId,
        IdVariable.MODEL_SUITE to modelSuiteId,
        IdVariable.MODEL_LINE to modelLineId,
        IdVariable.MODEL_OUTAGE to modelOutageId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<ModelOutageKey> {
    val defaultValue = ModelOutageKey("", "", "", "")

    override fun fromName(resourceName: String): ModelOutageKey? {
      return parser.parseIdVars(resourceName)?.let {
        ModelOutageKey(
          it.getValue(IdVariable.MODEL_PROVIDER),
          it.getValue(IdVariable.MODEL_SUITE),
          it.getValue(IdVariable.MODEL_LINE),
          it.getValue(IdVariable.MODEL_OUTAGE),
        )
      }
    }
  }
}
