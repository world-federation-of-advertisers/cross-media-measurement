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

private val parser =
  ResourceNameParser(
    "modelProviders/{model_provider}/modelSuites/{model_suite}/modelLines/{model_line}/modelRollouts/{model_rollout}"
  )

/** [ResourceKey] of a Model Rollout. */
data class ModelRolloutKey(override val parentKey: ModelLineKey, val modelRolloutId: String) :
  ResourceKey, ChildResourceKey {
  val modelProviderId: String = parentKey.modelProviderId
  val modelSuiteId: String = parentKey.modelSuiteId
  val modelLineId: String = parentKey.modelLineId

  constructor(
    modelProviderId: String,
    modelSuiteId: String,
    modelLineId: String,
    modelRolloutId: String,
  ) : this(ModelLineKey(modelProviderId, modelSuiteId, modelLineId), modelRolloutId)

  override fun toName(): String {
    return parser.assembleName(
      mapOf(
        IdVariable.MODEL_PROVIDER to modelProviderId,
        IdVariable.MODEL_SUITE to modelSuiteId,
        IdVariable.MODEL_LINE to modelLineId,
        IdVariable.MODEL_ROLLOUT to modelRolloutId,
      )
    )
  }

  companion object FACTORY : ResourceKey.Factory<ModelRolloutKey> {
    val defaultValue = ModelRolloutKey(ModelLineKey.defaultValue, "")

    override fun fromName(resourceName: String): ModelRolloutKey? {
      return parser.parseIdVars(resourceName)?.let {
        ModelRolloutKey(
          it.getValue(IdVariable.MODEL_PROVIDER),
          it.getValue(IdVariable.MODEL_SUITE),
          it.getValue(IdVariable.MODEL_LINE),
          it.getValue(IdVariable.MODEL_ROLLOUT),
        )
      }
    }
  }
}
