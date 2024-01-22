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

private val parser = ResourceNameParser("dataProviders/{data_provider}/modelShards/{model_shard}")

/** [ResourceKey] of a Model Shard. */
data class ModelShardKey(val dataProviderId: String, val modelShardId: String) : ResourceKey {
  override fun toName(): String {
    return parser.assembleName(
      mapOf(IdVariable.DATA_PROVIDER to dataProviderId, IdVariable.MODEL_SHARD to modelShardId)
    )
  }

  companion object FACTORY : ResourceKey.Factory<ModelShardKey> {
    val defaultValue = ModelShardKey("", "")

    override fun fromName(resourceName: String): ModelShardKey? {
      return parser.parseIdVars(resourceName)?.let {
        ModelShardKey(it.getValue(IdVariable.DATA_PROVIDER), it.getValue(IdVariable.MODEL_SHARD))
      }
    }
  }
}
