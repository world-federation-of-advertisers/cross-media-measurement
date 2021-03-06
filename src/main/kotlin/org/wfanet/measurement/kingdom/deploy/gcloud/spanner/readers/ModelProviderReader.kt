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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Struct
import org.wfanet.measurement.internal.kingdom.ModelProvider

class ModelProviderReader : SpannerReader<ModelProviderReader.Result>() {
  data class Result(val modelProvider: ModelProvider, val modelProviderId: Long)

  override val baseSql: String =
    """
    SELECT
      ModelProviders.ModelProviderId,
      ModelProviders.ExternalModelProviderId,
    FROM ModelProviders
    """.trimIndent()

  override val externalIdColumn: String = "ModelProviders.ExternalModelProviderId"

  override suspend fun translate(struct: Struct): Result =
    Result(buildModelProvider(struct), struct.getLong("ModelProviderId"))

  private fun buildModelProvider(struct: Struct): ModelProvider {
    return ModelProvider.newBuilder()
      .apply { externalModelProviderId = struct.getLong("ExternalModelProviderId") }
      .build()
  }
}
