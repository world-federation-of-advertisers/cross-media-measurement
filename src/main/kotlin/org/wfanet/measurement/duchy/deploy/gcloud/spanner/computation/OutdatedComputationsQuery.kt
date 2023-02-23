// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Struct
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.common.SqlBasedQuery
import org.wfanet.measurement.gcloud.spanner.statement

class OutdatedComputationsQuery(before: Timestamp) : SqlBasedQuery<String> {
  companion object {
    private val parameterizedQueryString =
      """
        SELECT GlobalComputationId,
        FROM Computations
        WHERE UpdateTime <= @before
      """
        .trimIndent()
  }

  override val sql = statement(parameterizedQueryString) { bind("before").to(before) }

  override fun asResult(struct: Struct): String = struct.getString("GlobalComputationId")
}
