/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.access.deploy.gcloud.spanner.db

import com.google.cloud.spanner.Options
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.statement

data class PolicyBindingResult(val policyId: Long, val roleId: Long, val principalId: Long)

/** Reads PolicyBindings by RoleId. */
fun AsyncDatabaseClient.ReadContext.readPolicyBindingsByRoleId(
  roleId: Long
): Flow<PolicyBindingResult> {
  val sql = buildString {
    appendLine(PolicyBindings.BASE_SQL)
    appendLine("WHERE RoleId = @roleId")
  }

  return executeQuery(
      statement(sql) { bind("roleId").to(roleId) },
      Options.tag("action=readPolicyBindingsByRoleId"),
    )
    .map { row ->
      PolicyBindingResult(
        policyId = row.getLong("PolicyId"),
        roleId = row.getLong("RoleId"),
        principalId = row.getLong("PrincipalId"),
      )
    }
}

private object PolicyBindings {
  val BASE_SQL =
    """
    SELECT
      PolicyId,
      RoleId,
      PrincipalId
    FROM
      PolicyBindings
    """
      .trimIndent()
}
