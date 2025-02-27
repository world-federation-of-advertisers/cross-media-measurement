/*
 * Copyright 2024 The Cross-Media Measurement Authors
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
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.statement

/** Returns the subset of [permissionIds] that the principal has on the protected resource. */
suspend fun AsyncDatabaseClient.ReadContext.checkPermissions(
  protectedResourceName: String,
  principalId: Long,
  permissionIds: Iterable<Long>,
): List<Long> {
  val sql =
    """
    SELECT
      PermissionId
    FROM
      PolicyBindings
      JOIN Policies USING (PolicyId)
      JOIN RolePermissions USING (RoleId)
    WHERE
      PrincipalId = @principalId
      AND ProtectedResourceName = @protectedResourceName
      AND PermissionId IN UNNEST(@permissionIds)
    """
      .trimIndent()
  return executeQuery(
      statement(sql) {
        bind("principalId").to(principalId)
        bind("protectedResourceName").to(protectedResourceName)
        bind("permissionIds").toInt64Array(permissionIds)
      },
      Options.tag("action=checkPermissions"),
    )
    .map { it.getLong("PermissionId") }
    .toList()
}
