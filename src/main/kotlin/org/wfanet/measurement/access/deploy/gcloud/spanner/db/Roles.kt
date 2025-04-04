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

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.access.service.internal.PermissionMapping
import org.wfanet.measurement.access.service.internal.PermissionNotFoundForRoleException
import org.wfanet.measurement.access.service.internal.RoleNotFoundException
import org.wfanet.measurement.common.api.ETags
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.access.ListRolesPageToken
import org.wfanet.measurement.internal.access.Role
import org.wfanet.measurement.internal.access.role

data class RoleResult(val roleId: Long, val role: Role)

/**
 * Reads a [Role] by its resource ID.
 *
 * @throws RoleNotFoundException
 * @throws PermissionNotFoundForRoleException
 */
suspend fun AsyncDatabaseClient.ReadContext.getRoleByResourceId(
  permissionMapping: PermissionMapping,
  roleResourceId: String,
): RoleResult {
  val sql =
    """
    SELECT
      RoleId,
      CreateTime,
      UpdateTime,
      ARRAY(
        SELECT ResourceType FROM RoleResourceTypes WHERE RoleResourceTypes.RoleId = Roles.RoleId
      ) AS ResourceTypes,
      ARRAY(
        SELECT PermissionId FROM RolePermissions WHERE RolePermissions.RoleId = Roles.RoleId
      ) AS PermissionIds
    FROM
      Roles
    WHERE
      RoleResourceId = @roleResourceId
    """
      .trimIndent()
  val row: Struct =
    executeQuery(
        statement(sql) { bind("roleResourceId").to(roleResourceId) },
        Options.tag("action=getRoleByResourceId"),
      )
      .singleOrNullIfEmpty() ?: throw RoleNotFoundException(roleResourceId)

  val permissionResourceIds =
    row.getLongList("PermissionIds").map {
      val mappingPermission: PermissionMapping.Permission =
        permissionMapping.getPermissionById(it)
          ?: throw PermissionNotFoundForRoleException(roleResourceId)
      mappingPermission.permissionResourceId
    }

  return RoleResult(
    row.getLong("RoleId"),
    role {
      this.roleResourceId = roleResourceId
      createTime = row.getTimestamp("CreateTime").toProto()
      updateTime = row.getTimestamp("UpdateTime").toProto()
      resourceTypes += row.getStringList("ResourceTypes")
      this.permissionResourceIds += permissionResourceIds
      etag = ETags.computeETag(updateTime.toInstant())
    },
  )
}

/**
 * Reads the ID of the [Role] with the specified resource ID.
 *
 * @throws RoleNotFoundException
 */
suspend fun AsyncDatabaseClient.ReadContext.getRoleIdByResourceId(roleResourceId: String): Long {
  val row =
    readRowUsingIndex("Roles", "RolesByResourceId", Key.of(roleResourceId), "RoleId")
      ?: throw RoleNotFoundException(roleResourceId)
  return row.getLong("RoleId")
}

/**
 * Reads the role IDs for the specified resource IDs.
 *
 * @throws RoleNotFoundException if no role ID is found for a specified resource ID
 */
suspend fun AsyncDatabaseClient.ReadContext.getRoleIdsByResourceIds(
  roleResourceIds: Collection<String>
): Map<String, Long> {
  if (roleResourceIds.isEmpty()) {
    return emptyMap()
  }

  val keys =
    KeySet.newBuilder()
      .apply {
        for (roleResourceId in roleResourceIds) {
          addKey(Key.of(roleResourceId))
        }
      }
      .build()

  val roleIdByResourceId: Map<String, Long> =
    readUsingIndex("Roles", "RolesByResourceId", keys, listOf("RoleResourceId", "RoleId"))
      .map { it.getString(0) to it.getLong(1) }
      .toList()
      .toMap()

  if (roleIdByResourceId.size != roleResourceIds.size) {
    for (roleResourceId in roleResourceIds) {
      if (!roleIdByResourceId.containsKey(roleResourceId)) {
        throw RoleNotFoundException(roleResourceId)
      }
    }
  }

  return roleIdByResourceId
}

/** Returns whether a [Role] with the specified [roleId] exists. */
suspend fun AsyncDatabaseClient.ReadContext.roleExists(roleId: Long): Boolean {
  return readRow("Roles", Key.of(roleId), listOf("RoleId")) != null
}

/**
 * Reads [Role]s ordered by resource ID.
 *
 * @throws PermissionNotFoundForRoleException
 */
fun AsyncDatabaseClient.ReadContext.readRoles(
  permissionMapping: PermissionMapping,
  limit: Int,
  after: ListRolesPageToken.After? = null,
): Flow<RoleResult> {
  val sql = buildString {
    appendLine(
      """
      SELECT
        Roles.*,
        ARRAY(
          SELECT ResourceType FROM RoleResourceTypes WHERE RoleResourceTypes.RoleId = Roles.RoleId
        ) AS ResourceTypes,
        ARRAY(
          SELECT PermissionId FROM RolePermissions WHERE RolePermissions.RoleId = Roles.RoleId
        ) AS PermissionIds
      FROM
        Roles
      """
        .trimIndent()
    )
    if (after != null) {
      appendLine("WHERE RoleResourceId > @afterRoleResourceId")
    }
    appendLine("ORDER BY RoleResourceId")
    appendLine("LIMIT @limit")
  }
  val query =
    statement(sql) {
      if (after != null) {
        bind("afterRoleResourceId").to(after.roleResourceId)
      }
      bind("limit").to(limit.toLong())
    }

  return executeQuery(query, Options.tag("action=readRoles")).map { row ->
    val roleResourceId = row.getString("RoleResourceId")
    val permissionResourceIds =
      row.getLongList("PermissionIds").map {
        val mappingPermission: PermissionMapping.Permission =
          permissionMapping.getPermissionById(it)
            ?: throw PermissionNotFoundForRoleException(roleResourceId)
        mappingPermission.permissionResourceId
      }
    RoleResult(
      row.getLong("RoleId"),
      role {
        this.roleResourceId = roleResourceId
        createTime = row.getTimestamp("CreateTime").toProto()
        updateTime = row.getTimestamp("UpdateTime").toProto()
        resourceTypes += row.getStringList("ResourceTypes")
        this.permissionResourceIds += permissionResourceIds
        etag = ETags.computeETag(updateTime.toInstant())
      },
    )
  }
}

/** Buffers an insert mutation for the Roles table. */
fun AsyncDatabaseClient.TransactionContext.insertRole(roleId: Long, roleResourceId: String) {
  bufferInsertMutation("Roles") {
    set("RoleId").to(roleId)
    set("RoleResourceId").to(roleResourceId)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers an update mutation for the Roles table. */
fun AsyncDatabaseClient.TransactionContext.updateRole(roleId: Long) {
  bufferUpdateMutation("Roles") {
    set("RoleId").to(roleId)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers a delete mutation for the Roles table. */
fun AsyncDatabaseClient.TransactionContext.deleteRole(roleId: Long) {
  buffer(Mutation.delete("Roles", Key.of(roleId)))
}

/** Buffers an insert mutation for the RoleResourceTypes table. */
fun AsyncDatabaseClient.TransactionContext.insertRoleResourceType(
  roleId: Long,
  resourceType: String,
) {
  bufferInsertMutation("RoleResourceTypes") {
    set("RoleId").to(roleId)
    set("ResourceType").to(resourceType)
  }
}

/** Buffers a delete mutation for the RoleResourceTypes table. */
fun AsyncDatabaseClient.TransactionContext.deleteRoleResourceType(
  roleId: Long,
  resourceType: String,
) {
  buffer(Mutation.delete("RoleResourceTypes", Key.of(roleId, resourceType)))
}

/** Buffers an insert mutation for the RolePermissions table. */
fun AsyncDatabaseClient.TransactionContext.insertRolePermission(roleId: Long, permissionId: Long) {
  bufferInsertMutation("RolePermissions") {
    set("RoleId").to(roleId)
    set("PermissionId").to(permissionId)
  }
}

/** Buffers a delete mutation for the RolePermissions table. */
fun AsyncDatabaseClient.TransactionContext.deleteRolePermission(roleId: Long, permissionId: Long) {
  buffer(Mutation.delete("RolePermissions", Key.of(roleId, permissionId)))
}
