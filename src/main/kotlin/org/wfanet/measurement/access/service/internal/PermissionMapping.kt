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

package org.wfanet.measurement.access.service.internal

import com.google.common.hash.Hashing
import org.wfanet.measurement.config.access.PermissionsConfig
import org.wfanet.measurement.internal.access.Permission
import org.wfanet.measurement.internal.access.permission

class PermissionMapping(config: PermissionsConfig) {
  data class Permission(
    val permissionId: Long,
    val permissionResourceId: String,
    val protectedResourceTypes: Set<String>,
  )

  /** Permissions sorted by resource ID. */
  val permissions: List<Permission> =
    buildList {
        for ((permissionResourceId, configPermission) in config.permissionsMap) {
          check(PERMISSION_RESOURCE_ID_REGEX.matches(permissionResourceId)) {
            "Invalid permission resource ID $permissionResourceId"
          }
          val permissionId = fingerprint(permissionResourceId)
          add(
            Permission(
              permissionId,
              permissionResourceId,
              configPermission.protectedResourceTypesList.toSet(),
            )
          )
        }
      }
      .sortedBy { it.permissionResourceId }

  private val permissionsById: Map<Long, Permission> =
    buildMap(permissions.size) {
      for (permission in permissions) {
        val existingPermission = get(permission.permissionId)
        if (existingPermission != null) {
          error(
            "Fingerprinting collision between permissions " +
              "${existingPermission.permissionResourceId} and ${permission.permissionResourceId}"
          )
        }
        put(permission.permissionId, permission)
      }
    }

  private val permissionsByResourceId: Map<String, Permission> =
    permissions.associateBy { it.permissionResourceId }

  fun getPermissionById(permissionId: Long) = permissionsById[permissionId]

  fun getPermissionByResourceId(permissionResourceId: String) =
    permissionsByResourceId[permissionResourceId]

  companion object {
    private fun fingerprint(input: String): Long =
      Hashing.farmHashFingerprint64().hashString(input, Charsets.UTF_8).asLong()

    private val PERMISSION_RESOURCE_ID_REGEX = Regex("^[a-zA-Z]([a-zA-Z0-9.-]{0,61}[a-zA-Z0-9])?$")
  }
}

fun PermissionMapping.Permission.toPermission(): Permission {
  val source = this
  return permission {
    permissionResourceId = source.permissionResourceId
    resourceTypes += source.protectedResourceTypes
  }
}
