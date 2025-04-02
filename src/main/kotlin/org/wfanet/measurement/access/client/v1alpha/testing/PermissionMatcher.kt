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

package org.wfanet.measurement.access.client.v1alpha.testing

import org.mockito.ArgumentMatcher
import org.mockito.kotlin.argThat
import org.wfanet.measurement.access.service.PermissionKey
import org.wfanet.measurement.access.v1alpha.CheckPermissionsRequest

/**
 * [ArgumentMatcher] matching
 * [CheckPermissionsRequest.permissionsList][CheckPermissionsRequest.getPermissionsList].
 */
class PermissionMatcher(private val permissionName: String) :
  ArgumentMatcher<CheckPermissionsRequest> {
  override fun matches(argument: CheckPermissionsRequest?): Boolean {
    if (argument == null) {
      return false
    }

    return argument.permissionsList.contains(permissionName)
  }

  companion object {
    fun hasPermission(permissionName: String) = argThat(PermissionMatcher(permissionName))

    fun hasPermissionId(permissionId: String) = hasPermission(PermissionKey(permissionId).toName())
  }
}
