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

package org.wfanet.measurement.access.deploy.common.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Descriptors
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.access.CheckPermissionsResponse
import org.wfanet.measurement.internal.access.ListPermissionsPageTokenKt
import org.wfanet.measurement.internal.access.ListPermissionsRequest
import org.wfanet.measurement.internal.access.ListPermissionsResponse
import org.wfanet.measurement.internal.access.Permission
import org.wfanet.measurement.internal.access.PermissionsGrpcKt
import org.wfanet.measurement.internal.access.checkPermissionsRequest
import org.wfanet.measurement.internal.access.checkPermissionsResponse
import org.wfanet.measurement.internal.access.getPermissionRequest
import org.wfanet.measurement.internal.access.listPermissionsPageToken
import org.wfanet.measurement.internal.access.listPermissionsRequest
import org.wfanet.measurement.internal.access.listPermissionsResponse

@RunWith(JUnit4::class)
abstract class PermissionsServiceTest {
  protected val tlsClientMapping = TestConfig.TLS_CLIENT_MAPPING

  /**
   * Service under test.
   *
   * This must be initialized using [tlsClientMapping].
   */
  protected abstract val service: PermissionsGrpcKt.PermissionsCoroutineImplBase

  @Test
  fun `checkPermissions returns all requested permissions for TLS client principal`() {
    val request = checkPermissionsRequest {
      protectedResourceName = TestConfig.MC_RESOURCE_NAME
      principalResourceId = TestConfig.MC_PRINCIPAL_RESOURCE_ID
      permissionResourceIds += "measurementConsumers.foo"
      permissionResourceIds += "measurementConsumers.bar"
    }

    val response: CheckPermissionsResponse = runBlocking { service.checkPermissions(request) }

    assertThat(response)
      .isEqualTo(
        checkPermissionsResponse { permissionResourceIds += request.permissionResourceIdsList }
      )
  }

  @Test
  fun `checkPermissions returns no permissions for TLS client principal with wrong protected resource`() {
    val request = checkPermissionsRequest {
      protectedResourceName = "measurementConsumers/404"
      principalResourceId = TestConfig.MC_PRINCIPAL_RESOURCE_ID
      permissionResourceIds += "measurementConsumers.foo"
      permissionResourceIds += "measurementConsumers.bar"
    }

    val response: CheckPermissionsResponse = runBlocking { service.checkPermissions(request) }

    assertThat(response).isEqualTo(CheckPermissionsResponse.getDefaultInstance())
  }

  companion object {
    private val RESOURCE_TYPES_FIELD: Descriptors.FieldDescriptor =
      Permission.getDescriptor().findFieldByNumber(Permission.RESOURCE_TYPES_FIELD_NUMBER)

    private val PERMISSIONS: List<Permission> =
      TestConfig.PERMISSION_MAPPING.permissions.map { it.toPermission() }
  }
}
