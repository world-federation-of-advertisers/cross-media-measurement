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

package org.wfanet.measurement.access.deploy.tools

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import io.grpc.Server
import io.grpc.ServerServiceDefinition
import io.grpc.netty.NettyServerBuilder
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.TimeUnit.SECONDS
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.verify
import org.wfanet.measurement.access.v1alpha.CheckPermissionsRequest
import org.wfanet.measurement.access.v1alpha.CheckPermissionsResponse
import org.wfanet.measurement.access.v1alpha.CreatePrincipalRequest
import org.wfanet.measurement.access.v1alpha.CreateRoleRequest
import org.wfanet.measurement.access.v1alpha.DeletePrincipalRequest
import org.wfanet.measurement.access.v1alpha.GetPermissionRequest
import org.wfanet.measurement.access.v1alpha.GetPrincipalRequest
import org.wfanet.measurement.access.v1alpha.GetRoleRequest
import org.wfanet.measurement.access.v1alpha.ListPermissionsRequest
import org.wfanet.measurement.access.v1alpha.ListPermissionsResponse
import org.wfanet.measurement.access.v1alpha.ListRolesRequest
import org.wfanet.measurement.access.v1alpha.ListRolesResponse
import org.wfanet.measurement.access.v1alpha.Permission
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt.PermissionsCoroutineImplBase
import org.wfanet.measurement.access.v1alpha.Principal
import org.wfanet.measurement.access.v1alpha.PrincipalKt.oAuthUser
import org.wfanet.measurement.access.v1alpha.PrincipalKt.tlsClient
import org.wfanet.measurement.access.v1alpha.PrincipalsGrpcKt.PrincipalsCoroutineImplBase
import org.wfanet.measurement.access.v1alpha.Role
import org.wfanet.measurement.access.v1alpha.RolesGrpcKt
import org.wfanet.measurement.access.v1alpha.UpdateRoleRequest
import org.wfanet.measurement.access.v1alpha.checkPermissionsRequest
import org.wfanet.measurement.access.v1alpha.checkPermissionsResponse
import org.wfanet.measurement.access.v1alpha.copy
import org.wfanet.measurement.access.v1alpha.createPrincipalRequest
import org.wfanet.measurement.access.v1alpha.createRoleRequest
import org.wfanet.measurement.access.v1alpha.deletePrincipalRequest
import org.wfanet.measurement.access.v1alpha.getPermissionRequest
import org.wfanet.measurement.access.v1alpha.getPrincipalRequest
import org.wfanet.measurement.access.v1alpha.getRoleRequest
import org.wfanet.measurement.access.v1alpha.listPermissionsRequest
import org.wfanet.measurement.access.v1alpha.listPermissionsResponse
import org.wfanet.measurement.access.v1alpha.listRolesRequest
import org.wfanet.measurement.access.v1alpha.listRolesResponse
import org.wfanet.measurement.access.v1alpha.lookupPrincipalRequest
import org.wfanet.measurement.access.v1alpha.permission
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.access.v1alpha.role
import org.wfanet.measurement.access.v1alpha.updateRoleRequest
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.grpc.toServerTlsContext
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.captureFirst

@RunWith(JUnit4::class)
class AccessTest {
  private val permissionsServiceMock: PermissionsCoroutineImplBase = mockService {
    onBlocking { getPermission(any()) }.thenReturn(GET_BOOK_PERMISSION)
    onBlocking { listPermissions(any()) }
      .thenReturn(listPermissionsResponse { permissions += GET_BOOK_PERMISSION })
    onBlocking { checkPermissions(any()) }
      .thenReturn(
        checkPermissionsResponse {
          permissions += GET_BOOK_PERMISSION_NAME
          permissions += WRITE_BOOK_PERMISSION_NAME
        }
      )
  }

  private val principalsServiceMock: PrincipalsCoroutineImplBase = mockService {
    onBlocking { getPrincipal(any()) }.thenReturn(USER_PRINCIPAL)
    onBlocking { createPrincipal(any()) }.thenReturn(USER_PRINCIPAL)
    onBlocking { lookupPrincipal(LOOKUP_USER_PRINCIPAL_REQUEST) }.thenReturn(USER_PRINCIPAL)
    onBlocking { lookupPrincipal(LOOKUP_TLS_PRINCIPAL_REQUEST) }.thenReturn(TLS_PRINCIPAL)
  }

  private val rolesServiceMock: RolesGrpcKt.RolesCoroutineImplBase = mockService {
    onBlocking { getRole(any()) }.thenReturn(ROLE)
    onBlocking { listRoles(any()) }.thenReturn(listRolesResponse { roles += ROLE })
    onBlocking { createRole(any()) }.thenReturn(ROLE)
    onBlocking { updateRole(any()) }.thenReturn(ROLE)
  }

  private val serverCerts =
    SigningCerts.fromPemFiles(
      certificateFile = SECRETS_DIR.resolve("reporting_tls.pem").toFile(),
      privateKeyFile = SECRETS_DIR.resolve("reporting_tls.key").toFile(),
      trustedCertCollectionFile = SECRETS_DIR.resolve("reporting_root.pem").toFile(),
    )

  private val services: List<ServerServiceDefinition> =
    listOf(
      permissionsServiceMock.bindService(),
      principalsServiceMock.bindService(),
      rolesServiceMock.bindService(),
    )

  private val server: Server =
    NettyServerBuilder.forPort(0)
      .sslContext(serverCerts.toServerTlsContext())
      .addServices(services)
      .build()

  @Before
  fun initServer() {
    server.start()
  }

  @After
  fun shutdownServer() {
    server.shutdown()
    server.awaitTermination(1, SECONDS)
  }

  @Test
  fun `principals get calls GetPrincipal with valid request `() {
    val args = commonArgs + arrayOf("principals", "get", PRINCIPAL_NAME)

    val output = callCli(args)

    val request: GetPrincipalRequest = captureFirst {
      runBlocking { verify(principalsServiceMock).getPrincipal(capture()) }
    }

    assertThat(request).isEqualTo(getPrincipalRequest { name = PRINCIPAL_NAME })
    assertThat(parseTextProto(output.reader(), Principal.getDefaultInstance()))
      .isEqualTo(USER_PRINCIPAL)
  }

  @Test
  fun `principals create calls CreatePrincipal with valid request with oauth user`() {
    val args =
      commonArgs +
        arrayOf(
          "principals",
          "create",
          "--name=$PRINCIPAL_NAME",
          "--issuer=example.com",
          "--subject=user1@example.com",
          "--principal-id=user-1",
        )
    val output = callCli(args)

    val request: CreatePrincipalRequest = captureFirst {
      runBlocking { verify(principalsServiceMock).createPrincipal(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        createPrincipalRequest {
          principal = principal {
            name = PRINCIPAL_NAME
            user = oAuthUser {
              issuer = "example.com"
              subject = "user1@example.com"
            }
          }
          principalId = "user-1"
        }
      )
    assertThat(parseTextProto(output.reader(), Principal.getDefaultInstance()))
      .isEqualTo(USER_PRINCIPAL)
  }

  @Test
  fun `principals delete calls DeletePrincipal with valid request`() {
    val args = commonArgs + arrayOf("principals", "delete", "--name=$PRINCIPAL_NAME")

    callCli(args)

    val request: DeletePrincipalRequest = captureFirst {
      runBlocking { verify(principalsServiceMock).deletePrincipal(capture()) }
    }

    assertThat(request).isEqualTo(deletePrincipalRequest { name = PRINCIPAL_NAME })
  }

  @Test
  fun `principals lookup calls LookupPrincipal with valid request with oauth user`() {
    val args =
      commonArgs +
        arrayOf("principals", "lookup", "--issuer=example.com", "--subject=user1@example.com")

    val output = callCli(args)

    assertThat(parseTextProto(output.reader(), Principal.getDefaultInstance()))
      .isEqualTo(USER_PRINCIPAL)
  }

  @Test
  fun `principals lookup calls LookupPrincipal with valid request with tls client`() {
    val args =
      commonArgs + arrayOf("principals", "lookup", "--principal-tls-client-cert-file=$MC_CERT_PATH")

    val output = callCli(args)

    assertThat(parseTextProto(output.reader(), Principal.getDefaultInstance()))
      .isEqualTo(TLS_PRINCIPAL)
  }

  @Test
  fun `roles get calls GetRole with valid request`() {
    val args = commonArgs + arrayOf("roles", "get", ROLE_NAME)
    val output = callCli(args)

    val request: GetRoleRequest = captureFirst {
      runBlocking { verify(rolesServiceMock).getRole(capture()) }
    }

    assertThat(request).isEqualTo(getRoleRequest { name = ROLE_NAME })
    assertThat(parseTextProto(output.reader(), Role.getDefaultInstance())).isEqualTo(ROLE)
  }

  @Test
  fun `roles list calls ListRoles with valid request`() {
    val args = commonArgs + arrayOf("roles", "list")
    val output = callCli(args)

    val request: ListRolesRequest = captureFirst {
      runBlocking { verify(rolesServiceMock).listRoles(capture()) }
    }

    assertThat(request).isEqualTo(listRolesRequest { pageSize = 1000 })
    assertThat(parseTextProto(output.reader(), ListRolesResponse.getDefaultInstance()))
      .isEqualTo(listRolesResponse { roles += ROLE })
  }

  @Test
  fun `roles create calls CreateRole with valid request`() {
    val args =
      commonArgs +
        arrayOf(
          "roles",
          "create",
          "--resource-type=$SHELF_RESOURCE",
          "--resource-type=$DESK_RESOURCE",
          "--permission=$GET_BOOK_PERMISSION_NAME",
          "--permission=$WRITE_BOOK_PERMISSION_NAME",
          "--etag=$ROLE_REQUEST_ETAG",
          "--role-id=$ROLE_ID",
        )

    val output = callCli(args)

    val request: CreateRoleRequest = captureFirst {
      runBlocking { verify(rolesServiceMock).createRole(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        createRoleRequest {
          role =
            ROLE.copy {
              name = ""
              etag = ROLE_REQUEST_ETAG
            }
          roleId = ROLE_ID
        }
      )
    assertThat(parseTextProto(output.reader(), Role.getDefaultInstance())).isEqualTo(ROLE)
  }

  @Test
  fun `roles update calls UpdateRole with valid request`() {
    val args =
      commonArgs +
        arrayOf(
          "roles",
          "update",
          "--name=$ROLE_NAME",
          "--resource-type=$SHELF_RESOURCE",
          "--resource-type=$DESK_RESOURCE",
          "--permission=$GET_BOOK_PERMISSION_NAME",
          "--permission=$WRITE_BOOK_PERMISSION_NAME",
          "--etag=$ROLE_REQUEST_ETAG",
        )

    val output = callCli(args)

    val request: UpdateRoleRequest = captureFirst {
      runBlocking { verify(rolesServiceMock).updateRole(capture()) }
    }

    assertThat(request)
      .isEqualTo(updateRoleRequest { role = ROLE.copy { etag = ROLE_REQUEST_ETAG } })
    assertThat(parseTextProto(output.reader(), Role.getDefaultInstance())).isEqualTo(ROLE)
  }

  @Test
  fun `permissions get calls GetPermission with valid request`() {
    val args = commonArgs + arrayOf("permissions", "get", GET_BOOK_PERMISSION_NAME)
    val output = callCli(args)

    val request: GetPermissionRequest = captureFirst {
      runBlocking { verify(permissionsServiceMock).getPermission(capture()) }
    }

    assertThat(request).isEqualTo(getPermissionRequest { name = GET_BOOK_PERMISSION_NAME })
    assertThat(parseTextProto(output.reader(), Permission.getDefaultInstance()))
      .isEqualTo(GET_BOOK_PERMISSION)
  }

  @Test
  fun `permissions list calls ListPermissions with valid request`() {
    val args = commonArgs + arrayOf("permissions", "list")
    val output = callCli(args)

    val request: ListPermissionsRequest = captureFirst {
      runBlocking { verify(permissionsServiceMock).listPermissions(capture()) }
    }

    assertThat(request).isEqualTo(listPermissionsRequest { pageSize = 1000 })
    assertThat(parseTextProto(output.reader(), ListPermissionsResponse.getDefaultInstance()))
      .isEqualTo(listPermissionsResponse { permissions += GET_BOOK_PERMISSION })
  }

  @Test
  fun `permissions check calls CheckPermissions with valid request`() {
    val args =
      commonArgs +
        arrayOf(
          "permissions",
          "check",
          "--principal=$PRINCIPAL_NAME",
          "--protected-resource=$SHELF_RESOURCE",
          "--permission=$GET_BOOK_PERMISSION_NAME",
          "--permission=$WRITE_BOOK_PERMISSION_NAME",
        )
    val output = callCli(args)

    val request: CheckPermissionsRequest = captureFirst {
      runBlocking { verify(permissionsServiceMock).checkPermissions(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        checkPermissionsRequest {
          principal = PRINCIPAL_NAME
          protectedResource += SHELF_RESOURCE
          permissions += GET_BOOK_PERMISSION_NAME
          permissions += WRITE_BOOK_PERMISSION_NAME
        }
      )

    assertThat(parseTextProto(output.reader(), CheckPermissionsResponse.getDefaultInstance()))
      .isEqualTo(
        checkPermissionsResponse {
          permissions += GET_BOOK_PERMISSION_NAME
          permissions += WRITE_BOOK_PERMISSION_NAME
        }
      )
  }

  private val commonArgs: Array<String>
    get() =
      arrayOf(
        "--tls-cert-file=$SECRETS_DIR/reporting_tls.pem",
        "--tls-key-file=$SECRETS_DIR/reporting_tls.key",
        "--cert-collection-file=$SECRETS_DIR/reporting_root.pem",
        "--access-public-api-target=$HOST:${server.port}",
      )

  private fun callCli(args: Array<String>): String {
    val capturedOutput = CommandLineTesting.capturingOutput(args, Access::main)
    CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    return capturedOutput.out
  }

  companion object {
    private const val HOST = "localhost"
    private val SECRETS_DIR: Path =
      getRuntimePath(
        Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
      )!!
    private val MC_CERT_PATH: Path = SECRETS_DIR.resolve("mc_tls.pem")
    private val MC_CERT_FILE: File = MC_CERT_PATH.toFile()
    private val TLS_CLIENT_AKID: ByteString = readCertificate(MC_CERT_FILE).authorityKeyIdentifier!!

    private const val PRINCIPAL_NAME = "principals/user-1"
    private val USER_PRINCIPAL = principal {
      name = PRINCIPAL_NAME
      user = oAuthUser {
        issuer = "example.com"
        subject = "user1@example.com"
      }
    }

    private val TLS_PRINCIPAL = principal {
      name = PRINCIPAL_NAME
      tlsClient = tlsClient { authorityKeyIdentifier = TLS_CLIENT_AKID }
    }

    private val LOOKUP_USER_PRINCIPAL_REQUEST = lookupPrincipalRequest {
      user = oAuthUser {
        issuer = "example.com"
        subject = "user1@example.com"
      }
    }

    private val LOOKUP_TLS_PRINCIPAL_REQUEST = lookupPrincipalRequest {
      tlsClient = tlsClient { authorityKeyIdentifier = TLS_CLIENT_AKID }
    }

    private const val ROLE_NAME = "roles/bookReader"
    private const val ROLE_ID = "bookReader"
    private const val SHELF_RESOURCE = "library.googleapis.com/Shelf"
    private const val DESK_RESOURCE = "library.googleapis.com/Desk"

    private const val ROLE_REQUEST_ETAG = "request-etag"
    private val ROLE = role {
      name = ROLE_NAME
      resourceTypes += SHELF_RESOURCE
      resourceTypes += DESK_RESOURCE
      permissions += GET_BOOK_PERMISSION_NAME
      permissions += WRITE_BOOK_PERMISSION_NAME
      etag = "response-etag"
    }

    private const val GET_BOOK_PERMISSION_NAME = "permissions/books.get"
    private const val WRITE_BOOK_PERMISSION_NAME = "permissions/books.write"
    private val GET_BOOK_PERMISSION = permission {
      name = GET_BOOK_PERMISSION_NAME
      resourceTypes += SHELF_RESOURCE
      resourceTypes += DESK_RESOURCE
    }
  }
}
