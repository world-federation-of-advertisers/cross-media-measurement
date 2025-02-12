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
import com.google.protobuf.Empty
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
import org.wfanet.measurement.access.v1alpha.AddPolicyBindingMembersRequest
import org.wfanet.measurement.access.v1alpha.CheckPermissionsRequest
import org.wfanet.measurement.access.v1alpha.CheckPermissionsResponse
import org.wfanet.measurement.access.v1alpha.CreatePolicyRequest
import org.wfanet.measurement.access.v1alpha.CreatePrincipalRequest
import org.wfanet.measurement.access.v1alpha.CreateRoleRequest
import org.wfanet.measurement.access.v1alpha.DeletePrincipalRequest
import org.wfanet.measurement.access.v1alpha.DeleteRoleRequest
import org.wfanet.measurement.access.v1alpha.GetPermissionRequest
import org.wfanet.measurement.access.v1alpha.GetPolicyRequest
import org.wfanet.measurement.access.v1alpha.GetPrincipalRequest
import org.wfanet.measurement.access.v1alpha.GetRoleRequest
import org.wfanet.measurement.access.v1alpha.ListPermissionsRequest
import org.wfanet.measurement.access.v1alpha.ListPermissionsResponse
import org.wfanet.measurement.access.v1alpha.ListRolesRequest
import org.wfanet.measurement.access.v1alpha.ListRolesResponse
import org.wfanet.measurement.access.v1alpha.LookupPolicyRequest
import org.wfanet.measurement.access.v1alpha.Permission
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt.PermissionsCoroutineImplBase
import org.wfanet.measurement.access.v1alpha.PoliciesGrpcKt.PoliciesCoroutineImplBase
import org.wfanet.measurement.access.v1alpha.Policy
import org.wfanet.measurement.access.v1alpha.PolicyKt
import org.wfanet.measurement.access.v1alpha.Principal
import org.wfanet.measurement.access.v1alpha.PrincipalKt.oAuthUser
import org.wfanet.measurement.access.v1alpha.PrincipalKt.tlsClient
import org.wfanet.measurement.access.v1alpha.PrincipalsGrpcKt.PrincipalsCoroutineImplBase
import org.wfanet.measurement.access.v1alpha.RemovePolicyBindingMembersRequest
import org.wfanet.measurement.access.v1alpha.Role
import org.wfanet.measurement.access.v1alpha.RolesGrpcKt
import org.wfanet.measurement.access.v1alpha.UpdateRoleRequest
import org.wfanet.measurement.access.v1alpha.addPolicyBindingMembersRequest
import org.wfanet.measurement.access.v1alpha.checkPermissionsRequest
import org.wfanet.measurement.access.v1alpha.checkPermissionsResponse
import org.wfanet.measurement.access.v1alpha.copy
import org.wfanet.measurement.access.v1alpha.createPolicyRequest
import org.wfanet.measurement.access.v1alpha.createPrincipalRequest
import org.wfanet.measurement.access.v1alpha.createRoleRequest
import org.wfanet.measurement.access.v1alpha.deletePrincipalRequest
import org.wfanet.measurement.access.v1alpha.deleteRoleRequest
import org.wfanet.measurement.access.v1alpha.getPermissionRequest
import org.wfanet.measurement.access.v1alpha.getPolicyRequest
import org.wfanet.measurement.access.v1alpha.getPrincipalRequest
import org.wfanet.measurement.access.v1alpha.getRoleRequest
import org.wfanet.measurement.access.v1alpha.listPermissionsRequest
import org.wfanet.measurement.access.v1alpha.listPermissionsResponse
import org.wfanet.measurement.access.v1alpha.listRolesRequest
import org.wfanet.measurement.access.v1alpha.listRolesResponse
import org.wfanet.measurement.access.v1alpha.lookupPolicyRequest
import org.wfanet.measurement.access.v1alpha.lookupPrincipalRequest
import org.wfanet.measurement.access.v1alpha.permission
import org.wfanet.measurement.access.v1alpha.policy
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.access.v1alpha.removePolicyBindingMembersRequest
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

  private val policiesServiceMock: PoliciesCoroutineImplBase = mockService {
    onBlocking { getPolicy(any()) }.thenReturn(POLICY)
    onBlocking { createPolicy(any()) }.thenReturn(POLICY)
    onBlocking { lookupPolicy(any()) }.thenReturn(POLICY)
    onBlocking { addPolicyBindingMembers(any()) }
      .thenReturn(
        POLICY.copy {
          bindings.clear()
          bindings += READER_BINDING.copy { members += NON_MEMBER_PRINCIPAL_NAME }
          bindings += WRITER_BINDING
        }
      )
    onBlocking { removePolicyBindingMembers(any()) }
      .thenReturn(
        POLICY.copy {
          bindings.clear()
          bindings +=
            READER_BINDING.copy {
              members.clear()
              members += WRITER_BINDING.membersList
            }
          bindings += WRITER_BINDING
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
    onBlocking { deleteRole(any()) }.thenReturn(Empty.getDefaultInstance())
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
      policiesServiceMock.bindService(),
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
    val args = commonArgs + arrayOf("principals", "get", OWNER_PRINCIPAL_NAME)

    val output = callCli(args)

    val request: GetPrincipalRequest = captureFirst {
      runBlocking { verify(principalsServiceMock).getPrincipal(capture()) }
    }

    assertThat(request).isEqualTo(getPrincipalRequest { name = OWNER_PRINCIPAL_NAME })
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
    val args = commonArgs + arrayOf("principals", "delete", OWNER_PRINCIPAL_NAME)

    callCli(args)

    val request: DeletePrincipalRequest = captureFirst {
      runBlocking { verify(principalsServiceMock).deletePrincipal(capture()) }
    }

    assertThat(request).isEqualTo(deletePrincipalRequest { name = OWNER_PRINCIPAL_NAME })
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
    val args = commonArgs + arrayOf("roles", "get", READER_ROLE_NAME)
    val output = callCli(args)

    val request: GetRoleRequest = captureFirst {
      runBlocking { verify(rolesServiceMock).getRole(capture()) }
    }

    assertThat(request).isEqualTo(getRoleRequest { name = READER_ROLE_NAME })
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
          "--role-id=$READER_ROLE_ID",
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
              etag = ""
            }
          roleId = READER_ROLE_ID
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
          "--name=$READER_ROLE_NAME",
          "--resource-type=$SHELF_RESOURCE",
          "--resource-type=$DESK_RESOURCE",
          "--permission=$GET_BOOK_PERMISSION_NAME",
          "--permission=$WRITE_BOOK_PERMISSION_NAME",
          "--etag=$REQUEST_ETAG",
        )

    val output = callCli(args)

    val request: UpdateRoleRequest = captureFirst {
      runBlocking { verify(rolesServiceMock).updateRole(capture()) }
    }

    assertThat(request).isEqualTo(updateRoleRequest { role = ROLE.copy { etag = REQUEST_ETAG } })
    assertThat(parseTextProto(output.reader(), Role.getDefaultInstance())).isEqualTo(ROLE)
  }

  @Test
  fun `roles delete calls DeleteRole with valid request`() {
    val args = commonArgs + arrayOf("roles", "delete", READER_ROLE_NAME)
    callCli(args)

    val request: DeleteRoleRequest = captureFirst {
      runBlocking { verify(rolesServiceMock).deleteRole(capture()) }
    }

    assertThat(request).isEqualTo(deleteRoleRequest { name = READER_ROLE_NAME })
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
          "--principal=$OWNER_PRINCIPAL_NAME",
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
          principal = OWNER_PRINCIPAL_NAME
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

  @Test
  fun `policies get calls GetPolicy with valid request`() {
    val args = commonArgs + arrayOf("policies", "get", POLICY_NAME)
    val output = callCli(args)

    val request: GetPolicyRequest = captureFirst {
      runBlocking { verify(policiesServiceMock).getPolicy(capture()) }
    }

    assertThat(request).isEqualTo(getPolicyRequest { name = POLICY_NAME })
    assertThat(parseTextProto(output.reader(), Policy.getDefaultInstance())).isEqualTo(POLICY)
  }

  @Test
  fun `policies create calls CreatePolicy with valid request`() {
    val args =
      commonArgs +
        arrayOf(
          "policies",
          "create",
          "--protected-resource=$SHELF_RESOURCE",
          "--binding-role=$READER_ROLE_NAME",
          "--binding-member=$OWNER_PRINCIPAL_NAME",
          "--binding-member=$EDITOR_PRINCIPAL_NAME",
          "--binding-member=$MEMBER_PRINCIPAL_NAME",
          "--binding-role=$WRITER_ROLE_NAME",
          "--binding-member=$OWNER_PRINCIPAL_NAME",
          "--binding-member=$EDITOR_PRINCIPAL_NAME",
          "--policy-id=$POLICY_ID",
        )
    val output = callCli(args)

    val request: CreatePolicyRequest = captureFirst {
      runBlocking { verify(policiesServiceMock).createPolicy(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        createPolicyRequest {
          policy = policy {
            protectedResource = SHELF_RESOURCE
            bindings += READER_BINDING
            bindings += WRITER_BINDING
          }
          policyId = POLICY_ID
        }
      )

    assertThat(parseTextProto(output.reader(), Policy.getDefaultInstance())).isEqualTo(POLICY)
  }

  @Test
  fun `policies lookup calls LookupPolicy with valid request`() {
    val args = commonArgs + arrayOf("policies", "lookup", "--protected-resource=$SHELF_RESOURCE")
    val output = callCli(args)

    val request: LookupPolicyRequest = captureFirst {
      runBlocking { verify(policiesServiceMock).lookupPolicy(capture()) }
    }

    assertThat(request).isEqualTo(lookupPolicyRequest { protectedResource = SHELF_RESOURCE })
    assertThat(parseTextProto(output.reader(), Policy.getDefaultInstance())).isEqualTo(POLICY)
  }

  @Test
  fun `policies add-members calls AddPolicyBindingMembers with valid request`() {
    val args =
      commonArgs +
        arrayOf(
          "policies",
          "add-members",
          "--name=$POLICY_NAME",
          "--binding-role=$READER_ROLE_NAME",
          "--binding-member=$NON_MEMBER_PRINCIPAL_NAME",
          "--etag=$REQUEST_ETAG",
        )
    val output = callCli(args)

    val request: AddPolicyBindingMembersRequest = captureFirst {
      runBlocking { verify(policiesServiceMock).addPolicyBindingMembers(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        addPolicyBindingMembersRequest {
          name = POLICY_NAME
          role = READER_ROLE_NAME
          members += NON_MEMBER_PRINCIPAL_NAME
          etag = REQUEST_ETAG
        }
      )
    assertThat(parseTextProto(output.reader(), Policy.getDefaultInstance()))
      .isEqualTo(
        POLICY.copy {
          bindings.clear()
          bindings +=
            PolicyKt.binding {
              role = "roles/bookReader"
              members += READER_BINDING.membersList
              members += NON_MEMBER_PRINCIPAL_NAME
            }
          bindings += WRITER_BINDING
        }
      )
  }

  @Test
  fun `policies remove-members calls RemovePolicyBindingMembers with valid request`() {
    val args =
      commonArgs +
        arrayOf(
          "policies",
          "remove-members",
          "--name=$POLICY_NAME",
          "--binding-role=$READER_ROLE_NAME",
          "--binding-member=$MEMBER_PRINCIPAL_NAME",
          "--etag=$REQUEST_ETAG",
        )
    val output = callCli(args)

    val request: RemovePolicyBindingMembersRequest = captureFirst {
      runBlocking { verify(policiesServiceMock).removePolicyBindingMembers(capture()) }
    }

    assertThat(request)
      .isEqualTo(
        removePolicyBindingMembersRequest {
          name = POLICY_NAME
          role = READER_ROLE_NAME
          members += MEMBER_PRINCIPAL_NAME
          etag = REQUEST_ETAG
        }
      )
    assertThat(parseTextProto(output.reader(), Policy.getDefaultInstance()))
      .isEqualTo(
        POLICY.copy {
          bindings.clear()
          bindings +=
            PolicyKt.binding {
              role = "roles/bookReader"
              members += WRITER_BINDING.membersList
            }
          bindings += WRITER_BINDING
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

    private const val OWNER_PRINCIPAL_NAME = "principals/owner"
    private const val EDITOR_PRINCIPAL_NAME = "principals/manager"
    private const val MEMBER_PRINCIPAL_NAME = "principals/member"
    private const val NON_MEMBER_PRINCIPAL_NAME = "principals/non-member"
    private val USER_PRINCIPAL = principal {
      name = OWNER_PRINCIPAL_NAME
      user = oAuthUser {
        issuer = "example.com"
        subject = "user1@example.com"
      }
    }

    private val TLS_PRINCIPAL = principal {
      name = OWNER_PRINCIPAL_NAME
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

    private const val READER_ROLE_ID = "bookReader"
    private const val READER_ROLE_NAME = "roles/$READER_ROLE_ID"
    private const val WRITER_ROLE_ID = "bookWriter"
    private const val WRITER_ROLE_NAME = "roles/$WRITER_ROLE_ID"
    private const val SHELF_RESOURCE = "library.googleapis.com/Shelf"
    private const val DESK_RESOURCE = "library.googleapis.com/Desk"

    private const val REQUEST_ETAG = "request-etag"
    private val ROLE = role {
      name = READER_ROLE_NAME
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

    private const val POLICY_ID = "policy-1"
    private const val POLICY_NAME = "policies/policy-1"
    private val READER_BINDING =
      PolicyKt.binding {
        role = "roles/bookReader"
        members += OWNER_PRINCIPAL_NAME
        members += EDITOR_PRINCIPAL_NAME
        members += MEMBER_PRINCIPAL_NAME
      }
    private val WRITER_BINDING =
      PolicyKt.binding {
        role = "roles/bookWriter"
        members += OWNER_PRINCIPAL_NAME
        members += EDITOR_PRINCIPAL_NAME
      }
    private val POLICY = policy {
      name = POLICY_NAME
      protectedResource = SHELF_RESOURCE
      bindings += READER_BINDING
      bindings += WRITER_BINDING
    }
  }
}
