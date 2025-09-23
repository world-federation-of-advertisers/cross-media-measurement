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

package org.wfanet.measurement.integration.common

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.FieldScope
import com.google.common.truth.extensions.proto.FieldScopes
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.rpc.errorInfo
import io.grpc.Channel
import io.grpc.ManagedChannel
import io.grpc.ServerServiceDefinition
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.netty.handler.ssl.ClientAuth
import java.io.File
import java.nio.file.Paths
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.wfanet.measurement.access.common.TlsClientPrincipalMapping
import org.wfanet.measurement.access.deploy.tools.Access
import org.wfanet.measurement.access.service.Errors
import org.wfanet.measurement.access.service.internal.PermissionMapping
import org.wfanet.measurement.access.service.v1alpha.Services
import org.wfanet.measurement.access.v1alpha.CheckPermissionsResponse
import org.wfanet.measurement.access.v1alpha.ListPermissionsResponse
import org.wfanet.measurement.access.v1alpha.ListRolesResponse
import org.wfanet.measurement.access.v1alpha.Permission
import org.wfanet.measurement.access.v1alpha.PermissionsGrpc
import org.wfanet.measurement.access.v1alpha.PermissionsGrpc.PermissionsBlockingStub
import org.wfanet.measurement.access.v1alpha.PoliciesGrpc
import org.wfanet.measurement.access.v1alpha.PoliciesGrpc.PoliciesBlockingStub
import org.wfanet.measurement.access.v1alpha.Policy
import org.wfanet.measurement.access.v1alpha.PolicyKt.binding
import org.wfanet.measurement.access.v1alpha.Principal
import org.wfanet.measurement.access.v1alpha.PrincipalKt.oAuthUser
import org.wfanet.measurement.access.v1alpha.PrincipalsGrpc
import org.wfanet.measurement.access.v1alpha.PrincipalsGrpc.PrincipalsBlockingStub
import org.wfanet.measurement.access.v1alpha.Role
import org.wfanet.measurement.access.v1alpha.RolesGrpc
import org.wfanet.measurement.access.v1alpha.RolesGrpc.RolesBlockingStub
import org.wfanet.measurement.access.v1alpha.createPolicyRequest
import org.wfanet.measurement.access.v1alpha.createPrincipalRequest
import org.wfanet.measurement.access.v1alpha.createRoleRequest
import org.wfanet.measurement.access.v1alpha.deleteRoleRequest
import org.wfanet.measurement.access.v1alpha.getPrincipalRequest
import org.wfanet.measurement.access.v1alpha.listRolesResponse
import org.wfanet.measurement.access.v1alpha.permission
import org.wfanet.measurement.access.v1alpha.policy
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.access.v1alpha.role
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.config.AuthorityKeyToPrincipalMap

abstract class InProcessAccessCliTest(
  private val accessServicesFactory: AccessServicesFactory,
  verboseGrpcLogging: Boolean = true,
) {
  private val permissionMapping = PermissionMapping(PERMISSIONS_CONFIG)

  private val internalAccessServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      val tlsClientMapping =
        TlsClientPrincipalMapping(AuthorityKeyToPrincipalMap.getDefaultInstance())
      val services = accessServicesFactory.create(permissionMapping, tlsClientMapping).toList()
      for (service in services) {
        addService(service)
      }
    }

  @get:Rule
  val ruleChain: TestRule = chainRulesSequentially(accessServicesFactory, internalAccessServer)

  private lateinit var server: CommonServer

  private lateinit var principalsStub: PrincipalsBlockingStub
  private lateinit var rolesStub: RolesBlockingStub
  private lateinit var permissionsStub: PermissionsBlockingStub
  private lateinit var policiesStub: PoliciesBlockingStub

  @Before
  fun initServer() {
    val internalChannel: Channel = internalAccessServer.channel
    val services: List<ServerServiceDefinition> =
      Services.build(internalChannel).toList().map { it.bindService() }
    val serverCerts =
      SigningCerts.fromPemFiles(
        REPORTING_TLS_CERT_FILE,
        REPORTING_TLS_KEY_FILE,
        REPORTING_CERT_COLLECTION_FILE,
      )

    server =
      CommonServer.fromParameters(
        verboseGrpcLogging = true,
        certs = serverCerts,
        clientAuth = ClientAuth.REQUIRE,
        nameForLogging = "access-cli-test",
        services = services,
      )
    server.start()

    val publicChannel: ManagedChannel =
      buildMutualTlsChannel("localhost:${server.port}", serverCerts)
    principalsStub = PrincipalsGrpc.newBlockingStub(publicChannel)
    rolesStub = RolesGrpc.newBlockingStub(publicChannel)
    permissionsStub = PermissionsGrpc.newBlockingStub(publicChannel)
    policiesStub = PoliciesGrpc.newBlockingStub(publicChannel)

    createPrincipals()
    createRoles()
    createPolicy()
  }

  @After
  fun shutdownServer() {
    server.close()
  }

  @Test
  fun `principals get prints Principal`() = runBlocking {
    val args = commonArgs + arrayOf("principals", "get", UK_PRINCIPAL_NAME)
    val output = callCli(args)
    assertThat(parseTextProto(output.reader(), Principal.getDefaultInstance()))
      .isEqualTo(UK_PRINCIPAL)
  }

  @Test
  fun `principals create prints User Principal`() = runBlocking {
    val args =
      commonArgs +
        arrayOf(
          "principals",
          "create",
          "--issuer=au.com",
          "--subject=user1@au.com",
          "--principal-id=user-au",
        )
    val output = callCli(args)
    assertThat(parseTextProto(output.reader(), Principal.getDefaultInstance()))
      .isEqualTo(
        principal {
          name = "principals/user-au"
          user = oAuthUser {
            issuer = "au.com"
            subject = "user1@au.com"
          }
        }
      )
  }

  @Test
  fun `principals delete succeeds`(): Unit = runBlocking {
    val args = commonArgs + arrayOf("principals", "delete", UK_PRINCIPAL_NAME)
    callCli(args)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        principalsStub.getPrincipal(getPrincipalRequest { name = UK_PRINCIPAL_NAME })
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `principals prints Principal`() = runBlocking {
    val args =
      commonArgs +
        arrayOf(
          "principals",
          "lookup",
          "--issuer=$UK_OAUTH_USER_ISSUER",
          "--subject=$UK_OAUTH_USER_SUBJECT",
        )
    val output = callCli(args)
    assertThat(parseTextProto(output.reader(), Principal.getDefaultInstance()))
      .isEqualTo(UK_PRINCIPAL)
  }

  @Test
  fun `roles get prints Role`() = runBlocking {
    val args = commonArgs + arrayOf("roles", "get", REPORT_READER_ROLE_NAME)
    val output = callCli(args)
    assertThat(parseTextProto(output.reader(), Role.getDefaultInstance()))
      .ignoringFieldScope(ROLE_IGNORED_FIELDS)
      .isEqualTo(REPORT_READER_ROLE)
  }

  @Test
  fun `roles list prints Roles`() = runBlocking {
    val args = commonArgs + arrayOf("roles", "list", "--page-size=1000")
    val output = callCli(args)
    assertThat(parseTextProto(output.reader(), ListRolesResponse.getDefaultInstance()))
      .ignoringFieldScope(ROLE_IGNORED_FIELDS)
      .isEqualTo(
        listRolesResponse {
          roles += REPORT_READER_ROLE
          roles += REPORT_WRITER_ROLE
        }
      )
  }

  @Test
  fun `roles create prints Role`() = runBlocking {
    val args =
      commonArgs +
        arrayOf(
          "roles",
          "create",
          "--resource-type=reporting.halo-cmm.org/EventGroup",
          "--permission=permissions/reporting.eventGroups.get",
          "--role-id=event-group-reader",
        )
    val output = callCli(args)
    assertThat(parseTextProto(output.reader(), Role.getDefaultInstance()))
      .ignoringFieldScope(ROLE_IGNORED_FIELDS)
      .isEqualTo(
        role {
          name = "roles/event-group-reader"
          resourceTypes += "reporting.halo-cmm.org/EventGroup"
          permissions += "permissions/reporting.eventGroups.get"
        }
      )
  }

  @Test
  fun `roles update prints Role`() = runBlocking {
    var args = commonArgs + arrayOf("roles", "get", REPORT_READER_ROLE_NAME)
    var output = callCli(args)
    val etag = parseTextProto(output.reader(), Role.getDefaultInstance()).etag

    args =
      commonArgs +
        arrayOf(
          "roles",
          "update",
          "--name=$REPORT_READER_ROLE_NAME",
          "--resource-type=$REPORTING_SET_RESOURCE_TYPE",
          "--permission=$LIST_REPORTING_SET_PERMISSION_NAME",
          "--etag=$etag",
        )

    output = callCli(args)
    assertThat(parseTextProto(output.reader(), Role.getDefaultInstance()))
      .ignoringFieldScope(ROLE_IGNORED_FIELDS)
      .isEqualTo(
        role {
          name = REPORT_READER_ROLE_NAME
          resourceTypes += REPORTING_SET_RESOURCE_TYPE
          permissions += LIST_REPORTING_SET_PERMISSION_NAME
        }
      )
  }

  @Test
  fun `role delete succeeds`() = runBlocking {
    val args = commonArgs + arrayOf("roles", "delete", REPORT_WRITER_ROLE_NAME)

    callCli(args)
    val exception =
      assertFailsWith<StatusRuntimeException> {
        rolesStub.deleteRole(deleteRoleRequest { name = REPORT_WRITER_ROLE_NAME })
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.ROLE_NOT_FOUND.name
          metadata[Errors.Metadata.ROLE.key] = REPORT_WRITER_ROLE_NAME
        }
      )
  }

  @Test
  fun `permissions get prints Permission`() = runBlocking {
    val args = commonArgs + arrayOf("permissions", "get", GET_REPORTING_SET_PERMISSION_NAME)
    val output = callCli(args)
    assertThat(parseTextProto(output.reader(), Permission.getDefaultInstance()))
      .isEqualTo(GET_REPORTING_SET_PERMISSION)
  }

  @Test
  fun `permissions list prints Permissions`() = runBlocking {
    val args = commonArgs + arrayOf("permissions", "list")
    val output = callCli(args)
    assertEquals(
      permissionMapping.permissions.size,
      parseTextProto(output.reader(), ListPermissionsResponse.getDefaultInstance())
        .permissionsList
        .size,
    )
  }

  @Test
  fun `permissions check prints Permissions`() = runBlocking {
    val args =
      commonArgs +
        arrayOf(
          "permissions",
          "check",
          "--protected-resource=$MEASUREMENT_CONSUMER_RESOURCE_TYPE",
          "--principal=$UK_PRINCIPAL_NAME",
          "--permission=$GET_REPORTING_SET_PERMISSION_NAME",
          "--permission=$LIST_REPORTING_SET_PERMISSION_NAME",
        )
    val output = callCli(args)
    assertThat(parseTextProto(output.reader(), CheckPermissionsResponse.getDefaultInstance()))
      .isEqualTo(CheckPermissionsResponse.getDefaultInstance())
  }

  @Test
  fun `policies get prints Policy`() = runBlocking {
    val args = commonArgs + arrayOf("policies", "get", REPORT_READ_POLICY_NAME)
    val output = callCli(args)
    assertThat(parseTextProto(output.reader(), Policy.getDefaultInstance()))
      .ignoringFieldScope(POLICY_IGNORED_FIELDS)
      .isEqualTo(REPORT_READ_POLICY)
  }

  @Test
  fun `policies lookup prints Policy`() = runBlocking {
    val args =
      commonArgs + arrayOf("policies", "lookup", "--protected-resource=$MEASUREMENT_CONSUMER")
    val output = callCli(args)

    assertThat(parseTextProto(output.reader(), Policy.getDefaultInstance()))
      .ignoringFieldScope(POLICY_IGNORED_FIELDS)
      .isEqualTo(REPORT_READ_POLICY)
  }

  @Test
  fun `policies create prints Policy`() = runBlocking {
    val args =
      commonArgs +
        arrayOf(
          "policies",
          "create",
          "--protected-resource=measurementConsumers/mc-2",
          "--binding-role=$REPORT_WRITER_ROLE_NAME",
          "--binding-member=$US_PRINCIPAL_NAME",
          "--binding-member=$UK_PRINCIPAL_NAME",
          "--policy-id=report-writer-policy",
        )
    val output = callCli(args)

    assertThat(parseTextProto(output.reader(), Policy.getDefaultInstance()))
      .ignoringFieldScope(POLICY_IGNORED_FIELDS)
      .isEqualTo(
        policy {
          name = "policies/report-writer-policy"
          protectedResource = "measurementConsumers/mc-2"
          bindings += binding {
            role = REPORT_WRITER_ROLE_NAME
            members += US_PRINCIPAL_NAME
            members += UK_PRINCIPAL_NAME
          }
        }
      )
  }

  @Test
  fun `policies add-members prints Policy`() = runBlocking {
    var args = commonArgs + arrayOf("policies", "get", REPORT_READ_POLICY_NAME)
    var output = callCli(args)
    val etag = parseTextProto(output.reader(), Policy.getDefaultInstance()).etag

    args =
      commonArgs +
        arrayOf(
          "policies",
          "add-members",
          "--name=$REPORT_READ_POLICY_NAME",
          "--binding-role=$REPORT_READER_ROLE_NAME",
          "--binding-member=$UK_PRINCIPAL_NAME",
          "--etag=$etag",
        )
    output = callCli(args)

    assertThat(parseTextProto(output.reader(), Policy.getDefaultInstance()))
      .ignoringFieldScope(POLICY_IGNORED_FIELDS)
      .isEqualTo(
        policy {
          name = REPORT_READ_POLICY_NAME
          protectedResource = MEASUREMENT_CONSUMER
          bindings += binding {
            role = REPORT_READER_ROLE_NAME
            members += US_PRINCIPAL_NAME
            members += UK_PRINCIPAL_NAME
          }
        }
      )
  }

  @Test
  fun `policies remove-members prints Policy`() = runBlocking {
    var args = commonArgs + arrayOf("policies", "get", REPORT_READ_POLICY_NAME)
    var output = callCli(args)
    val etag = parseTextProto(output.reader(), Policy.getDefaultInstance()).etag

    args =
      commonArgs +
        arrayOf(
          "policies",
          "remove-members",
          "--name=$REPORT_READ_POLICY_NAME",
          "--binding-role=$REPORT_READER_ROLE_NAME",
          "--binding-member=$US_PRINCIPAL_NAME",
          "--etag=$etag",
        )
    output = callCli(args)

    assertThat(parseTextProto(output.reader(), Policy.getDefaultInstance()))
      .ignoringFieldScope(POLICY_IGNORED_FIELDS)
      .isEqualTo(
        policy {
          name = REPORT_READ_POLICY_NAME
          protectedResource = MEASUREMENT_CONSUMER
        }
      )
  }

  private fun createPrincipals() {
    principalsStub.createPrincipal(
      createPrincipalRequest {
        principal = UK_PRINCIPAL
        principalId = UK_PRINCIPAL_ID
      }
    )

    principalsStub.createPrincipal(
      createPrincipalRequest {
        principal = US_PRINCIPAL
        principalId = US_PRINCIPAL_ID
      }
    )
  }

  private fun createRoles() {
    rolesStub.createRole(
      createRoleRequest {
        role = REPORT_READER_ROLE
        roleId = REPORT_READER_ROLE_ID
      }
    )

    rolesStub.createRole(
      createRoleRequest {
        role = REPORT_WRITER_ROLE
        roleId = REPORT_WRITER_ROLE_ID
      }
    )
  }

  private fun createPolicy() {
    policiesStub.createPolicy(
      createPolicyRequest {
        policy = policy {
          protectedResource = MEASUREMENT_CONSUMER
          bindings += binding {
            role = REPORT_READER_ROLE_NAME
            members += US_PRINCIPAL_NAME
          }
        }
        policyId = REPORT_READ_POLICY_ID
      }
    )
  }

  private val commonArgs: Array<String>
    get() =
      arrayOf(
        "--tls-cert-file=$REPORTING_TLS_CERT_FILE",
        "--tls-key-file=$REPORTING_TLS_KEY_FILE",
        "--cert-collection-file=$REPORTING_CERT_COLLECTION_FILE",
        "--access-public-api-target=$HOST:${server.port}",
      )

  private fun callCli(args: Array<String>): String {
    val capturedOutput = CommandLineTesting.capturingOutput(args, Access::main)
    CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    return capturedOutput.out
  }

  companion object {
    private const val HOST = "localhost"
    private val SECRETS_DIR: File =
      getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )!!
        .toFile()
    private val REPORTING_TLS_CERT_FILE: File = SECRETS_DIR.resolve("reporting_tls.pem")
    private val REPORTING_TLS_KEY_FILE: File = SECRETS_DIR.resolve("reporting_tls.key")
    private val REPORTING_CERT_COLLECTION_FILE: File = SECRETS_DIR.resolve("reporting_root.pem")

    private const val UK_PRINCIPAL_ID = "uk-user"
    private const val UK_PRINCIPAL_NAME = "principals/uk-user"
    private const val UK_OAUTH_USER_ISSUER = "uk.com"
    private const val UK_OAUTH_USER_SUBJECT = "user@uk.com"
    private val UK_PRINCIPAL = principal {
      name = UK_PRINCIPAL_NAME
      user = oAuthUser {
        issuer = UK_OAUTH_USER_ISSUER
        subject = UK_OAUTH_USER_SUBJECT
      }
    }

    private const val US_PRINCIPAL_ID = "us-user"
    private const val US_PRINCIPAL_NAME = "principals/us-user"
    private const val US_OAUTH_USER_ISSUER = "us.com"
    private const val US_OAUTH_USER_SUBJECT = "user@us.com"
    private val US_PRINCIPAL = principal {
      name = US_PRINCIPAL_NAME
      user = oAuthUser {
        issuer = US_OAUTH_USER_ISSUER
        subject = US_OAUTH_USER_SUBJECT
      }
    }

    private const val MEASUREMENT_CONSUMER_RESOURCE_TYPE = "halo.wfanet.org/MeasurementConsumer"
    private const val MEASUREMENT_CONSUMER = "measurementConsumers/mc-1"
    private const val REPORTING_SET_RESOURCE_TYPE = "reporting.halo-cmm.org/ReportingSet"

    private val ROLE_IGNORED_FIELDS: FieldScope =
      FieldScopes.allowingFieldDescriptors(
        Role.getDescriptor().findFieldByNumber(Role.ETAG_FIELD_NUMBER)
      )
    private const val REPORT_READER_ROLE_ID = "report-reader"
    private const val REPORT_READER_ROLE_NAME = "roles/report-reader"
    private val REPORT_READER_ROLE = role {
      name = REPORT_READER_ROLE_NAME
      resourceTypes += MEASUREMENT_CONSUMER_RESOURCE_TYPE
      permissions += GET_REPORTING_SET_PERMISSION_NAME
    }

    private const val REPORT_WRITER_ROLE_ID = "report-writer"
    private const val REPORT_WRITER_ROLE_NAME = "roles/report-writer"
    private val REPORT_WRITER_ROLE = role {
      name = REPORT_WRITER_ROLE_NAME
      resourceTypes += MEASUREMENT_CONSUMER_RESOURCE_TYPE
      permissions += GET_REPORTING_SET_PERMISSION_NAME
    }

    private const val GET_REPORTING_SET_PERMISSION_NAME = "permissions/reporting.reportingSets.get"
    private val GET_REPORTING_SET_PERMISSION = permission {
      name = GET_REPORTING_SET_PERMISSION_NAME
      resourceTypes += MEASUREMENT_CONSUMER_RESOURCE_TYPE
      resourceTypes += REPORTING_SET_RESOURCE_TYPE
    }
    private const val LIST_REPORTING_SET_PERMISSION_NAME = "permissions/reporting.reportingSets.get"

    private const val REPORT_READ_POLICY_ID = "report-reader-policy"
    private const val REPORT_READ_POLICY_NAME = "policies/$REPORT_READ_POLICY_ID"
    private val POLICY_IGNORED_FIELDS: FieldScope =
      FieldScopes.allowingFieldDescriptors(
        Policy.getDescriptor().findFieldByNumber(Policy.ETAG_FIELD_NUMBER)
      )
    private val REPORT_READ_POLICY = policy {
      name = REPORT_READ_POLICY_NAME
      protectedResource = MEASUREMENT_CONSUMER
      bindings += binding {
        role = REPORT_READER_ROLE_NAME
        members += US_PRINCIPAL_NAME
      }
    }
  }
}
