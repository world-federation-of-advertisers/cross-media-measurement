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

package org.wfanet.measurement.access.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.access.common.TlsClientPrincipalMapping
import org.wfanet.measurement.access.service.internal.Errors
import org.wfanet.measurement.access.service.internal.PermissionMapping
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.access.PoliciesGrpcKt
import org.wfanet.measurement.internal.access.Policy
import org.wfanet.measurement.internal.access.PolicyKt
import org.wfanet.measurement.internal.access.PrincipalKt.oAuthUser
import org.wfanet.measurement.internal.access.PrincipalsGrpcKt
import org.wfanet.measurement.internal.access.RolesGrpcKt
import org.wfanet.measurement.internal.access.addPolicyBindingMembersRequest
import org.wfanet.measurement.internal.access.copy
import org.wfanet.measurement.internal.access.createUserPrincipalRequest
import org.wfanet.measurement.internal.access.getPolicyRequest
import org.wfanet.measurement.internal.access.lookupPolicyRequest
import org.wfanet.measurement.internal.access.policy
import org.wfanet.measurement.internal.access.removePolicyBindingMembersRequest
import org.wfanet.measurement.internal.access.role

@RunWith(JUnit4::class)
abstract class PoliciesServiceTest {
  protected data class Services(
    /** Service under test. */
    val service: PoliciesGrpcKt.PoliciesCoroutineImplBase,
    val principalsService: PrincipalsGrpcKt.PrincipalsCoroutineImplBase,
    val rolesServices: RolesGrpcKt.RolesCoroutineImplBase,
  )

  protected abstract fun initServices(
    permissionMapping: PermissionMapping,
    tlsClientMapping: TlsClientPrincipalMapping,
    idGenerator: IdGenerator,
  ): Services

  private fun initServices(idGenerator: IdGenerator = IdGenerator.Default) =
    initServices(TestConfig.PERMISSION_MAPPING, TestConfig.TLS_CLIENT_MAPPING, idGenerator)

  @Test
  fun `createPolicy returns created Policy`() = runBlocking {
    val (service, principalsService, rolesService) = initServices()
    val startTime = Instant.now()
    val principal = principalsService.createUserPrincipal(CREATE_USER_PRINCIPAL_REQUEST)
    val role = rolesService.createRole(CREATE_ROLE_REQUEST)

    val request = policy {
      policyResourceId = "fantasy-shelf-policy"
      protectedResourceName = "shelves/fantasy"
      bindings[role.roleResourceId] =
        PolicyKt.members { memberPrincipalResourceIds += principal.principalResourceId }
    }
    val response: Policy = service.createPolicy(request)

    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .ignoringFields(
        Policy.CREATE_TIME_FIELD_NUMBER,
        Policy.UPDATE_TIME_FIELD_NUMBER,
        Policy.ETAG_FIELD_NUMBER,
      )
      .isEqualTo(request)
    assertThat(response.createTime.toInstant()).isGreaterThan(startTime)
    assertThat(response.updateTime).isEqualTo(response.createTime)
    assertThat(response.etag).isNotEmpty()
    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        service.getPolicy(getPolicyRequest { policyResourceId = request.policyResourceId })
      )
  }

  @Test
  fun `createPolicy throws INVALID_ARGUMENT if binding has no members`() = runBlocking {
    val (service, _, rolesService) = initServices()
    val role = rolesService.createRole(CREATE_ROLE_REQUEST)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createPolicy(
          policy {
            policyResourceId = "fantasy-shelf-policy"
            protectedResourceName = "shelves/fantasy"
            bindings[role.roleResourceId] = Policy.Members.getDefaultInstance()
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "member_principal_resource_ids"
        }
      )
  }

  @Test
  fun `lookupPolicy returns Policy`() = runBlocking {
    val (service, principalsService, rolesService) = initServices()
    val principal = principalsService.createUserPrincipal(CREATE_USER_PRINCIPAL_REQUEST)
    val role = rolesService.createRole(CREATE_ROLE_REQUEST)
    val policy: Policy =
      service.createPolicy(
        policy {
          policyResourceId = "fantasy-shelf-policy"
          protectedResourceName = "shelves/fantasy"
          bindings[role.roleResourceId] =
            PolicyKt.members { memberPrincipalResourceIds += principal.principalResourceId }
        }
      )

    val response: Policy =
      service.lookupPolicy(
        lookupPolicyRequest { protectedResourceName = policy.protectedResourceName }
      )

    assertThat(response).isEqualTo(policy)
  }

  @Test
  fun `addPolicyBindingMembers adds members to Policy binding`() = runBlocking {
    val (service, principalsService, rolesService) = initServices()
    val principal = principalsService.createUserPrincipal(CREATE_USER_PRINCIPAL_REQUEST)
    val role = rolesService.createRole(CREATE_ROLE_REQUEST)
    val policy: Policy =
      service.createPolicy(
        policy {
          policyResourceId = "fantasy-shelf-policy"
          protectedResourceName = "shelves/fantasy"
        }
      )

    val response =
      service.addPolicyBindingMembers(
        addPolicyBindingMembersRequest {
          policyResourceId = policy.policyResourceId
          roleResourceId = role.roleResourceId
          memberPrincipalResourceIds += principal.principalResourceId
        }
      )

    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .ignoringFields(Policy.UPDATE_TIME_FIELD_NUMBER, Policy.ETAG_FIELD_NUMBER)
      .isEqualTo(
        policy.copy {
          bindings[role.roleResourceId] =
            PolicyKt.members { memberPrincipalResourceIds += principal.principalResourceId }
        }
      )
    assertThat(response.updateTime.toInstant()).isGreaterThan(policy.updateTime.toInstant())
    assertThat(response.etag).isNotEqualTo(policy.etag)
    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(service.getPolicy(getPolicyRequest { policyResourceId = policy.policyResourceId }))
  }

  @Test
  fun `addPolicyBindingMembers throws FAILED_PRECONDITION if binding already has member`() =
    runBlocking {
      val (service, principalsService, rolesService) = initServices()
      val principal = principalsService.createUserPrincipal(CREATE_USER_PRINCIPAL_REQUEST)
      val role = rolesService.createRole(CREATE_ROLE_REQUEST)
      val policy: Policy =
        service.createPolicy(
          policy {
            policyResourceId = "fantasy-shelf-policy"
            protectedResourceName = "shelves/fantasy"
            bindings[role.roleResourceId] =
              PolicyKt.members { memberPrincipalResourceIds += principal.principalResourceId }
          }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.addPolicyBindingMembers(
            addPolicyBindingMembersRequest {
              policyResourceId = policy.policyResourceId
              roleResourceId = role.roleResourceId
              memberPrincipalResourceIds += principal.principalResourceId
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.POLICY_BINDING_MEMBERSHIP_ALREADY_EXISTS.name
            metadata[Errors.Metadata.POLICY_RESOURCE_ID.key] = policy.policyResourceId
            metadata[Errors.Metadata.ROLE_RESOURCE_ID.key] = role.roleResourceId
            metadata[Errors.Metadata.PRINCIPAL_RESOURCE_ID.key] = principal.principalResourceId
          }
        )
    }

  @Test
  fun `addPolicyBindingMembers throws ABORTED on etag mismatch`() = runBlocking {
    val (service, principalsService, rolesService) = initServices()
    val principal = principalsService.createUserPrincipal(CREATE_USER_PRINCIPAL_REQUEST)
    val role = rolesService.createRole(CREATE_ROLE_REQUEST)
    val policy: Policy =
      service.createPolicy(
        policy {
          policyResourceId = "fantasy-shelf-policy"
          protectedResourceName = "shelves/fantasy"
        }
      )

    val request = addPolicyBindingMembersRequest {
      policyResourceId = policy.policyResourceId
      roleResourceId = role.roleResourceId
      memberPrincipalResourceIds += principal.principalResourceId
      etag = "invalid"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.addPolicyBindingMembers(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.ETAG_MISMATCH.name
          metadata[Errors.Metadata.ETAG.key] = policy.etag
          metadata[Errors.Metadata.REQUEST_ETAG.key] = request.etag
        }
      )
  }

  @Test
  fun `removePolicyBindingMembers removes members from Policy binding`() = runBlocking {
    val (service, principalsService, rolesService) = initServices()
    val principal = principalsService.createUserPrincipal(CREATE_USER_PRINCIPAL_REQUEST)
    val principal2 = principalsService.createUserPrincipal(CREATE_USER_PRINCIPAL_2_REQUEST)
    val role = rolesService.createRole(CREATE_ROLE_REQUEST)
    val policy: Policy =
      service.createPolicy(
        policy {
          policyResourceId = "fantasy-shelf-policy"
          protectedResourceName = "shelves/fantasy"
          bindings[role.roleResourceId] =
            PolicyKt.members {
              memberPrincipalResourceIds += principal.principalResourceId
              memberPrincipalResourceIds += principal2.principalResourceId
            }
        }
      )

    val response: Policy =
      service.removePolicyBindingMembers(
        removePolicyBindingMembersRequest {
          policyResourceId = policy.policyResourceId
          roleResourceId = role.roleResourceId
          memberPrincipalResourceIds += principal2.principalResourceId
        }
      )

    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .ignoringFields(Policy.UPDATE_TIME_FIELD_NUMBER, Policy.ETAG_FIELD_NUMBER)
      .isEqualTo(
        policy.copy {
          bindings[role.roleResourceId] =
            PolicyKt.members { memberPrincipalResourceIds += principal.principalResourceId }
        }
      )
    assertThat(response.updateTime.toInstant()).isGreaterThan(policy.updateTime.toInstant())
    assertThat(response.etag).isNotEqualTo(policy.etag)
    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(service.getPolicy(getPolicyRequest { policyResourceId = policy.policyResourceId }))
  }

  @Test
  fun `removePolicyBindingMembers removes all members from Policy binding`() = runBlocking {
    val (service, principalsService, rolesService) = initServices()
    val principal = principalsService.createUserPrincipal(CREATE_USER_PRINCIPAL_REQUEST)
    val principal2 = principalsService.createUserPrincipal(CREATE_USER_PRINCIPAL_2_REQUEST)
    val role = rolesService.createRole(CREATE_ROLE_REQUEST)
    val policy: Policy =
      service.createPolicy(
        policy {
          policyResourceId = "fantasy-shelf-policy"
          protectedResourceName = "shelves/fantasy"
          bindings[role.roleResourceId] =
            PolicyKt.members {
              memberPrincipalResourceIds += principal.principalResourceId
              memberPrincipalResourceIds += principal2.principalResourceId
            }
        }
      )

    val response: Policy =
      service.removePolicyBindingMembers(
        removePolicyBindingMembersRequest {
          policyResourceId = policy.policyResourceId
          roleResourceId = role.roleResourceId
          memberPrincipalResourceIds += principal.principalResourceId
          memberPrincipalResourceIds += principal2.principalResourceId
        }
      )

    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .ignoringFields(Policy.UPDATE_TIME_FIELD_NUMBER, Policy.ETAG_FIELD_NUMBER)
      .isEqualTo(policy.copy { bindings.clear() })
    assertThat(response.updateTime.toInstant()).isGreaterThan(policy.updateTime.toInstant())
    assertThat(response.etag).isNotEqualTo(policy.etag)
    assertThat(response)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(service.getPolicy(getPolicyRequest { policyResourceId = policy.policyResourceId }))
  }

  @Test
  fun `removePolicyBindingMembers throws FAILED_PRECONDITION if binding does not have member`() =
    runBlocking {
      val (service, principalsService, rolesService) = initServices()
      val principal = principalsService.createUserPrincipal(CREATE_USER_PRINCIPAL_REQUEST)
      val role = rolesService.createRole(CREATE_ROLE_REQUEST)
      val policy: Policy =
        service.createPolicy(
          policy {
            policyResourceId = "fantasy-shelf-policy"
            protectedResourceName = "shelves/fantasy"
          }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.removePolicyBindingMembers(
            removePolicyBindingMembersRequest {
              policyResourceId = policy.policyResourceId
              roleResourceId = role.roleResourceId
              memberPrincipalResourceIds += principal.principalResourceId
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.POLICY_BINDING_MEMBERSHIP_NOT_FOUND.name
            metadata[Errors.Metadata.POLICY_RESOURCE_ID.key] = policy.policyResourceId
            metadata[Errors.Metadata.ROLE_RESOURCE_ID.key] = role.roleResourceId
            metadata[Errors.Metadata.PRINCIPAL_RESOURCE_ID.key] = principal.principalResourceId
          }
        )
    }

  @Test
  fun `removePolicyBindingMembers throws ABORTED on etag mistmatch`() = runBlocking {
    val (service, principalsService, rolesService) = initServices()
    val principal = principalsService.createUserPrincipal(CREATE_USER_PRINCIPAL_REQUEST)
    val role = rolesService.createRole(CREATE_ROLE_REQUEST)
    val policy: Policy =
      service.createPolicy(
        policy {
          policyResourceId = "fantasy-shelf-policy"
          protectedResourceName = "shelves/fantasy"
          bindings[role.roleResourceId] =
            PolicyKt.members { memberPrincipalResourceIds += principal.principalResourceId }
        }
      )

    val request = removePolicyBindingMembersRequest {
      policyResourceId = policy.policyResourceId
      roleResourceId = role.roleResourceId
      memberPrincipalResourceIds += principal.principalResourceId
      etag = "invalid"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.removePolicyBindingMembers(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.ETAG_MISMATCH.name
          metadata[Errors.Metadata.ETAG.key] = policy.etag
          metadata[Errors.Metadata.REQUEST_ETAG.key] = request.etag
        }
      )
  }

  companion object {
    private val CREATE_USER_PRINCIPAL_REQUEST = createUserPrincipalRequest {
      principalResourceId = "user-1"
      user = oAuthUser {
        issuer = "example-issuer"
        subject = "user@example.com"
      }
    }

    private val CREATE_USER_PRINCIPAL_2_REQUEST =
      CREATE_USER_PRINCIPAL_REQUEST.copy {
        principalResourceId = "user-2"
        user = user.copy { subject = "user2@example.com" }
      }

    private val CREATE_ROLE_REQUEST = role {
      roleResourceId = "shelfBookReader"
      permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_GET
      permissionResourceIds += TestConfig.PermissionResourceId.BOOKS_LIST
      resourceTypes += TestConfig.ResourceType.SHELF
    }
  }
}
