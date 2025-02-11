// Copyright 2025 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.access.service.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.stub
import org.wfanet.measurement.access.service.Errors
import org.wfanet.measurement.access.service.internal.EtagMismatchException
import org.wfanet.measurement.access.service.internal.PolicyAlreadyExistsException
import org.wfanet.measurement.access.service.internal.PolicyBindingMembershipAlreadyExistsException
import org.wfanet.measurement.access.service.internal.PolicyBindingMembershipNotFoundException
import org.wfanet.measurement.access.service.internal.PolicyNotFoundException
import org.wfanet.measurement.access.service.internal.PolicyNotFoundForProtectedResourceException
import org.wfanet.measurement.access.service.internal.PrincipalNotFoundException
import org.wfanet.measurement.access.service.internal.PrincipalTypeNotSupportedException
import org.wfanet.measurement.access.service.internal.RoleNotFoundException
import org.wfanet.measurement.access.v1alpha.GetPolicyRequest
import org.wfanet.measurement.access.v1alpha.LookupPolicyRequest
import org.wfanet.measurement.access.v1alpha.PolicyKt
import org.wfanet.measurement.access.v1alpha.addPolicyBindingMembersRequest
import org.wfanet.measurement.access.v1alpha.createPolicyRequest
import org.wfanet.measurement.access.v1alpha.getPolicyRequest
import org.wfanet.measurement.access.v1alpha.lookupPolicyRequest
import org.wfanet.measurement.access.v1alpha.policy
import org.wfanet.measurement.access.v1alpha.removePolicyBindingMembersRequest
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.access.PoliciesGrpcKt as InternalPoliciesGrpcKt
import org.wfanet.measurement.internal.access.PolicyKt as InternalPolicyKt
import org.wfanet.measurement.internal.access.Principal
import org.wfanet.measurement.internal.access.addPolicyBindingMembersRequest as internalAddPolicyBindingMembersRequest
import org.wfanet.measurement.internal.access.getPolicyRequest as internalGetPolicyRequest
import org.wfanet.measurement.internal.access.lookupPolicyRequest as internalLookupPolicyRequest
import org.wfanet.measurement.internal.access.policy as internalPolicy
import org.wfanet.measurement.internal.access.removePolicyBindingMembersRequest as internalRemovePolicyBindingMembersRequest

@RunWith(JUnit4::class)
class PoliciesServiceTest {
  private val internalServiceMock = mockService<InternalPoliciesGrpcKt.PoliciesCoroutineImplBase>()

  @get:Rule val grpcTestServer = GrpcTestServerRule { addService(internalServiceMock) }

  private lateinit var service: PoliciesService

  @Before
  fun initService() {
    service = PoliciesService(InternalPoliciesGrpcKt.PoliciesCoroutineStub(grpcTestServer.channel))
  }

  @Test
  fun `getPolicy returns policy`() = runBlocking {
    val internalPolicy = internalPolicy {
      policyResourceId = "policy-1"
      protectedResourceName = "books"
      bindings.put(
        "bookReader",
        InternalPolicyKt.members { memberPrincipalResourceIds += "user-1" },
      )
      etag = "etag"
    }

    internalServiceMock.stub { onBlocking { getPolicy(any()) } doReturn internalPolicy }

    val request = getPolicyRequest { name = "policies/${internalPolicy.policyResourceId}" }
    val response = service.getPolicy(request)

    verifyProtoArgument(
        internalServiceMock,
        InternalPoliciesGrpcKt.PoliciesCoroutineImplBase::getPolicy,
      )
      .isEqualTo(internalGetPolicyRequest { policyResourceId = internalPolicy.policyResourceId })
    assertThat(response)
      .isEqualTo(
        policy {
          name = request.name
          protectedResource = internalPolicy.protectedResourceName
          bindings +=
            PolicyKt.binding {
              role = "roles/bookReader"
              members += "principals/user-1"
            }
          etag = internalPolicy.etag
        }
      )
  }

  @Test
  fun `getPolicy throws REQUIRED_FIELD_NOT_SET when name is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getPolicy(GetPolicyRequest.getDefaultInstance())
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "name"
        }
      )
  }

  @Test
  fun `getPrincipal throws INVALID_FIELD_VALUE when name is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getPolicy(getPolicyRequest { name = "policy-1" })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "name"
        }
      )
  }

  @Test
  fun `getPolicy throws POLICY_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { getPolicy(any()) } doThrow
        PolicyNotFoundException("policy-1").asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    val request = getPolicyRequest { name = "policies/policy-1" }
    val exception = assertFailsWith<StatusRuntimeException> { service.getPolicy(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.POLICY_NOT_FOUND.name
          metadata[Errors.Metadata.POLICY.key] = request.name
        }
      )
  }

  @Test
  fun `createPolicy returns policy`() = runBlocking {
    val internalPolicy = internalPolicy {
      policyResourceId = "policy-1"
      protectedResourceName = "books"
      bindings.put(
        "bookReader",
        InternalPolicyKt.members { memberPrincipalResourceIds += "user-1" },
      )
      etag = "etag"
    }
    internalServiceMock.stub { onBlocking { createPolicy(any()) } doReturn internalPolicy }

    val request = createPolicyRequest {
      policy = policy {
        name = "policies/policy-1"
        protectedResource = "books"
        bindings +=
          PolicyKt.binding {
            role = "roles/bookReader"
            members += "principals/user-1"
          }
        etag = "etag"
      }
      policyId = "policy-1"
    }
    val response = service.createPolicy(request)

    verifyProtoArgument(
        internalServiceMock,
        InternalPoliciesGrpcKt.PoliciesCoroutineImplBase::createPolicy,
      )
      .isEqualTo(internalPolicy)

    assertThat(response).isEqualTo(request.policy)
  }

  @Test
  fun `createPolicy throws REQUIRED_FIELD_NOT_SET when policy is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createPolicy(createPolicyRequest { policyId = "policy-1" })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "policy"
        }
      )
  }

  @Test
  fun `createPolicy throws REQUIRED_FIELD_NOT_SET when policy id is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createPolicy(
          createPolicyRequest {
            policy = policy {
              name = "policies/policy-1"
              protectedResource = "books"
              bindings +=
                PolicyKt.binding {
                  role = "roles/bookReader"
                  members += "principals/user-1"
                }
              etag = "etag"
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "policy_id"
        }
      )
  }

  @Test
  fun `createPolicy throws INVALID_FIELD_VALUE when policy id is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createPolicy(
          createPolicyRequest {
            policy = policy {
              name = "policies/policy-1"
              protectedResource = "books"
              bindings +=
                PolicyKt.binding {
                  role = "roles/bookReader"
                  members += "principals/user-1"
                }
              etag = "etag"
            }
            policyId = "policies/policy-1"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "policy_id"
        }
      )
  }

  @Test
  fun `createPolicy throws INVALID_FIELD_VALUE when policy's bindings's role is malformed`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createPolicy(
            createPolicyRequest {
              policy = policy {
                name = "policies/policy-1"
                protectedResource = "books"
                bindings +=
                  PolicyKt.binding {
                    role = "bookReader"
                    members += "principals/user-1"
                  }
                etag = "etag"
              }
              policyId = "policy-1"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "policy.bindings.role"
          }
        )
    }

  @Test
  fun `createPolicy throws INVALID_FIELD_VALUE when policy's bindings's members is malformed`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createPolicy(
            createPolicyRequest {
              policy = policy {
                name = "policies/policy-1"
                protectedResource = "books"
                bindings +=
                  PolicyKt.binding {
                    role = "roles/bookReader"
                    members += "user-1"
                  }
                etag = "etag"
              }
              policyId = "policy-1"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "policy.bindings.members"
          }
        )
    }

  @Test
  fun `createPolicy throws PRINCIPAL_TYPE_NOT_SUPPORTED from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { createPolicy(any()) } doThrow
        PrincipalTypeNotSupportedException("user-1", Principal.IdentityCase.TLS_CLIENT)
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val request = createPolicyRequest {
      policy = policy {
        name = "policies/policy-1"
        protectedResource = "books"
        bindings +=
          PolicyKt.binding {
            role = "roles/bookReader"
            members += "principals/user-1"
          }
        etag = "etag"
      }
      policyId = "policy-1"
    }
    val exception = assertFailsWith<StatusRuntimeException> { service.createPolicy(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED.name
          metadata[Errors.Metadata.PRINCIPAL_TYPE.key] = Principal.IdentityCase.TLS_CLIENT.name
        }
      )
  }

  @Test
  fun `createPolicy throws POLICY_ALREADY_EXISTS from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { createPolicy(any()) } doThrow
        PolicyAlreadyExistsException().asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
    }

    val request = createPolicyRequest {
      policy = policy {
        name = "policies/policy-1"
        protectedResource = "books"
        bindings +=
          PolicyKt.binding {
            role = "roles/bookReader"
            members += "principals/user-1"
          }
        etag = "etag"
      }
      policyId = "policy-1"
    }
    val exception = assertFailsWith<StatusRuntimeException> { service.createPolicy(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.POLICY_ALREADY_EXISTS.name
        }
      )
  }

  @Test
  fun `createPolicy throws ROLE_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { createPolicy(any()) } doThrow
        RoleNotFoundException("bookReader").asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    val request = createPolicyRequest {
      policy = policy {
        name = "policies/policy-1"
        protectedResource = "books"
        bindings +=
          PolicyKt.binding {
            role = "roles/bookReader"
            members += "principals/user-1"
          }
        etag = "etag"
      }
      policyId = "policy-1"
    }
    val exception = assertFailsWith<StatusRuntimeException> { service.createPolicy(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.ROLE_NOT_FOUND.name
          metadata[Errors.Metadata.ROLE.key] = "roles/bookReader"
        }
      )
  }

  @Test
  fun `createPolicy throws PRINCIPAL_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { createPolicy(any()) } doThrow
        PrincipalNotFoundException("user-1")
          .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }

    val request = createPolicyRequest {
      policy = policy {
        name = "policies/policy-1"
        protectedResource = "books"
        bindings +=
          PolicyKt.binding {
            role = "roles/bookReader"
            members += "principals/user-1"
            members += "principals/user-2"
          }
        etag = "etag"
      }
      policyId = "policy-1"
    }
    val exception = assertFailsWith<StatusRuntimeException> { service.createPolicy(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.PRINCIPAL_NOT_FOUND.name
          metadata[Errors.Metadata.PRINCIPAL.key] = "principals/user-1"
        }
      )
  }

  @Test
  fun `lookupPolicy returns policy`() = runBlocking {
    val internalPolicy = internalPolicy {
      policyResourceId = "policy-1"
      protectedResourceName = "books"
      bindings.put(
        "bookReader",
        InternalPolicyKt.members { memberPrincipalResourceIds += "user-1" },
      )
      etag = "etag"
    }
    internalServiceMock.stub { onBlocking { lookupPolicy(any()) } doReturn internalPolicy }

    val request = lookupPolicyRequest { protectedResource = "books" }
    val response = service.lookupPolicy(request)

    verifyProtoArgument(
        internalServiceMock,
        InternalPoliciesGrpcKt.PoliciesCoroutineImplBase::lookupPolicy,
      )
      .isEqualTo(
        internalLookupPolicyRequest { protectedResourceName = internalPolicy.protectedResourceName }
      )

    assertThat(response)
      .isEqualTo(
        policy {
          name = "policies/${internalPolicy.policyResourceId}"
          protectedResource = request.protectedResource
          bindings +=
            PolicyKt.binding {
              role = "roles/bookReader"
              members += "principals/user-1"
            }
          etag = "etag"
        }
      )
  }

  @Test
  fun `lookupPolicy throws REQUIRED_FIELD_NOT_SET when protected resource is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.lookupPolicy(LookupPolicyRequest.getDefaultInstance())
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "protected_resource"
          }
        )
    }

  @Test
  fun `lookupPolicy throws POLICY_NOT_FOUND_FOR_PROTECTED_RESOURCE from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { lookupPolicy(any()) } doThrow
        PolicyNotFoundForProtectedResourceException("books")
          .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
    }

    val request = lookupPolicyRequest { protectedResource = "books" }
    val exception = assertFailsWith<StatusRuntimeException> { service.lookupPolicy(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.POLICY_NOT_FOUND_FOR_PROTECTED_RESOURCE.name
          metadata[Errors.Metadata.PROTECTED_RESOURCE.key] = request.protectedResource
        }
      )
  }

  @Test
  fun `addPolicyBindingMembers returns policy`() = runBlocking {
    val internalPolicy = internalPolicy {
      policyResourceId = "policy-1"
      protectedResourceName = "books"
      bindings.put(
        "bookReader",
        InternalPolicyKt.members {
          memberPrincipalResourceIds += "user-1"
          memberPrincipalResourceIds += "user-2"
        },
      )
      etag = "etag"
    }
    internalServiceMock.stub {
      onBlocking { addPolicyBindingMembers(any()) } doReturn internalPolicy
    }

    val request = addPolicyBindingMembersRequest {
      name = "policies/${internalPolicy.policyResourceId}"
      role = "roles/bookReader"
      members += "principals/user-2"
      etag = internalPolicy.etag
    }
    val response = service.addPolicyBindingMembers(request)

    verifyProtoArgument(
        internalServiceMock,
        InternalPoliciesGrpcKt.PoliciesCoroutineImplBase::addPolicyBindingMembers,
      )
      .isEqualTo(
        internalAddPolicyBindingMembersRequest {
          policyResourceId = internalPolicy.policyResourceId
          roleResourceId = "bookReader"
          memberPrincipalResourceIds += "user-2"
          etag = internalPolicy.etag
        }
      )

    assertThat(response)
      .isEqualTo(
        policy {
          name = "policies/${internalPolicy.policyResourceId}"
          protectedResource = internalPolicy.protectedResourceName
          bindings +=
            PolicyKt.binding {
              role = "roles/bookReader"
              members += "principals/user-1"
              members += "principals/user-2"
            }
          etag = request.etag
        }
      )
  }

  @Test
  fun `addPolicyBindingMembers throws REQUIRED_FIELD_NOT_SET when name is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.addPolicyBindingMembers(
          addPolicyBindingMembersRequest {
            role = "roles/bookReader"
            members += "principals/user-2"
            etag = "etag"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "name"
        }
      )
  }

  @Test
  fun `addPolicyBindingMembers throws INVALID_FIELD_VALUE when name is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.addPolicyBindingMembers(
          addPolicyBindingMembersRequest {
            name = "policy-1"
            role = "roles/bookReader"
            members += "principals/user-2"
            etag = "etag"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "name"
        }
      )
  }

  @Test
  fun `addPolicyBindingMembers throws REQUIRED_FIELD_NOT_SET when role is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.addPolicyBindingMembers(
          addPolicyBindingMembersRequest {
            name = "policies/policy-1"
            members += "principals/user-2"
            etag = "etag"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "role"
        }
      )
  }

  @Test
  fun `addPolicyBindingMembers throws INVALID_FIELD_VALUE when role is malformed`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.addPolicyBindingMembers(
          addPolicyBindingMembersRequest {
            name = "policies/policy-1"
            role = "bookReader"
            members += "principals/user-2"
            etag = "etag"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "role"
        }
      )
  }

  @Test
  fun `addPolicyBindingMembers throws INVALID_FIELD_VALUE when members is not set`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.addPolicyBindingMembers(
          addPolicyBindingMembersRequest {
            name = "policies/policy-1"
            role = "roles/bookReader"
            etag = "etag"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "members"
        }
      )
  }

  @Test
  fun `addPolicyBindingMembers throws INVALID_FIELD_VALUE when members is malformed`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.addPolicyBindingMembers(
            addPolicyBindingMembersRequest {
              name = "policies/policy-1"
              role = "roles/bookReader"
              members += "user-2"
              etag = "etag"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "members"
          }
        )
    }

  @Test
  fun `addPolicyBindingMembers throws POLICY_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { addPolicyBindingMembers(any()) } doThrow
        PolicyNotFoundException("policy-1").asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    val request = addPolicyBindingMembersRequest {
      name = "policies/policy-1"
      role = "roles/bookReader"
      members += "principals/user-2"
      etag = "etag"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.addPolicyBindingMembers(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.POLICY_NOT_FOUND.name
          metadata[Errors.Metadata.POLICY.key] = request.name
        }
      )
  }

  @Test
  fun `addPolicyBindingMembers throws ETAG_MISMTACH from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { addPolicyBindingMembers(any()) } doThrow
        EtagMismatchException("wrong-etag", "right-etag")
          .asStatusRuntimeException(Status.Code.ABORTED)
    }

    val request = addPolicyBindingMembersRequest {
      name = "policies/policy-1"
      role = "roles/bookReader"
      members += "principals/user-2"
      etag = "wrong-etag"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.addPolicyBindingMembers(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.ETAG_MISMATCH.name
          metadata[Errors.Metadata.REQUEST_ETAG.key] = request.etag
          metadata[Errors.Metadata.ETAG.key] = "right-etag"
        }
      )
  }

  @Test
  fun `addPolicyBindingMembers throws ROLE_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { addPolicyBindingMembers(any()) } doThrow
        RoleNotFoundException("bookReader")
          .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }

    val request = addPolicyBindingMembersRequest {
      name = "policies/policy-1"
      role = "roles/bookReader"
      members += "principals/user-2"
      etag = "etag"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.addPolicyBindingMembers(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.ROLE_NOT_FOUND.name
          metadata[Errors.Metadata.ROLE.key] = request.role
        }
      )
  }

  @Test
  fun `addPolicyBindingMembers throws PRINCIPAL_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { addPolicyBindingMembers(any()) } doThrow
        PrincipalNotFoundException("user-1")
          .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }

    val request = addPolicyBindingMembersRequest {
      name = "policies/policy-1"
      role = "roles/bookReader"
      members += "principals/user-1"
      etag = "etag"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.addPolicyBindingMembers(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.PRINCIPAL_NOT_FOUND.name
          metadata[Errors.Metadata.PRINCIPAL.key] = request.membersList.single()
        }
      )
  }

  @Test
  fun `addPolicyBindingMembers throws PRINCIPAL_TYPE_NOT_SUPPORTED from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { addPolicyBindingMembers(any()) } doThrow
        PrincipalTypeNotSupportedException("user-1", Principal.IdentityCase.TLS_CLIENT)
          .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }

    val request = addPolicyBindingMembersRequest {
      name = "policies/policy-1"
      role = "roles/bookReader"
      members += "principals/user-1"
      etag = "etag"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.addPolicyBindingMembers(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED.name
          metadata[Errors.Metadata.PRINCIPAL_TYPE.key] = Principal.IdentityCase.TLS_CLIENT.name
        }
      )
  }

  @Test
  fun `addPolicyBindingMembers throws POLICY_BINDING_MEMBERSHIP_ALREADY_EXISTS from backend`() =
    runBlocking {
      internalServiceMock.stub {
        onBlocking { addPolicyBindingMembers(any()) } doThrow
          PolicyBindingMembershipAlreadyExistsException("policy-1", "bookReader", "user-1")
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
      }

      val request = addPolicyBindingMembersRequest {
        name = "policies/policy-1"
        role = "roles/bookReader"
        members += "principals/user-1"
        etag = "etag"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> { service.addPolicyBindingMembers(request) }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.POLICY_BINDING_MEMBERSHIP_ALREADY_EXISTS.name
            metadata[Errors.Metadata.POLICY.key] = request.name
            metadata[Errors.Metadata.ROLE.key] = request.role
            metadata[Errors.Metadata.PRINCIPAL.key] = request.membersList.single()
          }
        )
    }

  @Test
  fun `removePolicyBindingMembers returns policy`() = runBlocking {
    val internalPolicy = internalPolicy {
      policyResourceId = "policy-1"
      protectedResourceName = "books"
      bindings.put(
        "bookReader",
        InternalPolicyKt.members { memberPrincipalResourceIds += "user-1" },
      )
      etag = "etag"
    }
    internalServiceMock.stub {
      onBlocking { removePolicyBindingMembers(any()) } doReturn internalPolicy
    }

    val request = removePolicyBindingMembersRequest {
      name = "policies/${internalPolicy.policyResourceId}"
      role = "roles/bookReader"
      members += "principals/user-2"
      etag = internalPolicy.etag
    }
    val response = service.removePolicyBindingMembers(request)

    verifyProtoArgument(
        internalServiceMock,
        InternalPoliciesGrpcKt.PoliciesCoroutineImplBase::removePolicyBindingMembers,
      )
      .isEqualTo(
        internalRemovePolicyBindingMembersRequest {
          policyResourceId = internalPolicy.policyResourceId
          roleResourceId = "bookReader"
          memberPrincipalResourceIds += "user-2"
          etag = internalPolicy.etag
        }
      )

    assertThat(response)
      .isEqualTo(
        policy {
          name = "policies/${internalPolicy.policyResourceId}"
          protectedResource = internalPolicy.protectedResourceName
          bindings +=
            PolicyKt.binding {
              role = "roles/bookReader"
              members += "principals/user-1"
            }
          etag = request.etag
        }
      )
  }

  @Test
  fun `removePolicyBindingMembers throws REQUIRED_FIELD_NOT_SET when name is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.removePolicyBindingMembers(
            removePolicyBindingMembersRequest {
              role = "roles/bookReader"
              members += "principals/user-2"
              etag = "etag"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "name"
          }
        )
    }

  @Test
  fun `removePolicyBindingMembers throws INVALID_FIELD_VALUE when name is malformed`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.removePolicyBindingMembers(
            removePolicyBindingMembersRequest {
              name = "policy-1"
              role = "roles/bookReader"
              members += "principals/user-2"
              etag = "etag"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "name"
          }
        )
    }

  @Test
  fun `removePolicyBindingMembers throws REQUIRED_FIELD_NOT_SET when members is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.removePolicyBindingMembers(
            removePolicyBindingMembersRequest {
              name = "policies/policy-1"
              role = "roles/bookReader"
              etag = "etag"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "members"
          }
        )
    }

  @Test
  fun `removePolicyBindingMembers throws REQUIRED_FIELD_NOT_SET when role is not set`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.removePolicyBindingMembers(
            removePolicyBindingMembersRequest {
              name = "policies/policy-1"
              members += "principals/user-2"
              etag = "etag"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "role"
          }
        )
    }

  @Test
  fun `removePolicyBindingMembers throws INVALID_FIELD_VALUE when role is malformed`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.removePolicyBindingMembers(
            removePolicyBindingMembersRequest {
              name = "policies/policy-1"
              role = "bookReader"
              members += "principals/user-2"
              etag = "etag"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "role"
          }
        )
    }

  @Test
  fun `removePolicyBindingMembers throws INVALID_FIELD_VALUE when members is malformed`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.removePolicyBindingMembers(
            removePolicyBindingMembersRequest {
              name = "policies/policy-1"
              role = "roles/bookReader"
              members += "user-2"
              etag = "etag"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "members"
          }
        )
    }

  @Test
  fun `removePolicyBindingMembers throws POLICY_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { removePolicyBindingMembers(any()) } doThrow
        PolicyNotFoundException("policy-1").asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    val request = removePolicyBindingMembersRequest {
      name = "policies/policy-1"
      role = "roles/bookReader"
      members += "principals/user-2"
      etag = "etag"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.removePolicyBindingMembers(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.POLICY_NOT_FOUND.name
          metadata[Errors.Metadata.POLICY.key] = request.name
        }
      )
  }

  @Test
  fun `removePolicyBindingMembers throws ETAG_MISMTACH from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { removePolicyBindingMembers(any()) } doThrow
        EtagMismatchException("wrong-etag", "right-etag")
          .asStatusRuntimeException(Status.Code.ABORTED)
    }

    val request = removePolicyBindingMembersRequest {
      name = "policies/policy-1"
      role = "roles/bookReader"
      members += "principals/user-2"
      etag = "wrong-etag"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.removePolicyBindingMembers(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.ETAG_MISMATCH.name
          metadata[Errors.Metadata.REQUEST_ETAG.key] = request.etag
          metadata[Errors.Metadata.ETAG.key] = "right-etag"
        }
      )
  }

  @Test
  fun `removePolicyBindingMembers throws ROLE_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { removePolicyBindingMembers(any()) } doThrow
        RoleNotFoundException("bookReader").asStatusRuntimeException(Status.Code.INTERNAL)
    }

    val request = removePolicyBindingMembersRequest {
      name = "policies/policy-1"
      role = "roles/bookReader"
      members += "principals/user-2"
      etag = "etag"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.removePolicyBindingMembers(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INTERNAL)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.ROLE_NOT_FOUND.name
          metadata[Errors.Metadata.ROLE.key] = request.role
        }
      )
  }

  @Test
  fun `removePolicyBindingMembers throws PRINCIPAL_NOT_FOUND from backend`() = runBlocking {
    internalServiceMock.stub {
      onBlocking { removePolicyBindingMembers(any()) } doThrow
        PrincipalNotFoundException("user-1").asStatusRuntimeException(Status.Code.INTERNAL)
    }

    val request = removePolicyBindingMembersRequest {
      name = "policies/policy-1"
      role = "roles/bookReader"
      members += "principals/user-1"
      etag = "etag"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { service.removePolicyBindingMembers(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INTERNAL)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.PRINCIPAL_NOT_FOUND.name
          metadata[Errors.Metadata.PRINCIPAL.key] = request.membersList.single()
        }
      )
  }

  @Test
  fun `removePolicyBindingMembers throws PRINCIPAL_TYPE_NOT_SUPPORTED from backend`() =
    runBlocking {
      internalServiceMock.stub {
        onBlocking { removePolicyBindingMembers(any()) } doThrow
          PrincipalTypeNotSupportedException("user-1", Principal.IdentityCase.TLS_CLIENT)
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
      }

      val request = removePolicyBindingMembersRequest {
        name = "policies/policy-1"
        role = "roles/bookReader"
        members += "principals/user-1"
        etag = "etag"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> { service.removePolicyBindingMembers(request) }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.PRINCIPAL_TYPE_NOT_SUPPORTED.name
            metadata[Errors.Metadata.PRINCIPAL_TYPE.key] = Principal.IdentityCase.TLS_CLIENT.name
          }
        )
    }

  @Test
  fun `removePolicyBindingMembers throws POLICY_BINDING_MEMBERSHIP_NOT_FOUND from backend`() =
    runBlocking {
      internalServiceMock.stub {
        onBlocking { removePolicyBindingMembers(any()) } doThrow
          PolicyBindingMembershipNotFoundException("policy-1", "bookReader", "user-1")
            .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
      }

      val request = removePolicyBindingMembersRequest {
        name = "policies/policy-1"
        role = "roles/bookReader"
        members += "principals/user-1"
        etag = "etag"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> { service.removePolicyBindingMembers(request) }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.POLICY_BINDING_MEMBERSHIP_NOT_FOUND.name
            metadata[Errors.Metadata.POLICY.key] = request.name
            metadata[Errors.Metadata.ROLE.key] = request.role
            metadata[Errors.Metadata.PRINCIPAL.key] = request.membersList.single()
          }
        )
    }
}
