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

package org.wfanet.measurement.access.service.v1alpha

import org.wfanet.measurement.access.service.PermissionKey
import org.wfanet.measurement.access.service.PolicyKey
import org.wfanet.measurement.access.service.PrincipalKey
import org.wfanet.measurement.access.service.RoleKey
import org.wfanet.measurement.access.v1alpha.Permission
import org.wfanet.measurement.access.v1alpha.Policy
import org.wfanet.measurement.access.v1alpha.PolicyKt.binding
import org.wfanet.measurement.access.v1alpha.Principal
import org.wfanet.measurement.access.v1alpha.PrincipalKt.oAuthUser
import org.wfanet.measurement.access.v1alpha.PrincipalKt.tlsClient
import org.wfanet.measurement.access.v1alpha.Role
import org.wfanet.measurement.access.v1alpha.permission
import org.wfanet.measurement.access.v1alpha.policy
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.access.v1alpha.role
import org.wfanet.measurement.internal.access.Permission as InternalPermission
import org.wfanet.measurement.internal.access.Policy as InternalPolicy
import org.wfanet.measurement.internal.access.Principal as InternalPrincipal
import org.wfanet.measurement.internal.access.PrincipalKt.oAuthUser as internalOAuthUser
import org.wfanet.measurement.internal.access.PrincipalKt.tlsClient as internalTlsClient
import org.wfanet.measurement.internal.access.Role as InternalRole

fun InternalPrincipal.toPrincipal(): Principal {
  val source = this
  return principal {
    name = PrincipalKey(source.principalResourceId).toName()
    if (source.hasUser()) {
      user = source.user.toOAuthUser()
    }
    if (source.hasTlsClient()) {
      tlsClient = source.tlsClient.toTlsClient()
    }
  }
}

fun InternalPrincipal.OAuthUser.toOAuthUser(): Principal.OAuthUser {
  val source = this
  return oAuthUser {
    issuer = source.issuer
    subject = source.subject
  }
}

fun InternalPrincipal.TlsClient.toTlsClient(): Principal.TlsClient {
  val source = this
  return tlsClient { authorityKeyIdentifier = source.authorityKeyIdentifier }
}

fun InternalPermission.toPermission(): Permission {
  val source = this
  return permission {
    name = PermissionKey(source.permissionResourceId).toName()
    resourceTypes += source.resourceTypesList
  }
}

fun InternalRole.toRole(): Role {
  val source = this
  return role {
    name = RoleKey(source.roleResourceId).toName()
    resourceTypes += source.resourceTypesList
    permissions += source.permissionResourceIdsList.map { PermissionKey(it).toName() }
    etag = source.etag
  }
}

fun Principal.OAuthUser.toInternal(): InternalPrincipal.OAuthUser {
  val source = this
  return internalOAuthUser {
    issuer = source.issuer
    subject = source.subject
  }
}

fun Principal.TlsClient.toInternal(): InternalPrincipal.TlsClient {
  val source = this
  return internalTlsClient { authorityKeyIdentifier = source.authorityKeyIdentifier }
}

fun InternalPolicy.toPolicy(): Policy {
  val source = this
  return policy {
    name = PolicyKey(source.policyResourceId).toName()
    protectedResource = source.protectedResourceName
    bindings +=
      source.bindingsMap.map { (role, members) ->
        binding {
          this.role = RoleKey(role).toName()
          this.members += members.memberPrincipalResourceIdsList.map { PrincipalKey(it).toName() }
        }
      }
    etag = source.etag
  }
}
