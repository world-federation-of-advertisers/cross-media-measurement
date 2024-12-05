package org.wfanet.measurement.access.service.v1alpha

import org.wfanet.measurement.internal.access.Principal as InternalPrincipal
import org.wfanet.measurement.internal.access.Role as InternalRole
import org.wfanet.measurement.access.service.PermissionKey
import org.wfanet.measurement.access.service.PrincipalKey
import org.wfanet.measurement.access.service.RoleKey
import org.wfanet.measurement.access.v1alpha.Principal
import org.wfanet.measurement.access.v1alpha.PrincipalKt.oAuthUser
import org.wfanet.measurement.access.v1alpha.PrincipalKt.tlsClient
import org.wfanet.measurement.access.v1alpha.Role
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.access.v1alpha.role

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

fun InternalRole.toRole(): Role {
  val source = this
  return role {
    name = RoleKey(source.roleResourceId).toName()
    resourceTypes += source.resourceTypesList
    permissions += source.permissionResourceIdsList.map { PermissionKey(it).toName()}
    etag = source.etag
  }
}
