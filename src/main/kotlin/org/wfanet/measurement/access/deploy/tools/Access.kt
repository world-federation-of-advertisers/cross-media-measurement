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

import io.grpc.ManagedChannel
import java.io.File
import java.time.Duration
import kotlin.properties.Delegates
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.access.v1alpha.CheckPermissionsResponse
import org.wfanet.measurement.access.v1alpha.ListPermissionsResponse
import org.wfanet.measurement.access.v1alpha.ListRolesResponse
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt.PermissionsCoroutineStub
import org.wfanet.measurement.access.v1alpha.PoliciesGrpcKt.PoliciesCoroutineStub
import org.wfanet.measurement.access.v1alpha.PolicyKt.binding
import org.wfanet.measurement.access.v1alpha.PrincipalKt
import org.wfanet.measurement.access.v1alpha.PrincipalsGrpcKt.PrincipalsCoroutineStub
import org.wfanet.measurement.access.v1alpha.RolesGrpcKt.RolesCoroutineStub
import org.wfanet.measurement.access.v1alpha.addPolicyBindingMembersRequest
import org.wfanet.measurement.access.v1alpha.checkPermissionsRequest
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
import org.wfanet.measurement.access.v1alpha.listRolesRequest
import org.wfanet.measurement.access.v1alpha.lookupPolicyRequest
import org.wfanet.measurement.access.v1alpha.lookupPrincipalRequest
import org.wfanet.measurement.access.v1alpha.policy
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.access.v1alpha.removePolicyBindingMembersRequest
import org.wfanet.measurement.access.v1alpha.role
import org.wfanet.measurement.access.v1alpha.updateRoleRequest
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import picocli.CommandLine
import picocli.CommandLine.ArgGroup
import picocli.CommandLine.Command
import picocli.CommandLine.Mixin
import picocli.CommandLine.Model.CommandSpec
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters
import picocli.CommandLine.ParentCommand
import picocli.CommandLine.Spec

private val CHANNEL_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30)

@Command(
  name = "Access",
  description = ["Interacts with Cross-Media Access API"],
  subcommands =
    [
      CommandLine.HelpCommand::class,
      Permissions::class,
      Principals::class,
      Policies::class,
      Roles::class,
    ],
)
class Access private constructor() : Runnable {
  @Mixin private lateinit var tlsFlags: TlsFlags

  @Option(
    names = ["--access-public-api-target"],
    description = ["gRPC target (authority) of the Access API server"],
    required = true,
  )
  private lateinit var target: String

  @Option(
    names = ["--access-public-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Access API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --access-public-api-target.",
      ],
    required = false,
  )
  private var certHost: String? = null

  val accessChannel: ManagedChannel by lazy {
    buildMutualTlsChannel(target, tlsFlags.signingCerts, certHost)
      .withShutdownTimeout(CHANNEL_SHUTDOWN_TIMEOUT)
  }

  override fun run() {
    // No-op. See subcommands.
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(Access(), args)
  }
}

private class PrincipalIdentity {
  @ArgGroup(exclusive = false, multiplicity = "1", heading = "OAuth user")
  var user: OAuthUserFlags? = null
    private set

  class OAuthUserFlags {
    @Option(names = ["--issuer"], description = ["OAuth issuer identifier"], required = true)
    lateinit var issuer: String
      private set

    @Option(names = ["--subject"], description = ["OAuth subject identifier"], required = true)
    lateinit var subject: String
      private set
  }

  @ArgGroup(exclusive = false, multiplicity = "1", heading = "tls client")
  var tlsClient: TlsClientFlags? = null
    private set

  class TlsClientFlags {
    @Option(
      names = ["--principal-tls-client-cert-file"],
      description = ["Path to TLS client certificate belonging to Principal"],
      defaultValue = "",
      required = true,
    )
    lateinit var tlsCertFile: File
      private set
  }
}

@Command(
  name = "principals",
  subcommands =
    [
      CommandLine.HelpCommand::class,
      GetPrincipal::class,
      CreatePrincipal::class,
      DeletePrincipal::class,
      LookupPrincipal::class,
    ],
)
private class Principals {
  @ParentCommand private lateinit var parentCommand: Access

  val principalsClient: PrincipalsCoroutineStub by lazy {
    PrincipalsCoroutineStub(parentCommand.accessChannel)
  }
}

@Command(name = "get", description = ["Get a Principal"])
class GetPrincipal : Runnable {
  @ParentCommand private lateinit var parentCommand: Principals

  @Parameters(index = "0", description = ["API resource name of the Principal"], arity = "1")
  private lateinit var principalName: String

  override fun run() {
    val principal = runBlocking {
      parentCommand.principalsClient.getPrincipal(getPrincipalRequest { name = principalName })
    }
    println(principal)
  }
}

@Command(name = "create", description = ["Create a principal"])
class CreatePrincipal : Runnable {
  @ParentCommand private lateinit var parentCommand: Principals

  @ArgGroup(exclusive = true, multiplicity = "1", heading = "Principal identity specification")
  private lateinit var principalIdentity: PrincipalIdentity

  @Option(
    names = ["--principal-id"],
    description = ["Resource ID of the Principal"],
    required = true,
  )
  private lateinit var id: String

  override fun run() {
    val principal = runBlocking {
      parentCommand.principalsClient.createPrincipal(
        createPrincipalRequest {
          principal = principal {
            if (principalIdentity.user != null) {
              user =
                PrincipalKt.oAuthUser {
                  issuer = principalIdentity.user!!.issuer
                  subject = principalIdentity.user!!.subject
                }
            } else {
              val certificate = readCertificate(principalIdentity.tlsClient!!.tlsCertFile)
              val akid = checkNotNull(certificate.authorityKeyIdentifier)
              tlsClient = PrincipalKt.tlsClient { authorityKeyIdentifier = akid }
            }
          }
          this.principalId = id
        }
      )
    }
    println(principal)
  }
}

@Command(name = "delete", description = ["Delete a principal"])
class DeletePrincipal : Runnable {
  @ParentCommand private lateinit var parentCommand: Principals

  @Parameters(index = "0", description = ["API resource name of the Principal"], arity = "1")
  private lateinit var principalName: String

  override fun run() {
    runBlocking {
      parentCommand.principalsClient.deletePrincipal(
        deletePrincipalRequest { this.name = principalName }
      )
    }
  }
}

@Command(name = "lookup", description = ["Lookup a principal"])
class LookupPrincipal : Runnable {
  @ParentCommand private lateinit var parentCommand: Principals

  @ArgGroup(exclusive = true, multiplicity = "1", heading = "Principal lookup key")
  private lateinit var lookupKey: PrincipalIdentity

  override fun run() {
    val principal = runBlocking {
      parentCommand.principalsClient.lookupPrincipal(
        lookupPrincipalRequest {
          if (lookupKey.user != null) {
            user =
              PrincipalKt.oAuthUser {
                issuer = lookupKey.user!!.issuer
                subject = lookupKey.user!!.subject
              }
          } else {
            val certificate = readCertificate(lookupKey.tlsClient!!.tlsCertFile)
            val akid = checkNotNull(certificate.authorityKeyIdentifier)
            tlsClient = PrincipalKt.tlsClient { authorityKeyIdentifier = akid }
          }
        }
      )
    }
    println(principal)
  }
}

@Command(
  name = "roles",
  subcommands =
    [
      CommandLine.HelpCommand::class,
      GetRole::class,
      ListRoles::class,
      CreateRole::class,
      UpdateRole::class,
      DeleteRole::class,
    ],
)
private class Roles {
  @ParentCommand private lateinit var parentCommand: Access

  val rolesClient: RolesCoroutineStub by lazy { RolesCoroutineStub(parentCommand.accessChannel) }
}

@Command(name = "get", description = ["Get a Role"])
class GetRole : Runnable {
  @ParentCommand private lateinit var parentCommand: Roles

  @Parameters(index = "0", description = ["API resource name of the Role"], arity = "1")
  private lateinit var roleName: String

  override fun run() {
    val role = runBlocking { parentCommand.rolesClient.getRole(getRoleRequest { name = roleName }) }
    println(role)
  }
}

@Command(name = "list", description = ["List Roles"])
class ListRoles : Runnable {
  @ParentCommand private lateinit var parentCommand: Roles

  @set:Option(
    names = ["--page-size"],
    description = ["The maximum number of Roles to return"],
    defaultValue = "1000",
    required = false,
  )
  private var listPageSize: Int by Delegates.notNull()

  @Option(
    names = ["--page-token"],
    description =
      [
        "A page token, received from a previous `ListRoles` call. Provide this to retrieve the subsequent page."
      ],
    defaultValue = "",
    required = false,
  )
  private lateinit var listPageToken: String

  override fun run() {
    val response: ListRolesResponse = runBlocking {
      parentCommand.rolesClient.listRoles(
        listRolesRequest {
          pageSize = listPageSize
          pageToken = listPageToken
        }
      )
    }
    println(response)
  }
}

@Command(name = "create", description = ["Create a Role"])
class CreateRole : Runnable {
  @ParentCommand private lateinit var parentCommand: Roles

  @Option(
    names = ["--resource-type"],
    description =
      ["Resource type that this Role can be granted on. Can be specified multiple times."],
    required = true,
  )
  private lateinit var resourceTypeList: List<String>

  @Option(
    names = ["--permission"],
    description =
      ["Resource name of permission granted by this Role. Can be specified multiple times."],
    required = true,
  )
  private lateinit var permissionList: List<String>

  @Option(names = ["--role-id"], description = ["Resource ID of the Role"], required = true)
  private lateinit var id: String

  override fun run() {
    val role = runBlocking {
      parentCommand.rolesClient.createRole(
        createRoleRequest {
          role = role {
            resourceTypes += resourceTypeList
            permissions += permissionList
          }
          roleId = id
        }
      )
    }

    println(role)
  }
}

@Command(name = "update", description = ["Update a Role"])
class UpdateRole : Runnable {
  @ParentCommand private lateinit var parentCommand: Roles

  @Option(names = ["--name"], description = ["API resource name of the Role"], required = true)
  private lateinit var roleName: String

  @Option(
    names = ["--resource-type"],
    description =
      ["Resource type that that this Role can be granted on. Can be specified multiple times."],
    required = true,
  )
  private lateinit var resourceTypeList: List<String>

  @Option(
    names = ["--permission"],
    description =
      ["Resource name of permission granted by this Role. Can be specified multiple times."],
    required = true,
  )
  private lateinit var permissionList: List<String>

  @Option(names = ["--etag"], description = ["Entity tag of the Role"], required = true)
  private lateinit var roleEtag: String

  override fun run() {
    val role = runBlocking {
      parentCommand.rolesClient.updateRole(
        updateRoleRequest {
          role = role {
            name = roleName
            resourceTypes += resourceTypeList
            permissions += permissionList
            etag = roleEtag
          }
        }
      )
    }

    println(role)
  }
}

@Command(name = "delete", description = ["Delete a Role"])
class DeleteRole : Runnable {
  @ParentCommand private lateinit var parentCommand: Roles

  @Parameters(index = "0", description = ["API resource name of the Role"], arity = "1")
  private lateinit var roleName: String

  override fun run() {
    runBlocking { parentCommand.rolesClient.deleteRole(deleteRoleRequest { name = roleName }) }
  }
}

@Command(
  name = "permissions",
  subcommands =
    [
      CommandLine.HelpCommand::class,
      GetPermission::class,
      ListPermissions::class,
      CheckPermissions::class,
    ],
)
private class Permissions {
  @ParentCommand private lateinit var parentCommand: Access

  val permissionsClient: PermissionsCoroutineStub by lazy {
    PermissionsCoroutineStub(parentCommand.accessChannel)
  }
}

@Command(name = "get", description = ["Get a Permission"])
class GetPermission : Runnable {
  @ParentCommand private lateinit var parentCommand: Permissions

  @Parameters(index = "0", description = ["API resource name of the Permission"], arity = "1")
  private lateinit var permissionName: String

  override fun run() {
    val permission = runBlocking {
      parentCommand.permissionsClient.getPermission(getPermissionRequest { name = permissionName })
    }
    println(permission)
  }
}

@Command(name = "list", description = ["List Permissions"])
class ListPermissions : Runnable {
  @ParentCommand private lateinit var parentCommand: Permissions

  @set:Option(
    names = ["--page-size"],
    description = ["The maximum number of Permissions to return"],
    defaultValue = "1000",
    required = false,
  )
  private var listPageSize: Int by Delegates.notNull()

  @Option(
    names = ["--page-token"],
    description =
      [
        "A page token, received from a previous `ListPermissions` call. Provide this to retrieve the subsequent page."
      ],
    defaultValue = "",
    required = false,
  )
  private lateinit var listPageToken: String

  override fun run() {
    val response: ListPermissionsResponse = runBlocking {
      parentCommand.permissionsClient.listPermissions(
        listPermissionsRequest {
          pageSize = listPageSize
          pageToken = listPageToken
        }
      )
    }
    println(response)
  }
}

@Command(name = "check", description = ["Check Permissions"])
class CheckPermissions : Runnable {
  @ParentCommand private lateinit var parentCommand: Permissions

  @Option(
    names = ["--protected-resource"],
    description =
      [
        "Name of resource on which to check permissions. " +
          "If not specified, this means the root of the protected API."
      ],
    defaultValue = "",
    required = false,
  )
  private lateinit var protectedResourceName: String

  @Option(
    names = ["--principal"],
    description = ["Resource name of the Principal"],
    required = true,
  )
  private lateinit var principalName: String

  @Option(
    names = ["--permission"],
    description = ["Resource name of permission to check. Can be specified multiple times."],
    required = true,
  )
  private lateinit var permissionList: List<String>

  override fun run() {
    val response: CheckPermissionsResponse = runBlocking {
      parentCommand.permissionsClient.checkPermissions(
        checkPermissionsRequest {
          protectedResource = protectedResourceName
          principal = principalName
          permissions += permissionList
        }
      )
    }

    println(response)
  }
}

@Command(
  name = "policies",
  subcommands =
    [
      CommandLine.HelpCommand::class,
      GetPolicy::class,
      CreatePolicy::class,
      LookupPolicy::class,
      AddPolicyBindingMembers::class,
      RemovePolicyBindingMembers::class,
    ],
)
private class Policies {
  @Spec private lateinit var commandSpec: CommandSpec

  val commandLine: CommandLine
    get() = commandSpec.commandLine()

  @ParentCommand private lateinit var parentCommand: Access

  val policiesClient: PoliciesCoroutineStub by lazy {
    PoliciesCoroutineStub(parentCommand.accessChannel)
  }
}

private class PolicyBinding {
  @Option(names = ["--binding-role"], description = ["Resource name of the Role"], required = true)
  lateinit var role: String
    private set

  @Option(
    names = ["--binding-member"],
    description =
      [
        "Resource name of the principal which is a member of the this Role on `resource`" +
          "Can be specified multiple times."
      ],
    required = true,
  )
  lateinit var members: List<String>
    private set
}

@Command(name = "get", description = ["Get a Policy"])
class GetPolicy : Runnable {
  @ParentCommand private lateinit var parentCommand: Policies

  @Parameters(index = "0", description = ["Resource name of the Policy"], arity = "1")
  private lateinit var policyName: String

  override fun run() {
    val policy = runBlocking {
      parentCommand.policiesClient.getPolicy(getPolicyRequest { name = policyName })
    }
    println(policy)
  }
}

@Command(name = "create", description = ["Create a Policy"])
class CreatePolicy : Runnable {
  @ParentCommand private lateinit var parentCommand: Policies

  @Option(
    names = ["--protected-resource"],
    description =
      [
        "Name of the resource protected by this Policy. " +
          "If not specified, this means the root of the protected API."
      ],
    required = false,
  )
  private lateinit var resource: String

  @ArgGroup(
    exclusive = false,
    multiplicity = "0..*",
    heading =
      "Policy Bindings that map Role to members (Principals). " +
        "Optional and can be specified multiple times.",
  )
  private lateinit var policyBindings: List<PolicyBinding>

  @Option(names = ["--policy-id"], description = ["Resource ID of the Policy"], required = false)
  private lateinit var id: String

  override fun run() {
    val createPolicyRequest = createPolicyRequest {
      policy = policy {
        protectedResource = resource
        bindings +=
          policyBindings.map { binding ->
            binding {
              role = binding.role
              members += binding.members
            }
          }
      }
      policyId = id
    }

    val policy = runBlocking { parentCommand.policiesClient.createPolicy(createPolicyRequest) }
    println(policy)
  }
}

@Command(name = "lookup", description = ["Lookup a Policy by lookup key"])
class LookupPolicy : Runnable {
  @ParentCommand private lateinit var parentCommand: Policies

  @Option(
    names = ["--protected-resource"],
    description = ["Name of the resource to which the policy applies"],
    required = true,
  )
  private lateinit var resource: String

  override fun run() {
    val policy = runBlocking {
      parentCommand.policiesClient.lookupPolicy(
        lookupPolicyRequest { protectedResource = resource }
      )
    }

    println(policy)
  }
}

private class PolicyBindingChangeFlags {
  @Option(names = ["--name"], description = ["Resource name of the Policy"], required = true)
  lateinit var policyName: String
    private set

  @ArgGroup(exclusive = false, multiplicity = "1", heading = "Policy Bindings to add/remove")
  lateinit var policyBindings: PolicyBinding

  @Option(names = ["--etag"], description = ["Current etag of the resource"], required = false)
  lateinit var currentEtag: String
    private set
}

@Command(name = "add-members", description = ["Add members to a Policy Binding"])
class AddPolicyBindingMembers : Runnable {
  @ParentCommand private lateinit var parentCommand: Policies

  @ArgGroup(exclusive = false, multiplicity = "1", heading = "Policy Binding addition flags")
  private lateinit var policyBindingChangeFlags: PolicyBindingChangeFlags

  override fun run() {
    val policy = runBlocking {
      parentCommand.policiesClient.addPolicyBindingMembers(
        addPolicyBindingMembersRequest {
          name = policyBindingChangeFlags.policyName
          role = policyBindingChangeFlags.policyBindings.role
          members += policyBindingChangeFlags.policyBindings.members
          etag = policyBindingChangeFlags.currentEtag
        }
      )
    }

    println(policy)
  }
}

@Command(name = "remove-members", description = ["Remove members from a Policy Binding"])
class RemovePolicyBindingMembers : Runnable {
  @ParentCommand private lateinit var parentCommand: Policies

  @ArgGroup(exclusive = false, multiplicity = "1", heading = "Policy Binding removal flags")
  private lateinit var policyBindingChangeFlags: PolicyBindingChangeFlags

  override fun run() {
    val policy = runBlocking {
      parentCommand.policiesClient.removePolicyBindingMembers(
        removePolicyBindingMembersRequest {
          name = policyBindingChangeFlags.policyName
          role = policyBindingChangeFlags.policyBindings.role
          members += policyBindingChangeFlags.policyBindings.members
          etag = policyBindingChangeFlags.currentEtag
        }
      )
    }
    println(policy)
  }
}
