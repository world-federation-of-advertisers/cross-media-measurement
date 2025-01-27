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
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.access.v1alpha.ListRolesResponse
import org.wfanet.measurement.access.v1alpha.PrincipalKt
import org.wfanet.measurement.access.v1alpha.PrincipalsGrpcKt.PrincipalsCoroutineStub
import org.wfanet.measurement.access.v1alpha.RolesGrpcKt.RolesCoroutineStub
import org.wfanet.measurement.access.v1alpha.createPrincipalRequest
import org.wfanet.measurement.access.v1alpha.createRoleRequest
import org.wfanet.measurement.access.v1alpha.deletePrincipalRequest
import org.wfanet.measurement.access.v1alpha.getPrincipalRequest
import org.wfanet.measurement.access.v1alpha.getRoleRequest
import org.wfanet.measurement.access.v1alpha.listRolesRequest
import org.wfanet.measurement.access.v1alpha.lookupPrincipalRequest
import org.wfanet.measurement.access.v1alpha.principal
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
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters
import picocli.CommandLine.ParentCommand

private val CHANNEL_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30)

@Command(
  name = "Access",
  description = ["Interacts with Cross-Media Access API"],
  subcommands = [CommandLine.HelpCommand::class, Principals::class, Roles::class],
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

  @Parameters(index = "0", description = ["API resource name of the Principal"])
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

  @Option(names = ["--name"], description = ["API resource name of the Principal"])
  private lateinit var principalName: String

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
            name = principalName
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

  @Option(names = ["--name"], description = ["API resource name of the Principal"])
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
    ],
)
private class Roles {
  @ParentCommand private lateinit var parentCommand: Access

  val rolesClient: RolesCoroutineStub by lazy { RolesCoroutineStub(parentCommand.accessChannel) }
}

@Command(name = "get", description = ["Get a Role"])
class GetRole : Runnable {
  @ParentCommand private lateinit var parentCommand: Roles

  @Parameters(index = "0", description = ["API resource name of the Role"])
  private lateinit var roleName: String

  override fun run() {
    val role = runBlocking { parentCommand.rolesClient.getRole(getRoleRequest { name = roleName }) }
    println(role)
  }
}

@Command(name = "list", description = ["List Roles"])
class ListRoles : Runnable {
  @ParentCommand private lateinit var parentCommand: Roles

  @Option(
    names = ["--page-size"],
    description = ["The maximum number of Roles to return"],
    required = false,
  )
  private var listPageSize: Int = 1000

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

  @Option(names = ["--name"], description = ["API resource name of the Role"])
  private lateinit var roleName: String

  @Option(
    names = ["--resource-type"],
    description = ["Set of resource types that this Role can be granted on"],
    required = true,
  )
  private lateinit var resourceTypeList: List<String>

  @Option(
    names = ["--permission"],
    description = ["Set of resource names of permissions granted by this Role"],
    required = true,
  )
  private lateinit var permissionList: List<String>

  @Option(names = ["--etag"], description = ["Entity tag of the Role"])
  private lateinit var roleEtag: String

  @Option(names = ["--role-id"], description = ["Resource ID of the Role"], required = true)
  private lateinit var id: String

  override fun run() {
    val role = runBlocking {
      parentCommand.rolesClient.createRole(
        createRoleRequest {
          role = role {
            name = roleName
            resourceTypes += resourceTypeList
            permissions += permissionList
            etag = roleEtag
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
    description = ["Set of resource types that this Role can be granted on"],
    required = true,
  )
  private lateinit var resourceTypeList: List<String>

  @Option(
    names = ["--permission"],
    description = ["Set of resource names of permissions granted by this Role"],
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
