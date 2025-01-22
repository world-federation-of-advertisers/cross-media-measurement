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

import com.google.protobuf.kotlin.toByteStringUtf8
import io.grpc.ManagedChannel
import java.time.Duration
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.access.v1alpha.PrincipalKt
import org.wfanet.measurement.access.v1alpha.PrincipalsGrpcKt.PrincipalsCoroutineStub
import org.wfanet.measurement.access.v1alpha.createPrincipalRequest
import org.wfanet.measurement.access.v1alpha.deletePrincipalRequest
import org.wfanet.measurement.access.v1alpha.getPrincipalRequest
import org.wfanet.measurement.access.v1alpha.lookupPrincipalRequest
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.common.commandLineMain
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
  subcommands = [CommandLine.HelpCommand::class, Principals::class],
)
class Access private constructor() : Runnable {
  @Spec private lateinit var commandSpec: CommandSpec

  val commandLine: CommandLine
    get() = commandSpec.commandLine()

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

class PrincipalIdentity {
  @ArgGroup(exclusive = false, multiplicity = "1", heading = "OAuth user")
  var user: OAuthUserFlags? = null

  class OAuthUserFlags {
    @Option(names = ["--issuer"], description = ["OAuth issuer identifier"], required = true)
    lateinit var issuer: String

    @Option(names = ["--subject"], description = ["OAuth subject identifier"])
    lateinit var subject: String
  }

  @ArgGroup(exclusive = false, multiplicity = "1", heading = "tls client")
  var tlsClient: TlsClientFlags? = null

  class TlsClientFlags {
    @Option(
      names = ["--authority-key-identifier"],
      description = ["Tls client authority key identifier (AKID)"],
      required = true,
    )
    lateinit var authorityKeyIdentifier: String
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
  @ParentCommand
  lateinit var parentCommand: Access
    private set

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
    val principal =
      runBlocking(Dispatchers.IO) {
        parentCommand.principalsClient.getPrincipal(getPrincipalRequest { name = principalName })
      }
    println(principal)
  }
}

@Command(name = "create", description = ["Create a principal"])
class CreatePrincipal : Runnable {
  @ParentCommand private lateinit var parentCommand: Principals

  @ArgGroup(exclusive = false, heading = "Principal specification")
  lateinit var principalFlags: PrincipalFlags

  class PrincipalFlags {
    @Option(names = ["--name"], description = ["API resource name of the Principal"])
    lateinit var name: String

    @ArgGroup(exclusive = true, multiplicity = "1", heading = "Principal identity specification")
    lateinit var identityInput: PrincipalIdentity
  }

  @Option(
    names = ["--principal-id"],
    description = ["Resource ID of the Principal"],
    required = true,
  )
  lateinit var id: String

  override fun run() {
    val principal =
      runBlocking(Dispatchers.IO) {
        parentCommand.principalsClient.createPrincipal(
          createPrincipalRequest {
            principal = principal {
              name = principalFlags.name
              if (principalFlags.identityInput.user != null) {
                user =
                  PrincipalKt.oAuthUser {
                    issuer = principalFlags.identityInput.user!!.issuer
                    subject = principalFlags.identityInput.user!!.subject
                  }
              } else {
                tlsClient =
                  PrincipalKt.tlsClient {
                    authorityKeyIdentifier =
                      principalFlags.identityInput.tlsClient!!
                        .authorityKeyIdentifier
                        .toByteStringUtf8()
                  }
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
  lateinit var principalName: String

  override fun run() {
    runBlocking(Dispatchers.IO) {
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
  lateinit var lookupKey: PrincipalIdentity

  override fun run() {
    val principal =
      runBlocking(Dispatchers.IO) {
        parentCommand.principalsClient.lookupPrincipal(
          lookupPrincipalRequest {
            if (lookupKey.user != null) {
              user =
                PrincipalKt.oAuthUser {
                  issuer = lookupKey.user!!.issuer
                  subject = lookupKey.user!!.subject
                }
            } else {
              tlsClient =
                PrincipalKt.tlsClient {
                  authorityKeyIdentifier =
                    lookupKey.tlsClient!!.authorityKeyIdentifier.toByteStringUtf8()
                }
            }
          }
        )
      }
    println(principal)
  }
}
