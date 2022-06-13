/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.api.v2alpha.tools

import com.google.crypto.tink.BinaryKeysetReader
import com.google.crypto.tink.CleartextKeysetHandle
import io.grpc.ManagedChannel
import java.io.File
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.Account
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineStub
import org.wfanet.measurement.api.v2alpha.activateAccountRequest
import org.wfanet.measurement.api.v2alpha.authenticateRequest
import org.wfanet.measurement.api.withIdToken
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.tink.PrivateJwkHandle
import org.wfanet.measurement.common.crypto.tink.SelfIssuedIdTokens
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters
import picocli.CommandLine.ParentCommand

private val CHANNEL_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30)

@Command(
  name = "MeasurementSystem",
  description = ["Interacts with the Cross-Media Measurement System API"],
  subcommands =
    [
      CommandLine.HelpCommand::class,
      Accounts::class,
    ]
)
class MeasurementSystem private constructor() : Runnable {
  @CommandLine.Mixin private lateinit var tlsFlags: TlsFlags

  @Option(
    names = ["--kingdom-public-api-target"],
    description = ["gRPC target (authority) of the Kingdom public API server"],
    required = true,
  )
  private lateinit var target: String

  @Option(
    names = ["--kingdom-public-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Kingdom public API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --kingdom-public-api-target.",
      ],
    required = false,
  )
  private var certHost: String? = null

  val kingdomChannel: ManagedChannel by lazy {
    buildMutualTlsChannel(
        target,
        SigningCerts.fromPemFiles(
          tlsFlags.certFile,
          tlsFlags.privateKeyFile,
          tlsFlags.certCollectionFile
        ),
        certHost
      )
      .withShutdownTimeout(CHANNEL_SHUTDOWN_TIMEOUT)
  }

  val rpcDispatcher: CoroutineDispatcher = Dispatchers.IO

  override fun run() {
    // No-op. See subcommands.
  }

  companion object {
    /**
     * Issuer for Self-issued OpenID Provider (SIOP).
     *
     * TODO(@SanjayVas): Use this from [SelfIssuedIdTokens] once it's exposed.
     */
    const val SELF_ISSUED_ISSUER = "https://self-issued.me"

    @JvmStatic fun main(args: Array<String>) = commandLineMain(MeasurementSystem(), args)
  }
}

@Command(
  name = "accounts",
)
private class Accounts {
  @ParentCommand
  lateinit var parentCommand: MeasurementSystem
    private set

  private val accountsClient: AccountsCoroutineStub by lazy {
    AccountsCoroutineStub(parentCommand.kingdomChannel)
  }

  @Command
  fun authenticate(
    @Option(
      names = ["--self-issued-openid-provider-key", "--siop-key"],
      description = ["Self-issued OpenID Provider key as a binary Tink keyset"]
    )
    siopKey: File
  ) {
    val response =
      runBlocking(parentCommand.rpcDispatcher) {
        accountsClient.authenticate(
          authenticateRequest { issuer = MeasurementSystem.SELF_ISSUED_ISSUER }
        )
      }

    // TODO(@SanjayVas): Use a util from common.crypto rather than directly interacting with Tink.
    val keysetHandle = CleartextKeysetHandle.read(BinaryKeysetReader.withFile(siopKey))
    val idToken: String =
      SelfIssuedIdTokens.generateIdToken(
        PrivateJwkHandle(keysetHandle),
        response.authenticationRequestUri,
        Clock.systemUTC()
      )

    println("ID Token: $idToken")
  }

  @Command
  fun activate(
    @Option(names = ["--id-token"]) idToken: String,
    @Parameters(index = "0", description = ["Resource name of the Account"]) name: String,
    @Option(names = ["--activation-token"]) activationToken: String,
  ) {
    val response: Account =
      runBlocking(parentCommand.rpcDispatcher) {
        accountsClient
          .withIdToken(idToken)
          .activateAccount(
            activateAccountRequest {
              this.name = name
              this.activationToken = activationToken
            }
          )
      }
    println(response)
  }
}
