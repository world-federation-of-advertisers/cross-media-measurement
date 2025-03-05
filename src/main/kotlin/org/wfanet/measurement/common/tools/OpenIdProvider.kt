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

package org.wfanet.measurement.common.tools

import com.google.crypto.tink.InsecureSecretKeyAccess
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.TinkProtoKeysetFormat
import java.io.Console
import java.io.File
import java.time.Duration
import org.wfanet.measurement.common.commandLineMain
import picocli.CommandLine

@CommandLine.Command(name = "OpenIdProvider", subcommands = [CommandLine.HelpCommand::class])
class OpenIdProvider private constructor() : Runnable {
  @CommandLine.Option(names = ["--issuer"], required = true) private lateinit var issuer: String

  @CommandLine.Option(
    names = ["--keyset"],
    description = ["Path to Tink keyset in binary format"],
    required = true,
  )
  private lateinit var keysetFile: File

  private val provider by lazy {
    val keysetHandle: KeysetHandle =
      TinkProtoKeysetFormat.parseKeyset(keysetFile.readBytes(), InsecureSecretKeyAccess.get())
    org.wfanet.measurement.common.grpc.testing.OpenIdProvider(issuer, keysetHandle)
  }

  override fun run() {
    CommandLine.usage(this, System.err)
  }

  @CommandLine.Command(name = "get-jwks", description = ["Prints the JSON Web Key Set (JWKS)"])
  private fun getJwks() {
    print(provider.providerConfig.jwks)
    consolePrintln()
  }

  @CommandLine.Command(name = "generate-access-token", description = ["Generates an access token"])
  private fun generateAccessToken(
    @CommandLine.Option(names = ["--audience"], required = true) audience: String,
    @CommandLine.Option(names = ["--subject"], required = true) subject: String,
    @CommandLine.Option(
      names = ["--scope"],
      description = ["Scope. Can be specified multiple times"],
      required = false,
    )
    scopes: Set<String>?,
    @CommandLine.Option(names = ["--ttl"], required = false, defaultValue = "5m") ttl: Duration,
  ) {
    val credentials = provider.generateCredentials(audience, subject, scopes ?: emptySet(), ttl)

    print(credentials.token)
    consolePrintln()
  }

  companion object {
    /** Prints a line to the system console if one is attached. */
    private fun consolePrintln() {
      val console: Console = System.console() ?: return
      console.writer().println()
    }

    @JvmStatic fun main(args: Array<String>) = commandLineMain(OpenIdProvider(), args)
  }
}
