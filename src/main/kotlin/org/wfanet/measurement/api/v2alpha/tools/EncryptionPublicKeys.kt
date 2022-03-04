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

import java.io.File
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.readByteString
import picocli.CommandLine

/**
 * Command-line utility for [EncryptionPublicKey] messages.
 *
 * Use the `help` subcommand for usage help.
 */
@CommandLine.Command(
  description = ["Utility for EncryptionPublicKey messages."],
  subcommands = [CommandLine.HelpCommand::class, Serialize::class, Sign::class]
)
class EncryptionPublicKeys private constructor() : Runnable {
  override fun run() {
    // No-op. See subcommands.
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(EncryptionPublicKeys(), args)
  }
}

@CommandLine.Command(name = "serialize", showDefaultValues = true)
private class Serialize : Runnable {
  @CommandLine.Option(
    names = ["--format"],
    description = ["EncryptionPublicKey format"],
    defaultValue = "TINK_KEYSET"
  )
  private lateinit var format: EncryptionPublicKey.Format

  @CommandLine.Option(
    names = ["--data"],
    description = ["File containing format-specific key data"],
    required = true
  )
  private lateinit var data: File

  @CommandLine.Option(names = ["--out", "-o"], description = ["Output file"], required = true)
  private lateinit var out: File

  override fun run() {
    val encryptionPublicKey = encryptionPublicKey {
      format = this@Serialize.format
      data = this@Serialize.data.readByteString()
    }

    out.outputStream().use { encryptionPublicKey.writeTo(it) }
  }
}

@CommandLine.Command(name = "sign")
private class Sign : Runnable {
  @CommandLine.Option(
    names = ["--certificate"],
    description = ["X.509 certificate in DER or PEM format"],
    required = true
  )
  private lateinit var certificateFile: File

  @CommandLine.Option(
    names = ["--signing-key"],
    description = ["Signing key in DER format"],
    required = true
  )
  private lateinit var signingKeyFile: File

  @CommandLine.Option(
    names = ["--in", "-i"],
    description = ["Input serialized EncryptionPublicKey message"],
    required = true
  )
  private lateinit var inFile: File

  @CommandLine.Option(names = ["--out", "-o"], description = ["Output file"], required = true)
  private lateinit var outFile: File

  override fun run() {
    val serialized = inFile.readByteString()

    // Validate that input is an EncryptionPublicKey message.
    EncryptionPublicKey.parseFrom(serialized)

    val certificate = certificateFile.inputStream().use { readCertificate(it) }
    val privateKey =
      readPrivateKey(signingKeyFile.readByteString(), certificate.publicKey.algorithm)
    val signature = SigningKeyHandle(certificate, privateKey).sign(serialized)

    outFile.outputStream().use { signature.writeTo(it) }
  }
}
