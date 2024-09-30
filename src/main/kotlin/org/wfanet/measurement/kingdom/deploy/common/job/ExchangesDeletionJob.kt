/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.common.job

import kotlin.properties.Delegates
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.internal.kingdom.ExchangesGrpcKt.ExchangesCoroutineStub
import org.wfanet.measurement.kingdom.batch.ExchangesDeletion
import org.wfanet.measurement.kingdom.deploy.common.server.KingdomApiServerFlags
import picocli.CommandLine

private class ExchangesRetentionFlags {
  @CommandLine.Mixin
  lateinit var tlsFlags: TlsFlags
    private set

  @CommandLine.Mixin
  lateinit var kingdomApiServerFlags: KingdomApiServerFlags
    private set

  @set:CommandLine.Option(
    names = ["--days-to-live"],
    defaultValue = "100",
    description =
      [
        "Days to live for an Exchange since its scheduled date. An Exchange won't live longer" +
          "than this duration."
      ],
  )
  var exchangesDaysToLive by Delegates.notNull<Long>()
    private set

  @set:CommandLine.Option(
    names = ["--dry-run"],
    description =
      ["Option to print the Exchanges that would be deleted instead of performing the deletion."],
    required = false,
    defaultValue = "false",
  )
  var dryRun by Delegates.notNull<Boolean>()
    private set
}

@CommandLine.Command(
  name = "ExchangeDeletionJob",
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
private fun run(@CommandLine.Mixin flags: ExchangesRetentionFlags) {
  val clientCerts =
    SigningCerts.fromPemFiles(
      certificateFile = flags.tlsFlags.certFile,
      privateKeyFile = flags.tlsFlags.privateKeyFile,
      trustedCertCollectionFile = flags.tlsFlags.certCollectionFile,
    )

  val channel =
    buildMutualTlsChannel(
        flags.kingdomApiServerFlags.internalApiFlags.target,
        clientCerts,
        flags.kingdomApiServerFlags.internalApiFlags.certHost,
      )
      .withVerboseLogging(flags.kingdomApiServerFlags.debugVerboseGrpcClientLogging)
      .withDefaultDeadline(flags.kingdomApiServerFlags.internalApiFlags.defaultDeadlineDuration)

  val internalExchangesClient = ExchangesCoroutineStub(channel)

  val exchangesDeletion =
    ExchangesDeletion(internalExchangesClient, flags.exchangesDaysToLive, flags.dryRun)
  exchangesDeletion.run()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
