// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.panelmatch.client.deploy

import java.time.Duration
import kotlin.properties.Delegates
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Party
import picocli.CommandLine.ArgGroup
import picocli.CommandLine.Model.CommandSpec
import picocli.CommandLine.Option
import picocli.CommandLine.ParameterException
import picocli.CommandLine.Spec

class ExchangeWorkflowFlags {

  @Spec lateinit var spec: CommandSpec

  @ArgGroup(exclusive = true, multiplicity = "1")
  lateinit var protocolOptions: ProtocolOptions
    private set

  @Option(names = ["--id"], description = ["Id of the provider"], required = true)
  lateinit var id: String
    private set

  @Option(
    names = ["--party-type"],
    description = ["Type of the party: \${COMPLETION-CANDIDATES}"],
    required = true,
  )
  lateinit var partyType: Party
    private set

  @ArgGroup(exclusive = false, multiplicity = "0..1")
  lateinit var tlsFlags: TlsFlags
    private set

  @Option(
    names = ["--storage-signing-algorithm"],
    defaultValue = "EC",
    description = ["The algorithm used in signing data written to shared storage."],
    required = true,
  )
  lateinit var certAlgorithm: String
    private set

  @Option(
    names = ["--polling-interval"],
    defaultValue = "1m",
    description = ["How long to sleep between finding and running an ExchangeStep."],
    required = true,
  )
  lateinit var pollingInterval: Duration
    private set

  @Option(
    names = ["--task-timeout"],
    defaultValue = "24h",
    description = ["How long to sleep between finding and running an ExchangeStep."],
    required = true,
  )
  lateinit var taskTimeout: Duration
    private set

  @set:Option(
    names = ["--preprocessing-max-byte-size"],
    defaultValue = "1000000",
    description = ["Max batch size for processing"],
    required = true,
  )
  var preProcessingMaxByteSize by Delegates.notNull<Long>()
    private set

  @set:Option(
    names = ["--preprocessing-file-count"],
    defaultValue = "1000",
    description = ["Number of output files from event preprocessing step"],
    required = true,
  )
  var preProcessingFileCount by Delegates.notNull<Int>()
    private set

  var maxParallelClaimedExchangeSteps: Int? = null
    private set

  @Option(
    names = ["--max-concurrent-tasks"],
    description = ["Maximum number of concurrent tasks to allow the daemon to run."],
    required = false,
  )
  private fun setMaxParallelClaimedSteps(value: Int?) {
    if ((value ?: 1) < 1) {
      throw ParameterException(
        spec.commandLine(),
        "Max-parallel-claimed-tasks must be greater than 0 if set. Currently it is $value",
      )
    }
    maxParallelClaimedExchangeSteps = value
  }

  @Option(
    names = ["--fallback-private-key-blob-key"],
    defaultValue = "",
    description =
      [
        "Fallback blob key for a kms-encrypted private signing key when a workflow does not generate one."
      ],
    required = true,
  )
  lateinit var fallbackPrivateKeyBlobKey: String
    private set

  @Option(
    names = ["--run-mode"],
    description =
      [
        "The manner in which the job is being run, determining the structure of the job's main loop"
      ],
    defaultValue = "DAEMON", // For backwards compatibility
    required = true,
  )
  lateinit var runMode: ExchangeWorkflowDaemon.RunMode
    private set
}

class ProtocolOptions {

  @ArgGroup(exclusive = false, multiplicity = "0..1")
  var kingdomBasedExchangeFlags: KingdomBasedExchangeFlags? = null
    private set

  @ArgGroup(exclusive = false, multiplicity = "0..1")
  var kingdomlessExchangeFlags: KingdomlessExchangeFlags? = null
    private set

  val protocolSpecificFlags: ProtocolSpecificFlags
    get() {
      return when {
        kingdomBasedExchangeFlags != null -> kingdomBasedExchangeFlags!!
        kingdomlessExchangeFlags != null -> kingdomlessExchangeFlags!!
        else -> error("Missing protocol-specific flags")
      }
    }
}

sealed interface ProtocolSpecificFlags

class KingdomBasedExchangeFlags : ProtocolSpecificFlags {

  @Option(
    names = ["--channel-shutdown-timeout"],
    defaultValue = "3s",
    description = ["How long to allow for the gRPC channel to shutdown."],
    required = true,
  )
  lateinit var channelShutdownTimeout: Duration
    private set

  @Option(
    names = ["--exchange-api-target"],
    description =
      ["Address and port for servers hosting /ExchangeSteps and /ExchangeStepAttempts services"],
    required = true,
  )
  lateinit var exchangeApiTarget: String
    private set

  @Option(
    names = ["--exchange-api-cert-host"],
    description = ["Expected hostname in the TLS certificate for --exchange-api-target"],
    required = true,
  )
  lateinit var exchangeApiCertHost: String
    private set

  @Option(
    names = ["--debug-verbose-grpc-client-logging"],
    description = ["Enables full gRPC request and response logging for outgoing gRPCs"],
  )
  var debugVerboseGrpcClientLogging = false
    private set
}

class KingdomlessExchangeFlags : ProtocolSpecificFlags {

  @Option(
    names = ["--kingdomless-recurring-exchange-ids"],
    description = ["IDs of recurring exchanges to run using the Kingdom-less protocol"],
    split = ",",
    required = true,
  )
  lateinit var kingdomlessRecurringExchangeIds: List<String>
    private set

  @Option(
    names = ["--checkpoint-signing-algorithm"],
    description = ["Algorithm to use for signing exchange checkpoints"],
    defaultValue = "SHA256withECDSA",
    required = true,
  )
  lateinit var checkpointSigningAlgorithm: String
    private set

  @Option(
    names = ["--lookback-window"],
    description = ["When claiming exchange tasks, how far back to look for available tasks"],
    defaultValue = "14d",
    required = true,
  )
  lateinit var lookbackWindow: Duration
    private set
}
