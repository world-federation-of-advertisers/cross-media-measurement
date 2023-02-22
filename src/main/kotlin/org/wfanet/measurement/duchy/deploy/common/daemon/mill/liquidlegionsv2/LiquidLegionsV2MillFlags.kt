// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.common.daemon.mill.liquidlegionsv2

import java.io.File
import java.time.Duration
import kotlin.properties.Delegates
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.identity.DuchyInfoFlags
import org.wfanet.measurement.duchy.deploy.common.CommonDuchyFlags
import org.wfanet.measurement.duchy.deploy.common.ComputationsServiceFlags
import org.wfanet.measurement.duchy.deploy.common.SystemApiFlags
import picocli.CommandLine

class LiquidLegionsV2MillFlags {
  @CommandLine.Mixin
  lateinit var duchy: CommonDuchyFlags
    private set

  @CommandLine.Mixin
  lateinit var tlsFlags: TlsFlags
    private set

  @CommandLine.Mixin
  lateinit var duchyInfoFlags: DuchyInfoFlags
    private set

  @CommandLine.Option(
    names = ["--duchy-computation-control-target"],
    description = ["Key-value pair of Duchy ID to ComputationControl service target."],
    required = true
  )
  lateinit var computationControlServiceTargets: Map<String, String>
    private set

  @CommandLine.Option(
    names = ["--channel-shutdown-timeout"],
    defaultValue = "3s",
    description = ["How long to allow for the gRPC channel to shutdown."]
  )
  lateinit var channelShutdownTimeout: Duration
    private set

  @CommandLine.Option(
    names = ["--polling-interval"],
    defaultValue = "2s",
    description = ["How long to sleep before polling the computation queue again if it is empty."]
  )
  lateinit var pollingInterval: Duration
    private set

  @CommandLine.Option(
    names = ["--work-lock-duration"],
    defaultValue = "5m",
    description = ["How long to hold work locks."]
  )
  lateinit var workLockDuration: Duration
    private set

  @CommandLine.Mixin
  lateinit var systemApiFlags: SystemApiFlags
    private set

  @CommandLine.Mixin
  lateinit var computationsServiceFlags: ComputationsServiceFlags
    private set

  @set:CommandLine.Option(
    names = ["--bytes-per-chunk"],
    description = ["The number of bytes in a chunk when sending rpc result to other duchy."],
    defaultValue = "32768" // 32 KiB. See https://github.com/grpc/grpc.github.io/issues/371.
  )
  var requestChunkSizeBytes by Delegates.notNull<Int>()
    private set

  @CommandLine.Option(
    names = ["--consent-signaling-certificate-resource-name"],
    description = ["The resource name of the duchy's consent signaling certificate."],
    required = true
  )
  lateinit var csCertificateName: String
    private set

  @CommandLine.Option(
    names = ["--consent-signaling-private-key-der-file"],
    description = ["The duchy's consent signaling private key (DER format) file."],
    required = true
  )
  lateinit var csPrivateKeyDerFile: File
    private set

  @CommandLine.Option(
    names = ["--consent-signaling-certificate-der-file"],
    description = ["The duchy's consent signaling certificate (DER format) file."],
    required = true
  )
  lateinit var csCertificateDerFile: File
    private set

  @CommandLine.ArgGroup(exclusive = false) var openTelemetryOptions: OpenTelemetryOptions? = null
  // All options here must be present or none of them must be present.
  class OpenTelemetryOptions {
    @CommandLine.Option(
      names = ["--otel-exporter-otlp-endpoint"],
      description = ["Endpoint for OpenTelemetry Collector."],
      required = true,
    )
    lateinit var otelExporterOtlpEndpoint: String
      private set

    @CommandLine.Option(
      names = ["--otel-service-name"],
      description = ["Service name to label duchy metrics with."],
      required = true,
    )
    lateinit var otelServiceName: String
      private set
  }
}
