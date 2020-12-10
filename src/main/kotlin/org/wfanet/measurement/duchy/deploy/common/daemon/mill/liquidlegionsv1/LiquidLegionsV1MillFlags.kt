// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.duchy.deploy.common.daemon.mill.liquidlegionsv1

import java.time.Duration
import kotlin.properties.Delegates
import org.wfanet.measurement.duchy.DuchyPublicKeys
import org.wfanet.measurement.duchy.deploy.common.CommonDuchyFlags
import picocli.CommandLine

class LiquidLegionsV1MillFlags {
  @CommandLine.Mixin
  lateinit var duchy: CommonDuchyFlags
    private set

  @CommandLine.Mixin
  lateinit var duchyPublicKeys: DuchyPublicKeys.Flags
    private set

  // TODO: Switch to a more secure option such as a keystore prior to initial
  // production deployment.
  @CommandLine.Option(
    names = ["--duchy-secret-key"],
    description = ["This Duchy's secret key component of its ElGamal key pair."],
    required = true
  )
  lateinit var duchySecretKey: String
    private set

  @CommandLine.Option(
    names = ["--channel-shutdown-timeout"],
    defaultValue = "3s",
    description = ["How long to allow for the gRPC channel to shutdown."]
  )
  lateinit var channelShutdownTimeout: Duration
    private set

  @CommandLine.Option(
    names = ["--computation-control-service-target"],
    description = [
      "gRPC target (authority string or URI) of the ComputationControl service in another Duchy.",
      "This is a key=value pair where the key is the other Duchy's name. It can be repeated."
    ],
    required = true
  )
  lateinit var computationControlServiceTargets: Map<String, String>
    private set

  @CommandLine.Option(
    names = ["--polling-interval"],
    defaultValue = "2s",
    description = ["How long to sleep before polling the computation queue again if it is empty."]
  )
  lateinit var pollingInterval: Duration
    private set

  @CommandLine.Option(
    names = ["--computations-service-target"],
    description = ["Address and port of the internal Computations service"],
    required = true
  )
  lateinit var computationsServiceTarget: String
    private set

  @CommandLine.Option(
    names = ["--metric-values-service-target"],
    description = ["Address and port of the same duchy's MetricValuesService"],
    required = true
  )
  lateinit var metricValuesServiceTarget: String
    private set

  @CommandLine.Option(
    names = ["--global-computation-service-target"],
    description = ["Address and port of the Kingdom's Global Computation Service"],
    required = true
  )
  lateinit var globalComputationsServiceTarget: String
    private set

  @CommandLine.Option(
    names = ["--mill-id"],
    description = ["The Identifier of the Mill."],
    required = true
  )
  lateinit var millId: String
    private set

  @set:CommandLine.Option(
    names = ["--bytes-per-chunk"],
    description = ["The number of bytes in a chunk when sending rpc result to other duchy."],
    defaultValue = "32768" // 32 KiB. See https://github.com/grpc/grpc.github.io/issues/371.
  )
  var requestChunkSizeBytes by Delegates.notNull<Int>()
    private set

  @set:CommandLine.Option(
    names = ["--liquid-legions-decay-rate"],
    description = ["The decay rate of liquid legions sketch."],
    defaultValue = "12.0"
  )
  var liquidLegionsDecayRate by Delegates.notNull<Double>()
    private set

  @set:CommandLine.Option(
    names = ["--liquid-legions-size"],
    description = ["The maximum size of liquid legions sketch."],
    defaultValue = "100000"
  )
  var liquidLegionsSize by Delegates.notNull<Long>()
    private set

  @set:CommandLine.Option(
    names = ["--sketch-max-frequency"],
    description = ["The maximum frequency to reveal in the histogram."],
    defaultValue = "10"
  )
  var sketchMaxFrequency by Delegates.notNull<Int>()
    private set
}
