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

package org.wfanet.measurement.duchy.mill

import java.time.Duration
import kotlin.properties.Delegates
import org.wfanet.measurement.crypto.DuchyPublicKeys
import org.wfanet.measurement.duchy.CommonDuchyFlags
import picocli.CommandLine

class MillFlags {
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
    description = ["How long to allow for the gRPC channel to shutdown."],
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
    defaultValue = "1s",
    description = ["How long to sleep before polling the computation queue again if it is empty."],
  )
  lateinit var pollingInterval: Duration
    private set

  @CommandLine.Option(
    names = ["--computation-storage-service-target"],
    required = true
  )
  lateinit var computationStorageServiceTarget: String
    private set

  @CommandLine.Option(
    names = ["--metric-values-service-target"],
    description = ["Address and port of the same duchy's MetricValuesService"],
    required = true
  )
  lateinit var metricValuesServiceTarget: String
    private set

  @CommandLine.Option(
    names = ["--global-computations-service-target"],
    description = ["Address and port of the Kingdom's Global Computations Service"],
    required = true
  )
  lateinit var globalComputationsServiceTarget: String
    private set

  @set:CommandLine.Option(
    names = ["--mill-id"],
    description = ["The Identifier of the Mill."],
    required = true,
    defaultValue = "mill-0000"
  )
  lateinit var millId: String
    private set

  @set:CommandLine.Option(
    names = ["--bytes-per-chunk"],
    description = ["The number of bytes in a chunk when sending result to other duchy."],
    required = true,
    defaultValue = "2000000"
  )
  var chunkSize by Delegates.notNull<Int>()
    private set
}
