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

import java.time.Duration
import kotlin.properties.Delegates
import org.wfanet.measurement.common.identity.DuchyInfoFlags
import org.wfanet.measurement.duchy.deploy.common.CommonDuchyFlags
import picocli.CommandLine

class LiquidLegionsV2MillFlags {
  @CommandLine.Mixin
  lateinit var duchy: CommonDuchyFlags
    private set

  @CommandLine.Mixin
  lateinit var duchyInfoFlags: DuchyInfoFlags
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
    names = ["--computations-service-target"],
    description = ["Address and port of the internal Computations service"],
    required = true
  )
  lateinit var computationsServiceTarget: String
    private set

  @CommandLine.Option(
    names = ["--system-computations-service-target"],
    description = ["Address and port of the Kingdom's System ComputationsService"],
    required = true
  )
  lateinit var systemComputationsServiceTarget: String
    private set

  @CommandLine.Option(
    names = ["--system-computation-log-entries-service-target"],
    description = ["Address and port of the Kingdom's System ComputationLogEntriesService"],
    required = true
  )
  lateinit var systemComputationLogEntriesServiceTarget: String
    private set

  @CommandLine.Option(
    names = ["--system-computation-participants-service-target"],
    description = ["Address and port of the Kingdom's System ComputationParticipantsService"],
    required = true
  )
  lateinit var systemComputationParticipantsServiceTarget: String
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
}
