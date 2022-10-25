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

package org.wfanet.measurement.duchy.deploy.common

import java.time.Duration
import picocli.CommandLine

class ComputationsServiceFlags {
  @CommandLine.Option(
    names = ["--computations-service-target"],
    description = ["gRPC target (authority) of the Duchy internal Computations API server"],
    required = true,
  )
  lateinit var target: String
    private set

  @CommandLine.Option(
    names = ["--computations-service-cert-host"],
    description =
      ["The expected hostname in the Duchy internal Computations server TLS certificate"],
    required = true,
  )
  lateinit var certHost: String
    private set

  @CommandLine.Option(
    names = ["--computations-default-deadline"],
    description = ["Default deadline duration for RPCs to Duchy internal Computations API"],
    defaultValue = "30s"
  )
  lateinit var defaultDeadlineDuration: Duration
    private set
}

class AsyncComputationControlServiceFlags {
  @CommandLine.Option(
    names = ["--async-computation-control-service-target"],
    description = ["Address and port of the AsyncComputationControlService."],
    required = true,
  )
  lateinit var target: String
    private set

  @CommandLine.Option(
    names = ["--async-computation-control-service-cert-host"],
    description = ["The expected hostname in the AsyncComputationControlServer's TLS certificate."],
    required = true,
  )
  lateinit var certHost: String
    private set
}

class SystemApiFlags {
  @CommandLine.Option(
    names = ["--kingdom-system-api-target"],
    description = ["gRPC target (authority) of the Kingdom system API server"],
    required = true,
  )
  lateinit var target: String
    private set

  @CommandLine.Option(
    names = ["--kingdom-system-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Kingdom system API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --kingdom-system-api-target.",
      ],
    required = false,
  )
  var certHost: String? = null
    private set
}
