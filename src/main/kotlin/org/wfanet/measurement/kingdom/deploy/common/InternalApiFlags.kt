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

package org.wfanet.measurement.kingdom.deploy.common

import java.time.Duration
import picocli.CommandLine

class InternalApiFlags {
  @CommandLine.Option(
    names = ["--internal-api-target"],
    description = ["gRPC target (authority) of the Kingdom internal API server"],
    required = true,
  )
  lateinit var target: String
    private set

  @CommandLine.Option(
    names = ["--internal-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Kingdom internal API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --internal-api-target.",
      ],
    required = false,
  )
  var certHost: String? = null
    private set

  @CommandLine.Option(
    names = ["--internal-api-default-deadline"],
    description = ["Default deadline duration for RPCs to internal API"],
    defaultValue = "30s"
  )
  lateinit var defaultDeadlineDuration: Duration
    private set
}
