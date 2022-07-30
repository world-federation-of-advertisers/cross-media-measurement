// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.common

import picocli.CommandLine

class KingdomApiFlags {
  @set:CommandLine.Option(
    names = ["--kingdom-api-target"],
    description = ["gRPC target (authority) of the Kingdom public API server"],
    required = true,
  )
  lateinit var target: String

  @CommandLine.Option(
    names = ["--kingdom-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Kingdom public API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --kingdom-api-target.",
      ],
    required = false,
  )
  var certHost: String? = null
    private set
}
