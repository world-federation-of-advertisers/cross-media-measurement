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

package org.wfanet.measurement.loadtest

import java.time.Duration
import picocli.CommandLine

class KingdomPublicApiFlags {
  @CommandLine.Option(
    names = ["--kingdom-public-api-target"],
    description = ["gRPC target (authority) of the Kingdom public API server"],
    required = true,
  )
  lateinit var target: String
    private set

  @CommandLine.Option(
    names = ["--kingdom-public-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Kingdom public API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --kingdom-public-api-target.",
      ],
    required = false,
  )
  var certHost: String? = null
    private set
}

class RequisitionFulfillmentServiceFlags {
  @CommandLine.Option(
    names = ["--requisition-fulfillment-service-target"],
    description = ["gRPC target (authority) of the Duchy RequisitionFullfillment server"],
    required = true,
  )
  lateinit var target: String
    private set

  @CommandLine.Option(
    names = ["--requisition-fulfillment-service-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Duchy RequisitionFullfillment server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from " +
          "--requisition-fulfillment-service-target.",
      ],
    required = false,
  )
  var certHost: String? = null
    private set
}

class KingdomInternalApiFlags {
  @CommandLine.Option(
    names = ["--kingdom-internal-api-target"],
    description = ["gRPC target (authority) of the Kingdom internal API server"],
    required = true,
  )
  lateinit var target: String
    private set

  @CommandLine.Option(
    names = ["--kingdom-internal-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Kingdom internal API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --kingdom-internal-api-target.",
      ],
    required = false,
  )
  var certHost: String? = null
    private set

  @CommandLine.Option(
    names = ["--kingdom-internal-api-default-deadline"],
    description = ["Default deadline duration for RPCs to Kingdom internal API"],
    defaultValue = "30s"
  )
  lateinit var defaultDeadlineDuration: Duration
    private set
}
