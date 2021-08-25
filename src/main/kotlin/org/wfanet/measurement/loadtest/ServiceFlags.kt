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

import picocli.CommandLine

class KingdomPublicApiFlags {
  @CommandLine.Option(
    names = ["--kingdom-public-api-target"],
    description = ["Address and port of the Kingdom's public APIs"],
    required = true
  )
  lateinit var target: String
    private set

  @CommandLine.Option(
    names = ["--kingdom-public-api-cert-host"],
    description = ["The expected hostname in the kingdom PublicApiServer's TLS certificate."],
    required = true
  )
  lateinit var certHost: String
    private set
}

class RequisitionFulfillmentServiceFlags {
  @CommandLine.Option(
    names = ["--requisition-fulfillment-service-target"],
    description = ["Address and port of the duchy's RequisitionFulfillmentService"],
    required = true
  )
  lateinit var target: String
    private set

  @CommandLine.Option(
    names = ["--requisition-fulfillment-service-cert-host"],
    description =
      ["The expected hostname in the duchy's RequisitionFulfillmentServer's TLS certificate."],
    required = true
  )
  lateinit var certHost: String
    private set
}

class EventGroupServiceFlags {
  @CommandLine.Option(
    names = ["--event-group-service-target"],
    description = ["Address and port of the EventGroupService"],
    required = true
  )
  lateinit var target: String
    private set

  @CommandLine.Option(
    names = ["--event-group-service-cert-host"],
    description = ["The expected hostname in theEventGroupService's TLS certificate."],
    required = true
  )
  lateinit var certHost: String
    private set
}
