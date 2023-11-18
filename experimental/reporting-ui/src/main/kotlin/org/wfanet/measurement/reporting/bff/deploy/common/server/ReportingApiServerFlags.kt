// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.bff.deploy.common.server

import kotlin.properties.Delegates
import picocli.CommandLine

class ReportingApiFlags {
  @set:CommandLine.Option(
    names = ["--reporting-api-target"],
    description = ["gRPC target Reporting public API server"],
    required = true,
  )
  lateinit var target: String

  @set:CommandLine.Option(
    names = ["--reporting-api-certHost"],
    description = ["gRPC cert Reporting public API server"],
    required = true,
  )
  lateinit var certHost: String
}

class ReportingApiServerFlags {
  @CommandLine.Mixin
  lateinit var reportingApiFlags: ReportingApiFlags
    private set

  @set:CommandLine.Option(
    names = ["--debug-verbose-grpc-client-logging"],
    description = ["Enables full gRPC request and response logging for outgoing gRPCs"],
    defaultValue = "false"
  )
  var debugVerboseGrpcClientLogging by Delegates.notNull<Boolean>()
    private set

  @set:CommandLine.Option(
    names = ["--measurement-consumer"],
    description = ["measurement consumer id of the service owner"],
    required = true,
  )
  lateinit var measurementConsumer: String
}
