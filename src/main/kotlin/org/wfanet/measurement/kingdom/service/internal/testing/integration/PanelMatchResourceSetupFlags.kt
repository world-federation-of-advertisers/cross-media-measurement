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

package org.wfanet.measurement.kingdom.service.internal.testing.integration

import kotlin.properties.Delegates
import org.wfanet.measurement.common.grpc.TlsFlags
import picocli.CommandLine

class PanelMatchResourceSetupFlags {
  /** The Panel Match ResourceSetup tls certificates. */
  @CommandLine.Mixin
  lateinit var tlsFlags: TlsFlags
    private set

  @set:CommandLine.Option(
    names = ["--internal-api-target"],
    description = ["Target for the Kingdom internal APIs"],
    required = true
  )
  lateinit var target: String

  @CommandLine.Option(
    names = ["--internal-api-cert-host"],
    description = ["The expected hostname in the Kingdom internal API Server's TLS certificate"],
    required = true
  )
  lateinit var certHost: String
    private set

  @set:CommandLine.Option(
    names = ["--debug-verbose-grpc-client-logging"],
    description = ["Enables full gRPC request and response logging for outgoing gRPCs"],
    defaultValue = "false"
  )
  var debugVerboseGrpcClientLogging by Delegates.notNull<Boolean>()
    private set
}
