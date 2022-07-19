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

package org.wfanet.measurement.loadtest.reporting

import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import org.wfanet.measurement.common.grpc.TlsFlags
import picocli.CommandLine

class ReportingSimulatorFlags {
  @CommandLine.Mixin
  lateinit var tlsFlags: TlsFlags
    private set

  @CommandLine.Mixin
  lateinit var reportingPublicApiFlags: ReportingPublicApiFlags
    private set

  @CommandLine.Option(
    names = ["--mc-resource-name"],
    description = ["The resource name of the measurement consumer."],
    required = true
  )
  lateinit var mcResourceName: String
    private set

  @CommandLine.Option(
    names = ["--run-id"],
    description = ["Unique identifier of the run, if not set, timestamp will be used."],
    required = false
  )
  var runId: String =
    DateTimeFormatter.ofPattern("yyyy-MM-ddHH-mm-ss-SSS")
      .withZone(ZoneOffset.UTC)
      .format(Instant.now())
    private set
}
