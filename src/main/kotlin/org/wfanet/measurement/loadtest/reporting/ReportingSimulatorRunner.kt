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

import io.grpc.ManagedChannel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.loadtest.config.EventFilters.EVENT_TEMPLATES_TO_FILTERS_MAP
import org.wfanet.measurement.reporting.v1alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.ReportsGrpcKt.ReportsCoroutineStub
import picocli.CommandLine

class ReportingSimulatorRunner : Runnable {
  @CommandLine.Mixin
  lateinit var flags: ReportingSimulatorFlags
    private set

  override fun run() {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = flags.tlsFlags.certFile,
        privateKeyFile = flags.tlsFlags.privateKeyFile,
        trustedCertCollectionFile = flags.tlsFlags.certCollectionFile
      )
    val v1alphaPublicApiChannel: ManagedChannel =
      buildMutualTlsChannel(
        flags.reportingPublicApiFlags.target,
        clientCerts,
        flags.reportingPublicApiFlags.certHost
      )
    val eventGroupsStub = EventGroupsCoroutineStub(v1alphaPublicApiChannel)
    val reportingSetsStub = ReportingSetsCoroutineStub(v1alphaPublicApiChannel)
    val reportsStub = ReportsCoroutineStub(v1alphaPublicApiChannel)

    val mcName = flags.mcResourceName

    val measurementConsumerData = MeasurementConsumerData(mcName)

    runBlocking {
      // Runs the reporting simulator.
      val reportingSimulator =
        ReportingSimulator(
          measurementConsumerData,
          eventGroupsStub,
          reportingSetsStub,
          reportsStub,
          EVENT_TEMPLATES_TO_FILTERS_MAP
        )

      launch { reportingSimulator.execute(flags.runId) }
    }
  }
}

fun main(args: Array<String>) = commandLineMain(ReportingSimulatorRunner(), args)
