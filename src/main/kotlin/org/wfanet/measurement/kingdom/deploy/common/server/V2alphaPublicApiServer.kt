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

package org.wfanet.measurement.kingdom.deploy.common.server

import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.identity.DuchyInfoFlags
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.kingdom.service.api.v2alpha.DataProvidersService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ExchangeStepAttemptsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ExchangeStepsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.MeasurementConsumersService
import org.wfanet.measurement.kingdom.service.api.v2alpha.RequisitionsService
import picocli.CommandLine

private const val SERVER_NAME = "V2alphaPublicApiServer"

@CommandLine.Command(
  name = "v2alpha_public_api_server",
  description = ["Server daemon for Kingdom v2alpha public API services."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin kingdomApiServerFlags: KingdomApiServerFlags,
  @CommandLine.Mixin duchyInfoFlags: DuchyInfoFlags,
  @CommandLine.Mixin commonServerFlags: CommonServer.Flags
) {
  runKingdomApiServer(kingdomApiServerFlags, SERVER_NAME, duchyInfoFlags, commonServerFlags) {
    channel ->
    listOf(
      DataProvidersService(DataProvidersCoroutineStub(channel)),
      ExchangeStepAttemptsService(ExchangeStepAttemptsCoroutineStub(channel)),
      ExchangeStepsService(ExchangeStepsCoroutineStub(channel)),
      MeasurementConsumersService(MeasurementConsumersCoroutineStub(channel)),
      RequisitionsService(RequisitionsCoroutineStub(channel))
      // TODO: add missing services, e.g. CertificatesService, etc.
      )
  }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
