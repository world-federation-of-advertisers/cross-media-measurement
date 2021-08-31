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
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub as InternalDataProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub as InternalExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub as InternalExchangeStepsCoroutineStub
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as InternalMeasurementConsumersCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub as InternalRequisitionsCoroutineStub
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfigFlags
import org.wfanet.measurement.kingdom.service.api.v2alpha.DataProvidersService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ExchangeStepAttemptsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ExchangeStepsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.MeasurementConsumersService
import org.wfanet.measurement.kingdom.service.api.v2alpha.RequisitionsService
import picocli.CommandLine

private const val SERVER_NAME = "V2alphaPublicApiServer"

@CommandLine.Command(
  name = SERVER_NAME,
  description = ["Server daemon for Kingdom v2alpha public API services."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin kingdomApiServerFlags: KingdomApiServerFlags,
  @CommandLine.Mixin duchyInfoFlags: DuchyInfoFlags,
  @CommandLine.Mixin commonServerFlags: CommonServer.Flags,
  @CommandLine.Mixin llv2ProtocolConfigFlags: Llv2ProtocolConfigFlags
) {
  Llv2ProtocolConfig.initializeFromFlags(llv2ProtocolConfigFlags)

  runKingdomApiServer(kingdomApiServerFlags, SERVER_NAME, duchyInfoFlags, commonServerFlags) {
    channel ->
    listOf(
      DataProvidersService(InternalDataProvidersCoroutineStub(channel)),
      ExchangeStepAttemptsService(InternalExchangeStepAttemptsCoroutineStub(channel)),
      ExchangeStepsService(InternalExchangeStepsCoroutineStub(channel)),
      MeasurementConsumersService(InternalMeasurementConsumersCoroutineStub(channel)),
      RequisitionsService(InternalRequisitionsCoroutineStub(channel))
      // TODO: add missing services, e.g. CertificatesService, etc.
      )
  }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
