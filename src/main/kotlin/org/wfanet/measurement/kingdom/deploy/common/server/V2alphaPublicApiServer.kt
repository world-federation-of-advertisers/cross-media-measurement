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
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineStub as InternalCertificatesCoroutineStub
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub as InternalDataProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineStub as InternalEventGroupsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub as InternalExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub as InternalExchangeStepsCoroutineStub
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as InternalMeasurementConsumersCoroutineStub
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub as InternalRequisitionsCoroutineStub
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfigFlags
import org.wfanet.measurement.kingdom.service.api.v2alpha.CertificatesService
import org.wfanet.measurement.kingdom.service.api.v2alpha.DataProvidersService
import org.wfanet.measurement.kingdom.service.api.v2alpha.EventGroupsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ExchangeStepAttemptsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ExchangeStepsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.MeasurementConsumersService
import org.wfanet.measurement.kingdom.service.api.v2alpha.MeasurementsService
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
  @CommandLine.Mixin commonServerFlags: CommonServer.Flags,
  @CommandLine.Mixin llv2ProtocolConfigFlags: Llv2ProtocolConfigFlags
) {
  Llv2ProtocolConfig.initializeFromFlags(llv2ProtocolConfigFlags)

  val clientCerts =
    SigningCerts.fromPemFiles(
      certificateFile = commonServerFlags.tlsFlags.certFile,
      privateKeyFile = commonServerFlags.tlsFlags.privateKeyFile,
      trustedCertCollectionFile = commonServerFlags.tlsFlags.certCollectionFile
    )
  val channel =
    buildMutualTlsChannel(
        kingdomApiServerFlags.internalApiFlags.target,
        clientCerts,
        kingdomApiServerFlags.internalApiFlags.certHost
      )
      .withVerboseLogging(kingdomApiServerFlags.debugVerboseGrpcClientLogging)

  // TODO: do we need something similar to .withDuchyIdentities() for EDP and MC?
  val service =
    listOf(
      CertificatesService(InternalCertificatesCoroutineStub(channel)),
      DataProvidersService(InternalDataProvidersCoroutineStub(channel)),
      EventGroupsService(InternalEventGroupsCoroutineStub(channel)),
      ExchangeStepAttemptsService(InternalExchangeStepAttemptsCoroutineStub(channel)),
      ExchangeStepsService(InternalExchangeStepsCoroutineStub(channel)),
      MeasurementsService(InternalMeasurementsCoroutineStub(channel)),
      MeasurementConsumersService(InternalMeasurementConsumersCoroutineStub(channel)),
      RequisitionsService(InternalRequisitionsCoroutineStub(channel))
    )
  CommonServer.fromFlags(commonServerFlags, SERVER_NAME, service).start().blockUntilShutdown()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
