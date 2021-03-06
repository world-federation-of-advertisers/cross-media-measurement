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

package org.wfanet.measurement.loadtest.frontend

import io.grpc.ManagedChannel
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.consent.crypto.keystore.testing.InMemoryKeyStore
import org.wfanet.measurement.loadtest.storage.SketchStore
import org.wfanet.measurement.storage.StorageClient
import picocli.CommandLine

/** The base class of the frontend simulator runner. */
abstract class FrontendSimulatorRunner : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: FrontendSimulatorFlags
    private set

  protected fun run(storageClient: StorageClient) {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = flags.tlsFlags.certFile,
        privateKeyFile = flags.tlsFlags.privateKeyFile,
        trustedCertCollectionFile = flags.tlsFlags.certCollectionFile
      )
    val v2alphaPublicApiChannel: ManagedChannel =
      buildMutualTlsChannel(
        flags.kingdomPublicApiFlags.target,
        clientCerts,
        flags.kingdomPublicApiFlags.certHost
      )
    val dataProvidersStub = DataProvidersCoroutineStub(v2alphaPublicApiChannel)
    val eventGroupsStub = EventGroupsCoroutineStub(v2alphaPublicApiChannel)
    val requisitionsStub = RequisitionsCoroutineStub(v2alphaPublicApiChannel)
    val measurementsStub = MeasurementsCoroutineStub(v2alphaPublicApiChannel)
    val measurementConsumersStub = MeasurementConsumersCoroutineStub(v2alphaPublicApiChannel)

    val inMemoryKeyStore = InMemoryKeyStore()
    val mcName = flags.mcResourceName
    val mcConsentSignalingKeyId = "$mcName-cs-private-key"
    val mcEncryptionKeyId = "$mcName-enc-private-key"

    val measurementConsumerData =
      MeasurementConsumerData(mcName, mcConsentSignalingKeyId, mcEncryptionKeyId)
    val outputDpParams =
      DifferentialPrivacyParams.newBuilder()
        .apply {
          epsilon = flags.outputDpEpsilon
          delta = flags.outputDpDelta
        }
        .build()

    runBlocking {
      // Populates data to the inMemoryKeyStore.
      inMemoryKeyStore.storePrivateKeyDer(
        mcConsentSignalingKeyId,
        flags.mcCsPrivateKeyDerFile.readBytes().toByteString()
      )
      inMemoryKeyStore.storePrivateKeyDer(
        mcEncryptionKeyId,
        flags.mcEncPrivateKeyDerFile.readBytes().toByteString()
      )

      // Runs the frontend simulator.
      FrontendSimulator(
          measurementConsumerData,
          outputDpParams,
          inMemoryKeyStore,
          dataProvidersStub,
          eventGroupsStub,
          measurementsStub,
          requisitionsStub,
          measurementConsumersStub,
          SketchStore(storageClient),
          flags.runId
        )
        .process()
    }
  }
}
