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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.protobuf.Message
import io.grpc.ManagedChannel
import java.time.Clock
import kotlin.random.Random
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.loadtest.config.PrivacyBudgets.createNoOpPrivacyBudgetManager
import picocli.CommandLine

/** The base class of the EdpSimulator runner. */
abstract class EdpSimulatorRunner : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: EdpSimulatorFlags
    private set

  protected fun run(
    eventQuery: EventQuery<Message>,
    metadataByReferenceIdSuffix: Map<String, Message>,
  ) {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = flags.tlsFlags.certFile,
        privateKeyFile = flags.tlsFlags.privateKeyFile,
        trustedCertCollectionFile = flags.tlsFlags.certCollectionFile,
      )

    val v2AlphaPublicApiChannel: ManagedChannel =
      buildMutualTlsChannel(
        flags.kingdomPublicApiFlags.target,
        clientCerts,
        flags.kingdomPublicApiFlags.certHost,
      )
    val requisitionsStub = RequisitionsCoroutineStub(v2AlphaPublicApiChannel)
    val eventGroupsStub = EventGroupsCoroutineStub(v2AlphaPublicApiChannel)
    val eventGroupMetadataDescriptorsStub =
      EventGroupMetadataDescriptorsCoroutineStub(v2AlphaPublicApiChannel)
    val measurementConsumersStub = MeasurementConsumersCoroutineStub(v2AlphaPublicApiChannel)
    val certificatesStub = CertificatesCoroutineStub(v2AlphaPublicApiChannel)
    val dataProvidersStub = DataProvidersCoroutineStub(v2AlphaPublicApiChannel)

    val requisitionFulfillmentStub =
      RequisitionFulfillmentCoroutineStub(
        buildMutualTlsChannel(
          flags.requisitionFulfillmentServiceFlags.target,
          clientCerts,
          flags.requisitionFulfillmentServiceFlags.certHost,
        )
      )
    val signingKeyHandle =
      loadSigningKey(flags.edpCsCertificateDerFile, flags.edpCsPrivateKeyDerFile)
    val certificateKey =
      DataProviderCertificateKey.fromName(flags.dataProviderCertificateResourceName)!!
    val edpData =
      EdpData(
        flags.dataProviderResourceName,
        flags.dataProviderDisplayName,
        loadPrivateKey(flags.edpEncryptionPrivateKeyset),
        signingKeyHandle,
        certificateKey,
      )

    val randomSeed = flags.randomSeed
    val random =
      if (randomSeed != null) {
        Random(randomSeed)
      } else {
        Random.Default
      }

    val edpSimulator =
      EdpSimulator(
        edpData,
        flags.mcResourceName,
        measurementConsumersStub,
        certificatesStub,
        dataProvidersStub,
        eventGroupsStub,
        eventGroupMetadataDescriptorsStub,
        requisitionsStub,
        requisitionFulfillmentStub,
        eventQuery,
        MinimumIntervalThrottler(Clock.systemUTC(), flags.throttlerMinimumInterval),
        createNoOpPrivacyBudgetManager(),
        clientCerts.trustedCertificates,
        random = random,
        compositionMechanism = flags.compositionMechanism,
      )
    runBlocking {
      edpSimulator.ensureEventGroups(metadataByReferenceIdSuffix)
      edpSimulator.run()
    }
  }
}
