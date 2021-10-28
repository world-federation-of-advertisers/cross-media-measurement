// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.common.daemon.mill.liquidlegionsv2

import java.time.Clock
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.consent.crypto.keystore.testing.InMemoryKeyStore
import org.wfanet.measurement.duchy.daemon.mill.CONSENT_SIGNALING_PRIVATE_KEY_ID
import org.wfanet.measurement.duchy.daemon.mill.Certificate
import org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2.LiquidLegionsV2Mill
import org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2.crypto.JniLiquidLegionsV2Encryption
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub as SystemComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as SystemComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineStub as SystemComputationsCoroutineStub
import picocli.CommandLine

abstract class LiquidLegionsV2MillDaemon : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: LiquidLegionsV2MillFlags
    private set

  protected fun run(storageClient: StorageClient) {
    DuchyInfo.initializeFromFlags(flags.duchyInfoFlags)
    val duchyName = flags.duchy.duchyName

    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = flags.tlsFlags.certFile,
        privateKeyFile = flags.tlsFlags.privateKeyFile,
        trustedCertCollectionFile = flags.tlsFlags.certCollectionFile
      )

    val computationsServiceChannel =
      buildMutualTlsChannel(
          flags.computationsServiceFlags.target,
          clientCerts,
          flags.computationsServiceFlags.certHost
        )
        .withShutdownTimeout(flags.channelShutdownTimeout)
    val dataClients =
      ComputationDataClients(
        ComputationsCoroutineStub(computationsServiceChannel).withDuchyId(duchyName),
        storageClient
      )

    val computationControlClientMap =
      DuchyInfo.entries.filter { it.duchyId != duchyName }.associate {
        it.duchyId to
          ComputationControlCoroutineStub(
              buildMutualTlsChannel(
                  it.computationControlServiceTarget,
                  clientCerts,
                  it.computationControlServiceCertHost
                )
                .withShutdownTimeout(flags.channelShutdownTimeout)
            )
            .withDuchyId(duchyName)
      }

    val systemApiChannel =
      buildMutualTlsChannel(flags.systemApiFlags.target, clientCerts, flags.systemApiFlags.certHost)
        .withShutdownTimeout(flags.channelShutdownTimeout)

    val systemComputationsClient =
      SystemComputationsCoroutineStub(systemApiChannel).withDuchyId(duchyName)
    val systemComputationParticipantsClient =
      SystemComputationParticipantsCoroutineStub(systemApiChannel).withDuchyId(duchyName)
    val systemComputationLogEntriesClient =
      SystemComputationLogEntriesCoroutineStub(systemApiChannel).withDuchyId(duchyName)

    val computationStatsClient = ComputationStatsCoroutineStub(computationsServiceChannel)

    val csCertificate =
      Certificate(
        flags.csCertificateName,
        readCertificate(flags.csCertificateDerFile.readBytes().toByteString())
      )
    val keyStore = InMemoryKeyStore() // TODO: use real keystore
    // This will be the name of the pod when deployed to Kubernetes. Note that the millId is
    // included in mill logs to help debugging.
    val millId = System.getenv("HOSTNAME")

    val mill =
      LiquidLegionsV2Mill(
        millId = millId,
        duchyId = flags.duchy.duchyName,
        keyStore = keyStore,
        consentSignalCert = csCertificate,
        dataClients = dataClients,
        systemComputationParticipantsClient = systemComputationParticipantsClient,
        systemComputationsClient = systemComputationsClient,
        systemComputationLogEntriesClient = systemComputationLogEntriesClient,
        computationStatsClient = computationStatsClient,
        workerStubs = computationControlClientMap,
        cryptoWorker = JniLiquidLegionsV2Encryption(),
        throttler = MinimumIntervalThrottler(Clock.systemUTC(), flags.pollingInterval),
        requestChunkSizeBytes = flags.requestChunkSizeBytes
      )

    runBlocking {
      // TODO: delete when an external keystore is used.
      keyStore.storePrivateKeyDer(
        CONSENT_SIGNALING_PRIVATE_KEY_ID,
        flags.csPrivateKeyDerFile.readBytes().toByteString()
      )
      mill.continuallyProcessComputationQueue()
    }
  }
}
