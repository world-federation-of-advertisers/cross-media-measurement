// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.common.daemon.mill.shareshuffle

import com.google.crypto.tink.Aead
import com.google.crypto.tink.BinaryKeysetReader
import com.google.crypto.tink.CleartextKeysetHandle
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.aead.AeadConfig
import com.google.protobuf.ByteString
import io.grpc.Channel
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.OpenTelemetry
import java.time.Clock
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.tink.TinkKeyStorageProvider
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.duchy.daemon.mill.Certificate
import org.wfanet.measurement.duchy.daemon.mill.shareshuffle.HonestMajorityShareShuffleMill
import org.wfanet.measurement.duchy.daemon.mill.shareshuffle.crypto.JniHonestMajorityShareShuffleCryptor
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.storage.TinkKeyStore
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.config.ProtocolsSetupConfig
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub as SystemComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as SystemComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineStub as SystemComputationsCoroutineStub
import picocli.CommandLine

abstract class HonestMajorityShareShuffleMillDaemon : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: HonestMajorityShareShuffleMillFlags
    private set

  protected fun run(storageClient: StorageClient) {
    DuchyInfo.initializeFromFlags(flags.duchyInfoFlags)
    val duchyName = flags.duchy.duchyName

    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = flags.tlsFlags.certFile,
        privateKeyFile = flags.tlsFlags.privateKeyFile,
        trustedCertCollectionFile = flags.tlsFlags.certCollectionFile,
      )

    val computationsServiceChannel: Channel =
      buildMutualTlsChannel(
          flags.computationsServiceFlags.target,
          clientCerts,
          flags.computationsServiceFlags.certHost,
        )
        .withShutdownTimeout(flags.channelShutdownTimeout)
        .withDefaultDeadline(flags.computationsServiceFlags.defaultDeadlineDuration)
    val dataClients =
      ComputationDataClients(
        ComputationsCoroutineStub(computationsServiceChannel).withDuchyId(duchyName),
        storageClient,
      )

    val computationControlClientMap =
      DuchyInfo.entries
        .filterKeys { it != duchyName }
        .mapValues { (duchyId, entry) ->
          ComputationControlCoroutineStub(
              buildMutualTlsChannel(
                  flags.computationControlServiceTargets.getValue(duchyId),
                  clientCerts,
                  entry.computationControlServiceCertHost,
                )
                .withShutdownTimeout(flags.channelShutdownTimeout)
            )
            .withDuchyId(duchyName)
        }

    val systemApiChannel =
      buildMutualTlsChannel(flags.systemApiFlags.target, clientCerts, flags.systemApiFlags.certHost)
        .withShutdownTimeout(flags.channelShutdownTimeout)

    val publicApiChannel =
      buildMutualTlsChannel(flags.publicApiFlags.target, clientCerts, flags.publicApiFlags.certHost)
        .withShutdownTimeout(flags.channelShutdownTimeout)

    val systemComputationsClient =
      SystemComputationsCoroutineStub(systemApiChannel).withDuchyId(duchyName)
    val systemComputationParticipantsClient =
      SystemComputationParticipantsCoroutineStub(systemApiChannel).withDuchyId(duchyName)
    val systemComputationLogEntriesClient =
      SystemComputationLogEntriesCoroutineStub(systemApiChannel).withDuchyId(duchyName)

    val publicCertificatesClient =
      CertificatesCoroutineStub(publicApiChannel).withDuchyId(duchyName)

    val computationStatsClient = ComputationStatsCoroutineStub(computationsServiceChannel)

    val csX509Certificate =
      flags.csCertificateDerFile.inputStream().use { input -> readCertificate(input) }
    val csCertificate = Certificate(flags.csCertificateName, csX509Certificate)
    // TODO(@renjiez): Read from a KMS-encrypted store instead.
    val csSigningKey =
      SigningKeyHandle(
        csX509Certificate,
        flags.csPrivateKeyDerFile.inputStream().use { input ->
          readPrivateKey(ByteString.readFrom(input), csX509Certificate.publicKey.algorithm)
        },
      )

    // This will be the name of the pod when deployed to Kubernetes. Note that the millId is
    // included in mill logs to help debugging.
    // TODO(@renjiez): add the parameter to override the millId.
    val millId = System.getenv("HOSTNAME")

    val privateKeyStore =
      flags.keyEncryptionKeyTinkFile?.let { file ->
        val keyUri = FakeKmsClient.KEY_URI_PREFIX + "kek"

        val keysetHandle: KeysetHandle =
          file.inputStream().use { input ->
            CleartextKeysetHandle.read(BinaryKeysetReader.withInputStream(input))
          }
        AeadConfig.register()
        val aead = keysetHandle.getPrimitive(Aead::class.java)
        val fakeKmsClient = FakeKmsClient().also { it.setAead(keyUri, aead) }
        TinkKeyStorageProvider(fakeKmsClient)
          .makeKmsPrivateKeyStore(TinkKeyStore(storageClient), keyUri)
      }

    // OpenTelemetry is usually enabled using the Java agent, in which case this will grab the
    // shared instance.
    val openTelemetry: OpenTelemetry = GlobalOpenTelemetry.get()

    val mill =
      HonestMajorityShareShuffleMill(
        millId = millId,
        duchyId = flags.duchy.duchyName,
        signingKey = csSigningKey,
        consentSignalCert = csCertificate,
        trustedCertificates = flags.tlsFlags.signingCerts.trustedCertificates,
        dataClients = dataClients,
        systemComputationParticipantsClient = systemComputationParticipantsClient,
        systemComputationsClient = systemComputationsClient,
        systemComputationLogEntriesClient = systemComputationLogEntriesClient,
        computationStatsClient = computationStatsClient,
        privateKeyStore = privateKeyStore,
        certificateClient = publicCertificatesClient,
        workerStubs = computationControlClientMap,
        cryptoWorker = JniHonestMajorityShareShuffleCryptor(),
        protocolSetupConfig =
          flags.protocolsSetupConfig.reader().use {
            parseTextProto(it, ProtocolsSetupConfig.getDefaultInstance()).honestMajorityShareShuffle
          },
        workLockDuration = flags.workLockDuration,
        openTelemetry = openTelemetry,
        requestChunkSizeBytes = flags.requestChunkSizeBytes,
      )

    runBlocking {
      withContext(CoroutineName("Mill $millId")) {
        val throttler = MinimumIntervalThrottler(Clock.systemUTC(), flags.pollingInterval)
        throttler.loopOnReady {
          logAndSuppressExceptionSuspend { mill.pollAndProcessNextComputation() }
        }
      }
    }
  }
}
