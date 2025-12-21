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

package org.wfanet.measurement.duchy.deploy.common.job.mill.liquidlegionsv2

import com.google.protobuf.ByteString
import io.grpc.Channel
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.duchy.mill.Certificate
import org.wfanet.measurement.duchy.mill.MillBase
import org.wfanet.measurement.duchy.mill.liquidlegionsv2.LiquidLegionsV2Mill
import org.wfanet.measurement.duchy.mill.liquidlegionsv2.ReachFrequencyLiquidLegionsV2Mill
import org.wfanet.measurement.duchy.mill.liquidlegionsv2.ReachOnlyLiquidLegionsV2Mill
import org.wfanet.measurement.duchy.mill.liquidlegionsv2.crypto.JniLiquidLegionsV2Encryption
import org.wfanet.measurement.duchy.mill.liquidlegionsv2.crypto.JniReachOnlyLiquidLegionsV2Encryption
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub as SystemComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as SystemComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineStub as SystemComputationsCoroutineStub
import picocli.CommandLine

/** Job which processes claimed work using a Liquid Legions v2 protocol. */
abstract class LiquidLegionsV2MillJob : Runnable {
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
        trustedCertCollectionFile = flags.tlsFlags.certCollectionFile,
      )

    val computationsServiceChannel: Channel =
      buildMutualTlsChannel(
          flags.computationsServiceFlags.target,
          clientCerts,
          flags.computationsServiceFlags.certHost,
          MillBase.SERVICE_CONFIG,
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
      buildMutualTlsChannel(
          flags.systemApiFlags.target,
          clientCerts,
          flags.systemApiFlags.certHost,
          MillBase.SERVICE_CONFIG,
        )
        .withShutdownTimeout(flags.channelShutdownTimeout)

    val systemComputationsClient =
      SystemComputationsCoroutineStub(systemApiChannel).withDuchyId(duchyName)
    val systemComputationParticipantsClient =
      SystemComputationParticipantsCoroutineStub(systemApiChannel).withDuchyId(duchyName)
    val systemComputationLogEntriesClient =
      SystemComputationLogEntriesCoroutineStub(systemApiChannel).withDuchyId(duchyName)

    val computationStatsClient = ComputationStatsCoroutineStub(computationsServiceChannel)

    val csX509Certificate =
      flags.csCertificateDerFile.inputStream().use { input -> readCertificate(input) }
    val csCertificate = Certificate(flags.csCertificateName, csX509Certificate)
    // TODO: Read from a KMS-encrypted store instead.
    val csSigningKey =
      SigningKeyHandle(
        csX509Certificate,
        flags.csPrivateKeyDerFile.inputStream().use { input ->
          readPrivateKey(ByteString.readFrom(input), csX509Certificate.publicKey.algorithm)
        },
      )

    // This will be the name of the pod when deployed to Kubernetes. Note that the millId is
    // included in mill logs to help debugging.
    val millId = flags.millId
    val claimedComputationFlags = flags.claimedComputationFlags

    val mill: LiquidLegionsV2Mill =
      when (claimedComputationFlags.claimedComputationType) {
        ComputationTypeEnum.ComputationType.LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
          ReachFrequencyLiquidLegionsV2Mill(
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
            workerStubs = computationControlClientMap,
            cryptoWorker = JniLiquidLegionsV2Encryption(),
            workLockDuration = flags.workLockDuration,
            requestChunkSizeBytes = flags.requestChunkSizeBytes,
            parallelism = flags.parallelism,
          )
        ComputationTypeEnum.ComputationType.REACH_ONLY_LIQUID_LEGIONS_SKETCH_AGGREGATION_V2 ->
          ReachOnlyLiquidLegionsV2Mill(
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
            workerStubs = computationControlClientMap,
            cryptoWorker = JniReachOnlyLiquidLegionsV2Encryption(),
            workLockDuration = flags.workLockDuration,
            requestChunkSizeBytes = flags.requestChunkSizeBytes,
            parallelism = flags.parallelism,
          )
        ComputationTypeEnum.ComputationType.HONEST_MAJORITY_SHARE_SHUFFLE,
        ComputationTypeEnum.ComputationType.TRUS_TEE,
        ComputationTypeEnum.ComputationType.UNSPECIFIED,
        ComputationTypeEnum.ComputationType.UNRECOGNIZED ->
          error("Unsupported ComputationType ${claimedComputationFlags.claimedComputationType}")
      }

    runBlocking(CoroutineName("Mill $millId")) {
      mill.processClaimedWork(
        claimedComputationFlags.claimedGlobalComputationId,
        claimedComputationFlags.claimedComputationVersion,
      )

      // Continue processing until work is exhausted.
      do {
        val workExhausted = !mill.claimAndProcessWork()
      } while (isActive && !workExhausted)
    }
  }
}
