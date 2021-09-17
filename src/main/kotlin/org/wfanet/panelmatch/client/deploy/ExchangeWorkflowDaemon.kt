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

package org.wfanet.panelmatch.client.deploy

import java.security.cert.X509Certificate
import java.time.Clock
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapperForJoinKeyExchange
import org.wfanet.panelmatch.client.launcher.CoroutineLauncher
import org.wfanet.panelmatch.client.launcher.ExchangeStepLauncher
import org.wfanet.panelmatch.client.launcher.ExchangeStepValidatorImpl
import org.wfanet.panelmatch.client.launcher.ExchangeTaskExecutor
import org.wfanet.panelmatch.client.launcher.GrpcApiClient
import org.wfanet.panelmatch.client.launcher.Identity
import org.wfanet.panelmatch.client.storage.VerifiedStorageClient
import org.wfanet.panelmatch.common.asTimeout
import org.wfanet.panelmatch.protocol.common.JniDeterministicCommutativeCipher
import picocli.CommandLine

/** Executes ExchangeWorkflows. */
abstract class ExchangeWorkflowDaemon : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: ExchangeWorkflowFlags
    private set

  /** [VerifiedStorageClient] for payloads to be shared with the other party. */
  abstract val sharedStorage: VerifiedStorageClient

  /** [VerifiedStorageClient] for payloads that should NOT shared with the other party. */
  abstract val privateStorage: VerifiedStorageClient

  abstract val dataProviderCertificate: X509Certificate

  abstract val modelProviderCertificate: X509Certificate

  override fun run() {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = flags.tlsFlags.certFile,
        privateKeyFile = flags.tlsFlags.privateKeyFile,
        trustedCertCollectionFile = flags.tlsFlags.certCollectionFile
      )

    val channel =
      buildMutualTlsChannel(
          flags.exchangeApiTarget.toString(),
          clientCerts,
          flags.exchangeApiCertHost
        )
        .withShutdownTimeout(flags.channelShutdownTimeout)

    val exchangeStepsClient = ExchangeStepsCoroutineStub(channel)
    val exchangeStepAttemptsClient = ExchangeStepAttemptsCoroutineStub(channel)

    val grpcApiClient =
      GrpcApiClient(
        Identity(flags.id, flags.partyType),
        exchangeStepsClient,
        exchangeStepAttemptsClient,
        Clock.systemUTC()
      )

    val pollingThrottler = MinimumIntervalThrottler(Clock.systemUTC(), flags.pollingInterval)

    val exchangeTaskMapper =
      ExchangeTaskMapperForJoinKeyExchange(
        deterministicCommutativeCryptor = JniDeterministicCommutativeCipher(),
        sharedStorage = sharedStorage,
        privateStorage = privateStorage
      )

    val stepExecutor =
      ExchangeTaskExecutor(
        apiClient = grpcApiClient,
        timeout = flags.taskTimeout.asTimeout(),
        sharedStorage = sharedStorage,
        privateStorage = privateStorage,
        getExchangeTaskForStep = exchangeTaskMapper::getExchangeTaskForStep
      )

    val launcher = CoroutineLauncher(stepExecutor = stepExecutor)

    val exchangeStepLauncher =
      ExchangeStepLauncher(
        apiClient = grpcApiClient,
        validator =
          ExchangeStepValidatorImpl(
            flags.partyType,
            dataProviderCertificate,
            modelProviderCertificate
          ),
        jobLauncher = launcher
      )

    runBlocking {
      pollingThrottler.loopOnReady {
        logAndSuppressExceptionSuspend { exchangeStepLauncher.findAndRunExchangeStep() }
      }
    }
  }
}
