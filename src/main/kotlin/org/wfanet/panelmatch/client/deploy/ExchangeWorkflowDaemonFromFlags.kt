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

import java.time.Clock
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.panelmatch.client.common.Identity
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapper
import org.wfanet.panelmatch.client.launcher.ApiClient
import org.wfanet.panelmatch.client.launcher.GrpcApiClient
import org.wfanet.panelmatch.common.Timeout
import org.wfanet.panelmatch.common.asTimeout
import org.wfanet.panelmatch.common.certificates.V2AlphaCertificateManager
import picocli.CommandLine

/** Executes ExchangeWorkflows. */
abstract class ExchangeWorkflowDaemonFromFlags : ExchangeWorkflowDaemon() {
  @CommandLine.Mixin
  protected lateinit var flags: ExchangeWorkflowFlags
    private set

  @CommandLine.Mixin
  lateinit var blobSizeFlags: BlobSizeFlags
    private set

  override val clock: Clock = Clock.systemUTC()

  override val certificateManager: V2AlphaCertificateManager by lazy {
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

    val certificateService = CertificatesGrpcKt.CertificatesCoroutineStub(channel)

    V2AlphaCertificateManager(
      certificateService = certificateService,
      rootCerts = rootCertificates,
      privateKeys = privateKeys,
      algorithm = flags.certAlgorithm,
      certificateAuthority = certificateAuthority,
      localName = identity.toName()
    )
  }

  override val exchangeTaskMapper: ExchangeTaskMapper by lazy {
    ProductionExchangeTaskMapper(
      inputTaskThrottler = throttler,
      privateStorageSelector = privateStorageSelector,
      sharedStorageSelector = sharedStorageSelector,
      certificateManager = certificateManager
    )
  }

  override val apiClient: ApiClient by lazy {
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

    GrpcApiClient(identity, exchangeStepsClient, exchangeStepAttemptsClient, Clock.systemUTC())
  }

  override val throttler: Throttler by lazy {
    MinimumIntervalThrottler(Clock.systemUTC(), flags.pollingInterval)
  }

  override val taskTimeout: Timeout by lazy { flags.taskTimeout.asTimeout() }

  override val identity: Identity by lazy { Identity(flags.id, flags.partyType) }

  override val scope: CoroutineScope by lazy { CoroutineScope(Dispatchers.Default) }
}
