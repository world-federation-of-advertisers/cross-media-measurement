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
import org.apache.beam.sdk.options.PipelineOptions
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.panelmatch.client.common.Identity
import org.wfanet.panelmatch.client.common.TaskParameters
import org.wfanet.panelmatch.client.eventpreprocessing.PreprocessingParameters
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapper
import org.wfanet.panelmatch.client.launcher.ApiClient
import org.wfanet.panelmatch.client.launcher.GrpcApiClient
import org.wfanet.panelmatch.common.Timeout
import org.wfanet.panelmatch.common.asTimeout
import org.wfanet.panelmatch.common.certificates.CertificateAuthority
import org.wfanet.panelmatch.common.certificates.V2AlphaCertificateManager
import org.wfanet.panelmatch.common.secrets.MutableSecretMap
import picocli.CommandLine.Mixin

/** Executes ExchangeWorkflows. */
abstract class ExchangeWorkflowDaemonFromFlags : ExchangeWorkflowDaemon() {
  @Mixin private lateinit var flags: ExchangeWorkflowFlags

  override val clock: Clock = Clock.systemUTC()

  /**
   * Maps Exchange paths (i.e. recurring_exchanges/{recurring_exchange_id}/exchanges/{date}) to
   * serialized SigningKeys protos.
   */
  protected abstract val privateKeys: MutableSecretMap

  /** Apache Beam options. */
  protected abstract val pipelineOptions: PipelineOptions

  /** [CertificateAuthority] for use in [V2AlphaCertificateManager]. */
  protected abstract val certificateAuthority: CertificateAuthority

  override val certificateManager: V2AlphaCertificateManager by lazy {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = flags.tlsFlags.certFile,
        privateKeyFile = flags.tlsFlags.privateKeyFile,
        trustedCertCollectionFile = flags.tlsFlags.certCollectionFile
      )

    val channel =
      buildMutualTlsChannel(flags.exchangeApiTarget, clientCerts, flags.exchangeApiCertHost)
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

  override val throttler: Throttler by lazy {
    MinimumIntervalThrottler(Clock.systemUTC(), flags.pollingInterval)
  }

  private val taskContext: TaskParameters by lazy {
    TaskParameters(
      setOf(
        PreprocessingParameters(
          maxByteSize = flags.preProcessingMaxByteSize,
          fileCount = flags.preProcessingFileCount,
        )
      )
    )
  }

  override val exchangeTaskMapper: ExchangeTaskMapper by lazy {
    ProductionExchangeTaskMapper(
      inputTaskThrottler = throttler,
      privateStorageSelector = privateStorageSelector,
      sharedStorageSelector = sharedStorageSelector,
      certificateManager = certificateManager,
      pipelineOptions = pipelineOptions,
      taskContext = taskContext,
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
      buildMutualTlsChannel(flags.exchangeApiTarget, clientCerts, flags.exchangeApiCertHost)
        .withShutdownTimeout(flags.channelShutdownTimeout)

    val exchangeStepsClient = ExchangeStepsCoroutineStub(channel)

    val exchangeStepAttemptsClient = ExchangeStepAttemptsCoroutineStub(channel)

    GrpcApiClient(identity, exchangeStepsClient, exchangeStepAttemptsClient, Clock.systemUTC())
  }

  override val taskTimeout: Timeout by lazy { flags.taskTimeout.asTimeout() }

  override val identity: Identity by lazy { Identity(flags.id, flags.partyType) }
}
