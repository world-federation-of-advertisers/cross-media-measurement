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

import io.grpc.Channel
import java.time.Clock
import org.apache.beam.sdk.options.PipelineOptions
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.common.crypto.SignatureAlgorithm
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.panelmatch.client.common.Identity
import org.wfanet.panelmatch.client.common.TaskParameters
import org.wfanet.panelmatch.client.eventpreprocessing.PreprocessingParameters
import org.wfanet.panelmatch.client.exchangetasks.ExchangeTaskMapper
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Party
import org.wfanet.panelmatch.client.launcher.ApiClient
import org.wfanet.panelmatch.client.launcher.GrpcApiClient
import org.wfanet.panelmatch.client.launcher.KingdomlessApiClient
import org.wfanet.panelmatch.client.launcher.withMaxParallelClaimedExchangeSteps
import org.wfanet.panelmatch.common.Timeout
import org.wfanet.panelmatch.common.asTimeout
import org.wfanet.panelmatch.common.certificates.CertificateAuthority
import org.wfanet.panelmatch.common.certificates.CertificateManager
import org.wfanet.panelmatch.common.certificates.KingdomlessCertificateManager
import org.wfanet.panelmatch.common.certificates.V2AlphaCertificateManager
import org.wfanet.panelmatch.common.loggerFor
import org.wfanet.panelmatch.common.secrets.MutableSecretMap
import picocli.CommandLine.Mixin

/** Executes ExchangeWorkflows. */
abstract class ExchangeWorkflowDaemonFromFlags : ExchangeWorkflowDaemon() {
  @Mixin protected lateinit var flags: ExchangeWorkflowFlags

  override val clock: Clock = Clock.systemUTC()

  /**
   * Limits the maximum number of workflow tasks (across all recurring exchanges) that the daemon
   * will be allowed to run concurrently. If not set, there is no limit. If set, must be >= 1.
   */
  override val maxParallelClaimedExchangeSteps: Int?
    get() = flags.maxParallelClaimedExchangeSteps

  /**
   * Maps Exchange paths (i.e. recurring_exchanges/{recurring_exchange_id}/exchanges/{date}) to
   * serialized SigningKeys protos.
   */
  protected abstract val privateKeys: MutableSecretMap

  /** Apache Beam options. */
  protected abstract fun makePipelineOptions(): PipelineOptions

  /** [CertificateAuthority] for use in [V2AlphaCertificateManager]. */
  protected abstract val certificateAuthority: CertificateAuthority

  private val clientCerts: SigningCerts by lazy {
    SigningCerts.fromPemFiles(
      certificateFile = flags.tlsFlags.certFile,
      privateKeyFile = flags.tlsFlags.privateKeyFile,
      trustedCertCollectionFile = flags.tlsFlags.certCollectionFile,
    )
  }

  private val channel: Channel by lazy {
    logger.info("Connecting to Kingdom")
    val kingdomBasedExchangeFlags = checkNotNull(flags.protocolOptions.kingdomBasedExchangeFlags)
    buildMutualTlsChannel(
        kingdomBasedExchangeFlags.exchangeApiTarget,
        clientCerts,
        kingdomBasedExchangeFlags.exchangeApiCertHost,
      )
      .withShutdownTimeout(kingdomBasedExchangeFlags.channelShutdownTimeout)
      .withVerboseLogging(kingdomBasedExchangeFlags.debugVerboseGrpcClientLogging)
  }

  override val certificateManager: CertificateManager by lazy {
    when (flags.protocolOptions.protocolSpecificFlags) {
      is KingdomBasedExchangeFlags -> createKingdomBasedCertificateManager()
      is KingdomlessExchangeFlags -> createKingdomlessCertificateManager()
    }
  }

  override val throttler: Throttler by lazy { createThrottler() }

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
    val inputTaskThrottler = createThrottler()
    ProductionExchangeTaskMapper(
      inputTaskThrottler = inputTaskThrottler,
      privateStorageSelector = privateStorageSelector,
      sharedStorageSelector = sharedStorageSelector,
      certificateManager = certificateManager,
      makePipelineOptions = ::makePipelineOptions,
      taskContext = taskContext,
    )
  }

  override val apiClient: ApiClient by lazy {
    val client =
      when (val protocolFlags = flags.protocolOptions.protocolSpecificFlags) {
        is KingdomBasedExchangeFlags -> createKingdomBasedApiClient()
        is KingdomlessExchangeFlags -> createKingdomlessApiClient(protocolFlags)
      }
    if (maxParallelClaimedExchangeSteps != null) {
      client.withMaxParallelClaimedExchangeSteps(maxParallelClaimedExchangeSteps!!)
    } else {
      client
    }
  }

  override val taskTimeout: Timeout by lazy { flags.taskTimeout.asTimeout() }

  override val identity: Identity by lazy { Identity(flags.id, flags.partyType) }

  override val runMode: RunMode by lazy { flags.runMode }

  private fun createThrottler(): Throttler = MinimumIntervalThrottler(clock, flags.pollingInterval)

  private fun createKingdomBasedCertificateManager(): CertificateManager {
    val certificateService = CertificatesCoroutineStub(channel)
    val resourceName =
      when (identity.party) {
        Party.DATA_PROVIDER -> DataProviderKey(identity.id).toName()
        Party.MODEL_PROVIDER -> ModelProviderKey(identity.id).toName()
        Party.PARTY_UNSPECIFIED,
        Party.UNRECOGNIZED -> error("Invalid Identity: $identity")
      }
    return V2AlphaCertificateManager(
      certificateService = certificateService,
      rootCerts = rootCertificates,
      privateKeys = privateKeys,
      algorithm = flags.certAlgorithm,
      certificateAuthority = certificateAuthority,
      localName = resourceName,
      fallbackPrivateKeyBlobKey = flags.fallbackPrivateKeyBlobKey,
    )
  }

  private fun createKingdomBasedApiClient(): ApiClient {
    val exchangeStepsClient = ExchangeStepsCoroutineStub(channel)
    val exchangeStepAttemptsClient = ExchangeStepAttemptsCoroutineStub(channel)
    return GrpcApiClient(
      identity = identity,
      exchangeStepsClient = exchangeStepsClient,
      exchangeStepAttemptsClient = exchangeStepAttemptsClient,
      clock = Clock.systemUTC(),
    )
  }

  private fun createKingdomlessCertificateManager(): CertificateManager {
    return KingdomlessCertificateManager(
      identity = identity,
      validExchangeWorkflows = validExchangeWorkflows,
      rootCerts = rootCertificates,
      privateKeys = privateKeys,
      algorithm = flags.certAlgorithm,
      certificateAuthority = certificateAuthority,
      fallbackPrivateKeyBlobKey = flags.fallbackPrivateKeyBlobKey,
    ) {
      sharedStorageSelector.getStorageClient(it)
    }
  }

  private fun createKingdomlessApiClient(
    kingdomlessExchangeFlags: KingdomlessExchangeFlags
  ): ApiClient {
    val signatureAlgorithm =
      requireNotNull(
        SignatureAlgorithm.fromJavaName(kingdomlessExchangeFlags.checkpointSigningAlgorithm)
      ) {
        "Invalid signing algorithm: ${kingdomlessExchangeFlags.checkpointSigningAlgorithm}"
      }
    return KingdomlessApiClient(
      identity = identity,
      recurringExchangeIds = kingdomlessExchangeFlags.kingdomlessRecurringExchangeIds,
      validExchangeWorkflows = validExchangeWorkflows,
      certificateManager = certificateManager,
      algorithm = signatureAlgorithm,
      lookbackWindow = kingdomlessExchangeFlags.lookbackWindow,
      stepTimeout = flags.taskTimeout,
      clock = Clock.systemUTC(),
    ) {
      sharedStorageSelector.getStorageClient(it)
    }
  }

  companion object {
    @JvmStatic protected val logger by loggerFor()
  }
}
