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

package org.wfanet.measurement.duchy.deploy.common.daemon.herald

import com.google.crypto.tink.Aead
import com.google.crypto.tink.BinaryKeysetReader
import com.google.crypto.tink.CleartextKeysetHandle
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.aead.AeadConfig
import io.grpc.Channel
import java.io.File
import java.time.Clock
import java.time.Duration
import kotlin.properties.Delegates
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.tink.TinkKeyStorageProvider
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.duchy.deploy.common.CommonDuchyFlags
import org.wfanet.measurement.duchy.deploy.common.ComputationsServiceFlags
import org.wfanet.measurement.duchy.deploy.common.SystemApiFlags
import org.wfanet.measurement.duchy.herald.ContinuationTokenManager
import org.wfanet.measurement.duchy.herald.Herald
import org.wfanet.measurement.duchy.storage.TinkKeyStore
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineStub
import org.wfanet.measurement.internal.duchy.config.ProtocolsSetupConfig
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.Computation
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as SystemComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineStub as SystemComputationsCoroutineStub
import picocli.CommandLine

class HeraldFlags {
  @CommandLine.Mixin
  lateinit var duchy: CommonDuchyFlags
    private set

  @CommandLine.Mixin
  lateinit var tlsFlags: TlsFlags
    private set

  @CommandLine.Option(
    names = ["--channel-shutdown-timeout"],
    defaultValue = "3s",
    description = ["How long to allow for the gRPC channel to shutdown."],
  )
  lateinit var channelShutdownTimeout: Duration
    private set

  @CommandLine.Mixin
  lateinit var systemApiFlags: SystemApiFlags
    private set

  @CommandLine.Mixin
  lateinit var computationsServiceFlags: ComputationsServiceFlags
    private set

  @CommandLine.Option(
    names = ["--protocols-setup-config"],
    description = ["ProtocolsSetupConfig proto message in text format."],
    required = true,
  )
  lateinit var protocolsSetupConfig: File
    private set

  @CommandLine.Option(
    names = ["--key-encryption-key-file"],
    description = ["The key encryption key file (binary format) used for private key store."],
  )
  var keyEncryptionKeyTinkFile: File? = null
    private set

  @CommandLine.Option(
    names = ["--deletable-computation-state"],
    description =
      [
        "Terminal State (SUCCEEDED, FAILED or CANCELLED) in which the Computation can be " +
          "deleted. This can be specified multiple times."
      ],
    required = false,
  )
  var deletableComputationStates: Set<Computation.State> = emptySet()
    private set

  @set:CommandLine.Option(
    names = ["--debug-verbose-grpc-client-logging"],
    description = ["Enables full gRPC request and response logging for outgoing gRPCs"],
    defaultValue = "false",
  )
  var verboseGrpcClientLogging by Delegates.notNull<Boolean>()
    private set
}

abstract class HeraldDaemon : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: HeraldFlags
    private set

  @CommandLine.Command(
    name = "HeraldDaemon",
    mixinStandardHelpOptions = true,
    showDefaultValues = true,
  )
  protected fun run(storageClient: StorageClient) {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = flags.tlsFlags.certFile,
        privateKeyFile = flags.tlsFlags.privateKeyFile,
        trustedCertCollectionFile = flags.tlsFlags.certCollectionFile,
      )

    val systemServiceChannel =
      buildMutualTlsChannel(
          flags.systemApiFlags.target,
          clientCerts,
          flags.systemApiFlags.certHost,
          Herald.SERVICE_CONFIG,
        )
        .withShutdownTimeout(flags.channelShutdownTimeout)
        .withVerboseLogging(flags.verboseGrpcClientLogging)
    val systemComputationsClient =
      SystemComputationsCoroutineStub(systemServiceChannel).withDuchyId(flags.duchy.duchyName)
    val systemComputationParticipantsClient =
      SystemComputationParticipantsCoroutineStub(systemServiceChannel)
        .withDuchyId(flags.duchy.duchyName)

    val internalComputationsChannel: Channel =
      buildMutualTlsChannel(
          flags.computationsServiceFlags.target,
          clientCerts,
          flags.computationsServiceFlags.certHost,
          Herald.SERVICE_CONFIG,
        )
        .withShutdownTimeout(flags.channelShutdownTimeout)
        .withDefaultDeadline(flags.computationsServiceFlags.defaultDeadlineDuration)
        .withVerboseLogging(flags.verboseGrpcClientLogging)
    val internalComputationsClient = ComputationsCoroutineStub(internalComputationsChannel)

    val continuationTokenClient = ContinuationTokensCoroutineStub(internalComputationsChannel)
    val continuationTokenManager = ContinuationTokenManager(continuationTokenClient)
    // This will be the name of the pod when deployed to Kubernetes.
    val heraldId = System.getenv("HOSTNAME")

    // TODO(@renjiez): Read from a KMS-encrypted store instead.
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

    val herald =
      Herald(
        heraldId = heraldId,
        duchyId = flags.duchy.duchyName,
        internalComputationsClient = internalComputationsClient,
        systemComputationsClient = systemComputationsClient,
        systemComputationParticipantClient = systemComputationParticipantsClient,
        privateKeyStore = privateKeyStore,
        continuationTokenManager = continuationTokenManager,
        protocolsSetupConfig =
          flags.protocolsSetupConfig.reader().use {
            parseTextProto(it, ProtocolsSetupConfig.getDefaultInstance())
          },
        clock = Clock.systemUTC(),
        deletableComputationStates = flags.deletableComputationStates,
      )
    runBlocking { herald.continuallySyncStatuses() }
  }
}
