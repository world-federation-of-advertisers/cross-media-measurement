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

package org.wfanet.measurement.kingdom.deploy.tools

import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import io.grpc.Channel
import java.io.File
import java.util.concurrent.Callable
import kotlin.system.exitProcess
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.AccountKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.RecurringExchangeKey
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineStub
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.DataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelProviderKt
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineStub
import org.wfanet.measurement.internal.kingdom.account
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.createMeasurementConsumerCreationTokenRequest
import org.wfanet.measurement.internal.kingdom.createRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.modelProvider
import org.wfanet.measurement.internal.kingdom.recurringExchange
import org.wfanet.measurement.internal.kingdom.recurringExchangeDetails
import org.wfanet.measurement.kingdom.deploy.common.InternalApiFlags
import org.wfanet.measurement.kingdom.service.api.v2alpha.fillCertificateFromDer
import org.wfanet.measurement.kingdom.service.api.v2alpha.toInternal
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.HelpCommand
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option

private const val API_VERSION = "v2alpha"

private class ApiFlags {
  @Mixin private lateinit var internalApiFlags: InternalApiFlags
  @Mixin private lateinit var tlsFlags: TlsFlags

  val channel: Channel by lazy {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = tlsFlags.certFile,
        privateKeyFile = tlsFlags.privateKeyFile,
        trustedCertCollectionFile = tlsFlags.certCollectionFile
      )

    buildMutualTlsChannel(internalApiFlags.target, clientCerts, internalApiFlags.certHost)
      .withVerboseLogging(true)
  }
}

private abstract class CreatePrincipalCommand : Callable<Int> {
  @Mixin protected lateinit var apiFlags: ApiFlags

  @Option(
    names = ["--certificate-der-file"],
    description = ["Certificate for the principal"],
    required = true
  )
  private lateinit var certificateDerFile: File

  protected val certificate: Certificate by lazy {
    certificate { fillCertificateFromDer(certificateDerFile.readBytes().toByteString()) }
  }

  @Option(
    names = ["--encryption-public-key-file"],
    description = ["Principal's serialized EncryptionPublicKey"],
    required = true
  )
  private lateinit var encryptionPublicKeyFile: File

  protected val serializedEncryptionPublicKey: ByteString by lazy {
    encryptionPublicKeyFile.readBytes().toByteString()
  }

  @Option(
    names = ["--encryption-public-key-signature-file"],
    description = ["Principal's signature of serialized EncryptionPublicKey"],
    required = true
  )
  private lateinit var encryptionPublicKeySignatureFile: File

  protected val encryptionPublicKeySignature: ByteString by lazy {
    encryptionPublicKeySignatureFile.readBytes().toByteString()
  }
}

@Command(name = "account", description = ["Creates an Account"])
private class CreateAccountCommand : CreatePrincipalCommand() {
  override fun call(): Int {
    val internalAccountsClient = AccountsCoroutineStub(apiFlags.channel)
    val internalAccount = runBlocking { internalAccountsClient.createAccount(account {}) }
    val accountName = AccountKey(externalIdToApiId(internalAccount.externalAccountId)).toName()
    val accountActivationToken = externalIdToApiId(internalAccount.activationToken)
    println("Account Name: $accountName")
    println("Activation Token: $accountActivationToken")

    return 0
  }
}

@Command(name = "mc_creation_token", description = ["Create a Measurement Consumer Creation Token"])
private class CreateMcCreationTokenCommand : Callable<Int> {
  @Mixin lateinit var apiFlags: ApiFlags

  override fun call(): Int {
    val internalAccountsClient = AccountsCoroutineStub(apiFlags.channel)
    val mcCreationToken = runBlocking {
      externalIdToApiId(
        internalAccountsClient.createMeasurementConsumerCreationToken(
            createMeasurementConsumerCreationTokenRequest {}
          )
          .measurementConsumerCreationToken
      )
    }
    println("Creation Token: $mcCreationToken")

    return 0
  }
}

@Command(name = "data_provider", description = ["Creates a DataProvider"])
private class CreateDataProviderCommand : CreatePrincipalCommand() {
  override fun call(): Int {
    val dataProvider = dataProvider {
      certificate = this@CreateDataProviderCommand.certificate

      details =
        DataProviderKt.details {
          apiVersion = API_VERSION
          publicKey = serializedEncryptionPublicKey
          publicKeySignature = this@CreateDataProviderCommand.encryptionPublicKeySignature
        }
    }

    val dataProvidersStub = DataProvidersCoroutineStub(apiFlags.channel)
    val outputDataProvider =
      runBlocking(Dispatchers.IO) { dataProvidersStub.createDataProvider(dataProvider) }

    val apiId = externalIdToApiId(outputDataProvider.externalDataProviderId)
    val dataProviderName = DataProviderKey(apiId).toName()
    println("Data Provider Name: $dataProviderName")

    return 0
  }
}

@Command(name = "model_provider", description = ["Creates a ModelProvider"])
private class CreateModelProviderCommand : CreatePrincipalCommand() {
  override fun call(): Int {
    val modelProvider = modelProvider {
      certificate = this@CreateModelProviderCommand.certificate

      details =
        ModelProviderKt.details {
          apiVersion = API_VERSION
          publicKey = serializedEncryptionPublicKey
          publicKeySignature = this@CreateModelProviderCommand.encryptionPublicKeySignature
        }
    }

    val modelProvidersStub = ModelProvidersCoroutineStub(apiFlags.channel)
    val outputModelProvider =
      runBlocking(Dispatchers.IO) { modelProvidersStub.createModelProvider(modelProvider) }

    val apiId = externalIdToApiId(outputModelProvider.externalModelProviderId)
    val modelProviderName = ModelProviderKey(apiId).toName()
    println("Model Provider Name: $modelProviderName")

    return 0
  }
}

@Command(name = "recurring_exchange", description = ["Creates a RecurringExchange"])
private class CreateRecurringExchangeCommand : Callable<Int> {
  @Option(
    names = ["--model-provider"],
    description = ["API resource name of the ModelProvider"],
    required = true,
  )
  private lateinit var modelProviderName: String

  @Option(
    names = ["--data-provider"],
    description = ["API resource name of the DataProvider"],
    required = true,
  )
  private lateinit var dataProviderName: String

  @Option(
    names = ["--exchange-workflow-file"],
    description = ["Public API serialized ExchangeWorkflow"],
    required = true,
  )
  private lateinit var exchangeWorkflowFile: File

  @Mixin private lateinit var apiFlags: ApiFlags

  override fun call(): Int {
    val modelProviderKey = requireNotNull(ModelProviderKey.fromName(modelProviderName))
    val dataProviderKey = requireNotNull(DataProviderKey.fromName(dataProviderName))

    val serializedExchangeWorkflow = exchangeWorkflowFile.readBytes()
    val v2AlphaExchangeWorkflow = ExchangeWorkflow.parseFrom(serializedExchangeWorkflow)

    val recurringExchange = recurringExchange {
      externalModelProviderId = apiIdToExternalId(modelProviderKey.modelProviderId)
      externalDataProviderId = apiIdToExternalId(dataProviderKey.dataProviderId)
      state = RecurringExchange.State.ACTIVE
      details =
        recurringExchangeDetails {
          this.externalExchangeWorkflow = serializedExchangeWorkflow.toByteString()
          exchangeWorkflow = v2AlphaExchangeWorkflow.toInternal()
        }
    }

    val recurringExchangesStub = RecurringExchangesCoroutineStub(apiFlags.channel)

    val outputRecurringExchange =
      runBlocking(Dispatchers.IO) {
        recurringExchangesStub.createRecurringExchange(
          createRecurringExchangeRequest { this.recurringExchange = recurringExchange }
        )
      }

    val apiId = externalIdToApiId(outputRecurringExchange.externalRecurringExchangeId)
    println(RecurringExchangeKey(apiId).toName())

    return 0
  }
}

@Command(
  name = "create_resource",
  description = ["Creates resources in the Kingdom"],
  subcommands =
    [
      HelpCommand::class,
      CreateAccountCommand::class,
      CreateMcCreationTokenCommand::class,
      CreateDataProviderCommand::class,
      CreateModelProviderCommand::class,
      CreateRecurringExchangeCommand::class,
    ]
)
class CreateResource : Callable<Int> {
  /** Return 0 for success -- all work happens in subcommands. */
  override fun call(): Int = 0
}

/**
 * Creates resources within the Kingdom.
 *
 * Use the `help` command to see usage details:
 *
 * ```
 * $ bazel build //src/main/kotlin/org/wfanet/measurement/kingdom/deploy/tools:create_resource
 * $ bazel-bin/src/main/kotlin/org/wfanet/measurement/kingdom/deploy/tools/create_resource help
 * Usage: create_resource [COMMAND]
 * Creates resources in the Kingdom
 * Commands:
 *  help                Displays help information about the specified command
 *  account             Creates an Account
 *  mc_creation_token   Creates a Measurement Consumer Creation Token
 *  data_provider       Creates a DataProvider
 *  model_provider      Creates a ModelProvider
 *  recurring_exchange  Creates a RecurringExchange
 * ```
 */
fun main(args: Array<String>) {
  exitProcess(CommandLine(CreateResource()).execute(*args))
}
