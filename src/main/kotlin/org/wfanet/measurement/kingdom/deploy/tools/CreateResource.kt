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

import io.grpc.Channel
import java.io.File
import java.util.concurrent.Callable
import kotlin.system.exitProcess
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
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
import org.wfanet.measurement.common.toByteString
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.DataProviderKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelProviderKt
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineStub
import org.wfanet.measurement.internal.kingdom.certificate
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
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option

private const val API_VERSION = "v2alpha"

@Command(
  name = "create_resource",
  description = ["Creates resources in the Kingdom"],
)
class CreateResource : Callable<Int> {
  @Mixin private lateinit var internalApiFlags: InternalApiFlags
  @Mixin private lateinit var tlsFlags: TlsFlags

  @Option(names = ["--certificate-der-file"], required = false)
  private lateinit var certificateDerFile: File

  private val certificate: Certificate by lazy {
    certificate { fillCertificateFromDer(certificateDerFile.readBytes().toByteString()) }
  }

  @Option(names = ["--public-key-file"], required = false) private lateinit var publicKeyFile: File

  @Option(names = ["--public-key-signature-file"], required = false)
  private lateinit var publicKeySignatureFile: File

  private val channel: Channel by lazy {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = tlsFlags.certFile,
        privateKeyFile = tlsFlags.privateKeyFile,
        trustedCertCollectionFile = tlsFlags.certCollectionFile
      )

    buildMutualTlsChannel(internalApiFlags.target, clientCerts, internalApiFlags.certHost)
      .withVerboseLogging(true)
  }

  /** Return 0 for success -- all work happens in subcommands. */
  override fun call(): Int = 0

  @Command(name = "data_provider", description = ["Creates a DataProvider"])
  fun createDataProvider(): Int {
    val dataProvider = dataProvider {
      certificate = this@CreateResource.certificate

      details =
        DataProviderKt.details {
          apiVersion = API_VERSION
          publicKey = publicKeyFile.readBytes().toByteString()
          publicKeySignature = publicKeySignatureFile.readBytes().toByteString()
        }
    }

    val dataProvidersStub = DataProvidersCoroutineStub(channel)
    val outputDataProvider =
      runBlocking(Dispatchers.IO) { dataProvidersStub.createDataProvider(dataProvider) }

    val apiId = externalIdToApiId(outputDataProvider.externalDataProviderId)
    println(DataProviderKey(apiId).toName())

    return 0
  }

  @Command(name = "model_provider", description = ["Creates a ModelProvider"])
  fun createModelProvider(): Int {
    val modelProvider = modelProvider {
      certificate = this@CreateResource.certificate

      details =
        ModelProviderKt.details {
          apiVersion = API_VERSION
          publicKey = publicKeyFile.readBytes().toByteString()
          publicKeySignature = publicKeySignatureFile.readBytes().toByteString()
        }
    }

    val modelProvidersStub = ModelProvidersCoroutineStub(channel)
    val outputModelProvider =
      runBlocking(Dispatchers.IO) { modelProvidersStub.createModelProvider(modelProvider) }

    val apiId = externalIdToApiId(outputModelProvider.externalModelProviderId)
    println(ModelProviderKey(apiId).toName())

    return 0
  }

  @Command(name = "recurring_exchange", description = ["Creates a RecurringExchange"])
  fun createRecurringExchange(
    @Option(names = ["--model-provider"]) modelProviderName: String,
    @Option(names = ["--data-provider"]) dataProviderName: String,
    @Option(
      names = ["--exchange-workflow-file"],
      description = ["Public API serialized ExchangeWorkflow"]
    )
    exchangeWorkflowFile: File,
  ): Int {
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

    val recurringExchangesStub = RecurringExchangesCoroutineStub(channel)

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

fun main(args: Array<String>) {
  exitProcess(CommandLine(CreateResource()).execute(*args))
}
