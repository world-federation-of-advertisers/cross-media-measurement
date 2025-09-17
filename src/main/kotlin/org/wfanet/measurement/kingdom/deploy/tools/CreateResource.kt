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
import java.security.cert.X509Certificate
import java.time.LocalDate
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.AccountKey
import org.wfanet.measurement.api.v2alpha.CanonicalRecurringExchangeKey
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DuchyCertificateKey
import org.wfanet.measurement.api.v2alpha.DuchyKey
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SignatureAlgorithm
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineStub
import org.wfanet.measurement.internal.kingdom.Certificate
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineStub
import org.wfanet.measurement.internal.kingdom.account
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.createMeasurementConsumerCreationTokenRequest
import org.wfanet.measurement.internal.kingdom.createRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.dataProviderDetails
import org.wfanet.measurement.internal.kingdom.modelProvider
import org.wfanet.measurement.internal.kingdom.recurringExchange
import org.wfanet.measurement.internal.kingdom.recurringExchangeDetails
import org.wfanet.measurement.kingdom.deploy.common.InternalApiFlags
import org.wfanet.measurement.kingdom.service.api.v2alpha.fillCertificateFromDer
import org.wfanet.measurement.kingdom.service.api.v2alpha.fillFromX509
import org.wfanet.measurement.kingdom.service.api.v2alpha.toInternal
import picocli.CommandLine.ArgGroup
import picocli.CommandLine.Command
import picocli.CommandLine.HelpCommand
import picocli.CommandLine.ITypeConverter
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option
import picocli.CommandLine.ParentCommand

private const val RECURRING_EXCHANGE_CRON_SCHEDULE = "@daily"

@Command(name = "account", description = ["Creates an Account"])
private class CreateAccountCommand : Runnable {
  @ParentCommand private lateinit var parent: CreateResource

  override fun run() {
    val internalAccountsClient = AccountsCoroutineStub(parent.channel)
    val internalAccount = runBlocking { internalAccountsClient.createAccount(account {}) }
    val accountName = AccountKey(externalIdToApiId(internalAccount.externalAccountId)).toName()
    val accountActivationToken = externalIdToApiId(internalAccount.activationToken)
    println("Account Name: $accountName")
    println("Activation Token: $accountActivationToken")
  }
}

@Command(name = "mc-creation-token", description = ["Creates a MeasurementConsumer Creation Token"])
private class CreateMcCreationTokenCommand : Runnable {
  @ParentCommand private lateinit var parent: CreateResource

  override fun run() {
    val internalAccountsClient = AccountsCoroutineStub(parent.channel)
    val mcCreationToken = runBlocking {
      externalIdToApiId(
        internalAccountsClient
          .createMeasurementConsumerCreationToken(createMeasurementConsumerCreationTokenRequest {})
          .measurementConsumerCreationToken
      )
    }
    println("Creation Token: $mcCreationToken")
  }
}

@Command(name = "data-provider", description = ["Creates a DataProvider"])
private class CreateDataProviderCommand : Runnable {
  @ParentCommand private lateinit var parent: CreateResource

  @Option(
    names = ["--certificate-der-file"],
    description = ["Certificate for the principal"],
    required = true,
  )
  private lateinit var certificateDerFile: File

  private val certificate: Certificate by lazy {
    certificate { fillCertificateFromDer(certificateDerFile.readBytes().toByteString()) }
  }

  @Option(
    names = ["--encryption-public-key-file"],
    description = ["Principal's serialized EncryptionPublicKey"],
    required = true,
  )
  private lateinit var encryptionPublicKeyFile: File

  private val serializedEncryptionPublicKey: ByteString by lazy {
    encryptionPublicKeyFile.readBytes().toByteString()
  }

  @Option(
    names = ["--encryption-public-key-api-version"],
    description = ["API version of the EncryptionPublicKey message"],
    defaultValue = "v2alpha",
    converter = [VersionConverter::class],
  )
  private lateinit var publicKeyApiVersion: Version

  @Option(
    names = ["--encryption-public-key-signature-file"],
    description = ["Principal's signature of serialized EncryptionPublicKey"],
    required = true,
  )
  private lateinit var encryptionPublicKeySignatureFile: File

  private val encryptionPublicKeySignature: ByteString by lazy {
    encryptionPublicKeySignatureFile.readBytes().toByteString()
  }

  @Option(
    names = ["--encryption-public-key-signature-algorithm"],
    description = ["Algorithm for EncryptionPublicKey signature"],
    required = true,
  )
  private lateinit var signatureAlgorithm: SignatureAlgorithm

  @Option(
    names = ["--required-duchies"],
    description =
      [
        "The set of duchies externals IDS that must be included in all computations involving this DataProvider"
      ],
    required = false,
  )
  private var requiredDuchies: List<String> = emptyList()

  override fun run() {
    // Verify that serialized message can be parsed to an EncryptionPublicKey message.
    when (publicKeyApiVersion) {
      Version.V2_ALPHA -> EncryptionPublicKey.parseFrom(serializedEncryptionPublicKey)
    }

    val dataProvider = dataProvider {
      certificate = this@CreateDataProviderCommand.certificate

      details = dataProviderDetails {
        apiVersion = publicKeyApiVersion.string
        publicKey = serializedEncryptionPublicKey
        publicKeySignature = encryptionPublicKeySignature
        publicKeySignatureAlgorithmOid = signatureAlgorithm.oid
      }

      requiredExternalDuchyIds += requiredDuchies
    }

    val dataProvidersStub = DataProvidersCoroutineStub(parent.channel)
    val outputDataProvider =
      runBlocking(Dispatchers.IO) { dataProvidersStub.createDataProvider(dataProvider) }

    val apiId = externalIdToApiId(outputDataProvider.externalDataProviderId)
    val dataProviderName = DataProviderKey(apiId).toName()
    val certificateName =
      DataProviderCertificateKey(
          apiId,
          externalIdToApiId(outputDataProvider.certificate.externalCertificateId),
        )
        .toName()
    println("DataProvider name: $dataProviderName")
    println("Certificate name: $certificateName")
  }
}

@Command(name = "model-provider", description = ["Creates a ModelProvider"])
private class CreateModelProviderCommand : Runnable {
  @ParentCommand private lateinit var parent: CreateResource

  override fun run() {
    val modelProvidersStub = ModelProvidersCoroutineStub(parent.channel)
    val outputModelProvider =
      runBlocking(Dispatchers.IO) { modelProvidersStub.createModelProvider(modelProvider {}) }

    val apiId = externalIdToApiId(outputModelProvider.externalModelProviderId)
    val modelProviderName = ModelProviderKey(apiId).toName()
    println("Model Provider Name: $modelProviderName")
  }
}

@Command(name = "duchy-certificate", description = ["Creates a Certificate for a Duchy"])
private class CreateDuchyCertificateCommand : Runnable {
  @ParentCommand private lateinit var parent: CreateResource

  @ArgGroup(exclusive = true, multiplicity = "1") private lateinit var duchy: Duchy

  @Option(
    names = ["--certificate-file", "--cert-file"],
    description = ["Filesystem path to X.509 certificate in PEM or DER format"],
    required = true,
  )
  private lateinit var certificateFile: File

  private val duchyId: String
    get() {
      if (duchy.duchyId != null) {
        return duchy.duchyId!!
      }

      val duchyKey = DuchyKey.fromName(duchy.duchyName!!) ?: error("Invalid resource name")
      return duchyKey.duchyId
    }

  override fun run() {
    val x509Certificate: X509Certificate = certificateFile.inputStream().use { readCertificate(it) }
    val certificatesStub = CertificatesCoroutineStub(parent.channel)

    val certificate =
      runBlocking(Dispatchers.IO) {
        certificatesStub.createCertificate(
          certificate {
            externalDuchyId = duchyId
            fillFromX509(x509Certificate)
          }
        )
      }

    val certificateKey =
      DuchyCertificateKey(
        certificate.externalDuchyId,
        externalIdToApiId(certificate.externalCertificateId),
      )
    println("Certificate name: ${certificateKey.toName()}")
  }

  class Duchy {
    @Option(names = ["--duchy"], description = ["API resource name of the Duchy"], required = true)
    var duchyName: String? = null
      private set

    @Option(
      names = ["--duchy-id"],
      description = ["ID of the Duchy in the public API"],
      required = true,
    )
    var duchyId: String? = null
      private set
  }
}

@Command(name = "recurring-exchange", description = ["Creates a RecurringExchange"])
private class CreateRecurringExchangeCommand : Runnable {
  @ParentCommand private lateinit var parent: CreateResource

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

  @Option(names = ["--next-exchange-date"], description = ["Next exchange date"], required = true)
  private lateinit var nextExchangeDate: LocalDate

  @Option(
    names = ["--exchange-workflow-file"],
    description = ["Public API serialized ExchangeWorkflow"],
    required = true,
  )
  private lateinit var exchangeWorkflowFile: File

  override fun run() {
    val modelProviderKey = requireNotNull(ModelProviderKey.fromName(modelProviderName))
    val dataProviderKey = requireNotNull(DataProviderKey.fromName(dataProviderName))

    val serializedExchangeWorkflow = exchangeWorkflowFile.readBytes()
    val v2AlphaExchangeWorkflow = ExchangeWorkflow.parseFrom(serializedExchangeWorkflow)

    val recurringExchange = recurringExchange {
      externalModelProviderId = apiIdToExternalId(modelProviderKey.modelProviderId)
      externalDataProviderId = apiIdToExternalId(dataProviderKey.dataProviderId)
      state = RecurringExchange.State.ACTIVE
      nextExchangeDate = this@CreateRecurringExchangeCommand.nextExchangeDate.toProtoDate()
      details = recurringExchangeDetails {
        apiVersion = Version.V2_ALPHA.string
        this.externalExchangeWorkflow = serializedExchangeWorkflow.toByteString()
        exchangeWorkflow = v2AlphaExchangeWorkflow.toInternal()
        cronSchedule = RECURRING_EXCHANGE_CRON_SCHEDULE
      }
    }

    val recurringExchangesStub = RecurringExchangesCoroutineStub(parent.channel)

    val outputRecurringExchange =
      runBlocking(Dispatchers.IO) {
        recurringExchangesStub.createRecurringExchange(
          createRecurringExchangeRequest { this.recurringExchange = recurringExchange }
        )
      }

    val apiId = externalIdToApiId(outputRecurringExchange.externalRecurringExchangeId)
    println(CanonicalRecurringExchangeKey(apiId).toName())
  }
}

@Command(
  name = "CreateResource",
  description = ["Creates resources in the Kingdom"],
  sortOptions = false,
  subcommands =
    [
      HelpCommand::class,
      CreateAccountCommand::class,
      CreateMcCreationTokenCommand::class,
      CreateDataProviderCommand::class,
      CreateModelProviderCommand::class,
      CreateRecurringExchangeCommand::class,
      CreateDuchyCertificateCommand::class,
    ],
)
private class CreateResource : Runnable {
  @Mixin private lateinit var tlsFlags: TlsFlags
  @Mixin private lateinit var internalApiFlags: InternalApiFlags

  val channel: Channel by lazy {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = tlsFlags.certFile,
        privateKeyFile = tlsFlags.privateKeyFile,
        trustedCertCollectionFile = tlsFlags.certCollectionFile,
      )

    buildMutualTlsChannel(internalApiFlags.target, clientCerts, internalApiFlags.certHost)
      .withVerboseLogging(true)
  }

  override fun run() {
    // No-op. Everything happens in subcommands.
  }
}

private class VersionConverter : ITypeConverter<Version> {
  override fun convert(value: String): Version {
    return Version.fromString(value)
  }
}

/**
 * Creates resources within the Kingdom.
 *
 * Use the `help` command to see usage details.
 */
fun main(args: Array<String>) = commandLineMain(CreateResource(), args)
