/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.api.v2alpha.tools

import com.google.crypto.tink.BinaryKeysetReader
import com.google.crypto.tink.CleartextKeysetHandle
import com.google.crypto.tink.KeysetHandle
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.ByteString
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.any
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.kotlin.unpack
import com.google.type.interval
import io.grpc.ManagedChannel
import java.io.File
import java.security.SecureRandom
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration as JavaDuration
import java.time.Instant
import java.time.LocalDate
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.Account
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ApiKey
import org.wfanet.measurement.api.v2alpha.ApiKeysGrpcKt.ApiKeysCoroutineStub
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.ListModelLinesRequestKt.filter
import org.wfanet.measurement.api.v2alpha.ListModelOutagesRequestKt
import org.wfanet.measurement.api.v2alpha.ListModelRolloutsRequestKt
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKt.DataProviderEntryKt as DataProviderEntries
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelOutage
import org.wfanet.measurement.api.v2alpha.ModelOutagesGrpcKt.ModelOutagesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRelease
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt.ModelReleasesCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelRollout
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelShard
import org.wfanet.measurement.api.v2alpha.ModelShardKt.modelBlob
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt.ModelShardsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelSuite
import org.wfanet.measurement.api.v2alpha.ModelSuitesGrpcKt.ModelSuitesCoroutineStub
import org.wfanet.measurement.api.v2alpha.PublicKey
import org.wfanet.measurement.api.v2alpha.PublicKeysGrpcKt.PublicKeysCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.EventGroupEntryKt as EventGroupEntries
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.activateAccountRequest
import org.wfanet.measurement.api.v2alpha.apiKey
import org.wfanet.measurement.api.v2alpha.authenticateRequest
import org.wfanet.measurement.api.v2alpha.cancelMeasurementRequest
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createApiKeyRequest
import org.wfanet.measurement.api.v2alpha.createCertificateRequest
import org.wfanet.measurement.api.v2alpha.createMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.createModelLineRequest
import org.wfanet.measurement.api.v2alpha.createModelOutageRequest
import org.wfanet.measurement.api.v2alpha.createModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.createModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.createModelShardRequest
import org.wfanet.measurement.api.v2alpha.createModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.dateInterval
import org.wfanet.measurement.api.v2alpha.deleteModelOutageRequest
import org.wfanet.measurement.api.v2alpha.deleteModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.deleteModelShardRequest
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.getModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.getModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.listModelLinesRequest
import org.wfanet.measurement.api.v2alpha.listModelOutagesRequest
import org.wfanet.measurement.api.v2alpha.listModelReleasesRequest
import org.wfanet.measurement.api.v2alpha.listModelRolloutsRequest
import org.wfanet.measurement.api.v2alpha.listModelShardsRequest
import org.wfanet.measurement.api.v2alpha.listModelSuitesRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.modelOutage
import org.wfanet.measurement.api.v2alpha.modelRelease
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.modelShard
import org.wfanet.measurement.api.v2alpha.modelSuite
import org.wfanet.measurement.api.v2alpha.publicKey
import org.wfanet.measurement.api.v2alpha.replaceDataProviderCapabilitiesRequest
import org.wfanet.measurement.api.v2alpha.replaceDataProviderRequiredDuchiesRequest
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.revokeCertificateRequest
import org.wfanet.measurement.api.v2alpha.scheduleModelRolloutFreezeRequest
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.setModelLineActiveEndTimeRequest
import org.wfanet.measurement.api.v2alpha.setModelLineHoldbackModelLineRequest
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.api.v2alpha.updatePublicKeyRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.api.withIdToken
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.tink.PrivateJwkHandle
import org.wfanet.measurement.common.crypto.tink.SelfIssuedIdTokens
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.verifyResult
import picocli.CommandLine
import picocli.CommandLine.ArgGroup
import picocli.CommandLine.Command
import picocli.CommandLine.Mixin
import picocli.CommandLine.Model.CommandSpec
import picocli.CommandLine.Option
import picocli.CommandLine.ParameterException
import picocli.CommandLine.Parameters
import picocli.CommandLine.ParentCommand
import picocli.CommandLine.Spec

private val CHANNEL_SHUTDOWN_TIMEOUT = JavaDuration.ofSeconds(30)

@Command(
  name = "MeasurementSystem",
  description = ["Interacts with the Cross-Media Measurement System API"],
  subcommands =
    [
      CommandLine.HelpCommand::class,
      Accounts::class,
      Certificates::class,
      PublicKeys::class,
      MeasurementConsumers::class,
      Measurements::class,
      ApiKeys::class,
      DataProviders::class,
      ModelLines::class,
      ModelReleases::class,
      ModelOutages::class,
      ModelShards::class,
      ModelRollouts::class,
      ModelSuites::class,
    ],
)
class MeasurementSystem private constructor() : Runnable {
  @Spec private lateinit var commandSpec: CommandSpec

  val commandLine: CommandLine
    get() = commandSpec.commandLine()

  @Mixin private lateinit var tlsFlags: TlsFlags

  @Option(
    names = ["--kingdom-public-api-target"],
    description = ["gRPC target (authority) of the Kingdom public API server"],
    required = true,
  )
  private lateinit var target: String

  @Option(
    names = ["--kingdom-public-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Kingdom public API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --kingdom-public-api-target.",
      ],
    required = false,
  )
  private var certHost: String? = null

  val trustedCertificates: Map<ByteString, X509Certificate>
    get() = tlsFlags.signingCerts.trustedCertificates

  val kingdomChannel: ManagedChannel by lazy {
    buildMutualTlsChannel(target, tlsFlags.signingCerts, certHost)
      .withShutdownTimeout(CHANNEL_SHUTDOWN_TIMEOUT)
  }

  val rpcDispatcher: CoroutineDispatcher = Dispatchers.IO

  override fun run() {
    // No-op. See subcommands.
  }

  companion object {
    /**
     * Issuer for Self-issued OpenID Provider (SIOP).
     *
     * TODO(@SanjayVas): Use this from [SelfIssuedIdTokens] once it's exposed.
     */
    const val SELF_ISSUED_ISSUER = "https://self-issued.me"

    @JvmStatic fun main(args: Array<String>) = commandLineMain(MeasurementSystem(), args)
  }
}

@Command(name = "accounts", subcommands = [CommandLine.HelpCommand::class])
private class Accounts {
  @ParentCommand
  lateinit var parentCommand: MeasurementSystem
    private set

  private val accountsClient: AccountsCoroutineStub by lazy {
    AccountsCoroutineStub(parentCommand.kingdomChannel)
  }

  @Command
  fun authenticate(
    @Option(
      names = ["--self-issued-openid-provider-key", "--siop-key"],
      description = ["Self-issued OpenID Provider key as a binary Tink keyset"],
    )
    siopKey: File
  ) {
    val response =
      runBlocking(parentCommand.rpcDispatcher) {
        accountsClient.authenticate(
          authenticateRequest { issuer = MeasurementSystem.SELF_ISSUED_ISSUER }
        )
      }

    // TODO(@SanjayVas): Use a util from common.crypto rather than directly interacting with Tink.
    val keysetHandle: KeysetHandle =
      siopKey.inputStream().use { input ->
        CleartextKeysetHandle.read(BinaryKeysetReader.withInputStream(input))
      }
    val idToken: String =
      SelfIssuedIdTokens.generateIdToken(
        PrivateJwkHandle(keysetHandle),
        response.authenticationRequestUri,
        Clock.systemUTC(),
      )

    println("ID Token: $idToken")
  }

  @Command
  fun activate(
    @Option(names = ["--id-token"]) idTokenOption: String? = null,
    @Parameters(index = "0", description = ["Resource name of the Account"]) name: String,
    @Option(names = ["--activation-token"], required = true) activationToken: String,
  ) {
    // TODO(remkop/picocli#882): Use built-in Picocli functionality once available.
    val idToken: String = idTokenOption ?: String(System.console().readPassword("ID Token: "))

    val response: Account =
      runBlocking(parentCommand.rpcDispatcher) {
        accountsClient
          .withIdToken(idToken)
          .activateAccount(
            activateAccountRequest {
              this.name = name
              this.activationToken = activationToken
            }
          )
      }
    println(response)
  }
}

@Command(name = "certificates", subcommands = [CommandLine.HelpCommand::class])
private class Certificates {
  @ParentCommand
  lateinit var parentCommand: MeasurementSystem
    private set

  @Option(
    names = ["--api-key"],
    paramLabel = "<apiKey>",
    description = ["API authentication key. Required when parent type is MeasurementConsumer."],
  )
  var authenticationKey: String? = null

  private val certificatesClient: CertificatesCoroutineStub by lazy {
    val client = CertificatesCoroutineStub(parentCommand.kingdomChannel)
    if (authenticationKey == null) client else client.withAuthenticationKey(authenticationKey)
  }

  @Command(description = ["Creates a Certificate resource"])
  fun create(
    @Option(
      names = ["--parent"],
      paramLabel = "<parent>",
      required = true,
      description = ["Name of parent resource"],
    )
    parent: String,
    @Option(
      names = ["--certificate"],
      paramLabel = "<certPath>",
      description = ["Path to X.509 certificate in PEM or DER format"],
      required = true,
    )
    certificateFile: File,
  ) {
    if (authenticationKey == null && MeasurementConsumerKey.fromName(parent) != null) {
      throw ParameterException(
        parentCommand.commandLine,
        "API authentication key is required when parent type is MeasurementConsumer",
      )
    }

    val certificate: X509Certificate = certificateFile.inputStream().use { readCertificate(it) }
    val request = createCertificateRequest {
      this.parent = parent
      this.certificate = certificate { x509Der = certificate.encoded.toByteString() }
    }
    val response: Certificate =
      runBlocking(parentCommand.rpcDispatcher) { certificatesClient.createCertificate(request) }
    println("Certificate name: ${response.name}")
  }

  @Command(description = ["Revokes a Certificate"])
  fun revoke(
    @Option(names = ["--revocation-state"], paramLabel = "<revocationState>", required = true)
    revocationState: Certificate.RevocationState,
    @Parameters(index = "0", paramLabel = "<name>", description = ["Resource name"]) name: String,
  ) {
    if (authenticationKey == null && MeasurementConsumerCertificateKey.fromName(name) != null) {
      throw ParameterException(
        parentCommand.commandLine,
        "API authentication key is required when parent type is MeasurementConsumer",
      )
    }

    val request = revokeCertificateRequest {
      this.name = name
      this.revocationState = revocationState
    }
    val response =
      runBlocking(parentCommand.rpcDispatcher) { certificatesClient.revokeCertificate(request) }
    println(response)
  }
}

@Command(name = "public-keys", subcommands = [CommandLine.HelpCommand::class])
private class PublicKeys {
  @ParentCommand
  lateinit var parentCommand: MeasurementSystem
    private set

  @Option(
    names = ["--api-key"],
    paramLabel = "<apiKey>",
    description = ["API authentication key. Required when parent type is MeasurementConsumer."],
  )
  var authenticationKey: String? = null

  private val publicKeysClient: PublicKeysCoroutineStub by lazy {
    val client = PublicKeysCoroutineStub(parentCommand.kingdomChannel)
    if (authenticationKey == null) client else client.withAuthenticationKey(authenticationKey)
  }

  @Command(description = ["Updates a PublicKey resource"])
  fun update(
    @Option(
      names = ["--public-key"],
      paramLabel = "<publicKeyFile>",
      description = ["Path to serialized EncryptionPublicKey message"],
      required = true,
    )
    publicKeyFile: File,
    @Option(
      names = ["--public-key-signature"],
      paramLabel = "<publicKeySignatureFile>",
      description = ["Path to signature of public key"],
      required = true,
    )
    publicKeySignatureFile: File,
    @Option(
      names = ["--certificate"],
      paramLabel = "<certificate>",
      description = ["Name of Certificate resource to verify public key signature"],
      required = true,
    )
    certificate: String,
    @Parameters(index = "0", paramLabel = "<name>", description = ["Resource name"]) name: String,
  ) {
    val packedEncryptionPublicKey = any {
      value = publicKeyFile.readByteString()
      typeUrl = ProtoReflection.getTypeUrl(EncryptionPublicKey.getDescriptor())
    }
    try {
      packedEncryptionPublicKey.unpack<EncryptionPublicKey>()
    } catch (e: InvalidProtocolBufferException) {
      throw ParameterException(parentCommand.commandLine, "Invalid EncryptionPublicKey", e)
    }

    val request = updatePublicKeyRequest {
      this.publicKey = publicKey {
        this.name = name
        this.publicKey = signedMessage {
          setMessage(packedEncryptionPublicKey)
          signature = publicKeySignatureFile.readByteString()
        }
        this.certificate = certificate
      }
    }
    val response: PublicKey =
      runBlocking(parentCommand.rpcDispatcher) { publicKeysClient.updatePublicKey(request) }
    println(response)
  }
}

@Command(name = "measurement-consumers", subcommands = [CommandLine.HelpCommand::class])
private class MeasurementConsumers {
  @ParentCommand
  lateinit var parentCommand: MeasurementSystem
    private set

  private val measurementConsumersClient: MeasurementConsumersCoroutineStub by lazy {
    MeasurementConsumersCoroutineStub(parentCommand.kingdomChannel)
  }

  @Command(
    description =
      [
        "Creates a MeasurementConsumer resource.",
        "Use the EncryptionPublicKeys tool to serialize/sign the encryption public key.",
      ]
  )
  fun create(
    @Option(names = ["--creation-token"], paramLabel = "<creationToken>", required = true)
    creationToken: String,
    @Option(
      names = ["--certificate"],
      paramLabel = "<certPath>",
      description = ["Path to X.509 certificate in PEM or DER format"],
      required = true,
    )
    certificateFile: File,
    @Option(
      names = ["--public-key"],
      paramLabel = "<pubKeyPath>",
      description = ["Path to serialized EncryptionPublicKey message"],
      required = true,
    )
    publicKeyFile: File,
    @Option(
      names = ["--public-key-signature"],
      paramLabel = "<pubKeySigPath>",
      description = ["Path to public key signature"],
      required = true,
    )
    publicKeySignatureFile: File,
    @Option(names = ["--display-name"], paramLabel = "<displayName>", defaultValue = "")
    displayName: String,
    @Option(names = ["--id-token"], paramLabel = "<idToken>") idTokenOption: String? = null,
  ) {
    // TODO(remkop/picocli#882): Use built-in Picocli functionality once available.
    val idToken: String = idTokenOption ?: String(System.console().readPassword("ID Token: "))

    val packedEncryptionPublicKey = any {
      value = publicKeyFile.readByteString()
      typeUrl = ProtoReflection.getTypeUrl(EncryptionPublicKey.getDescriptor())
    }
    try {
      packedEncryptionPublicKey.unpack<EncryptionPublicKey>()
    } catch (e: InvalidProtocolBufferException) {
      throw ParameterException(parentCommand.commandLine, "Invalid EncryptionPublicKey", e)
    }

    val certificate: X509Certificate = certificateFile.inputStream().use { readCertificate(it) }
    val request = createMeasurementConsumerRequest {
      measurementConsumer = measurementConsumer {
        measurementConsumerCreationToken = creationToken
        certificateDer = certificate.encoded.toByteString()
        publicKey = signedMessage {
          setMessage(packedEncryptionPublicKey)
          signature = publicKeySignatureFile.readByteString()
        }
        this.displayName = displayName
      }
    }
    val response: MeasurementConsumer =
      runBlocking(parentCommand.rpcDispatcher) {
        measurementConsumersClient.withIdToken(idToken).createMeasurementConsumer(request)
      }
    println(response)
  }
}

@Command(
  name = "measurements",
  sortOptions = false,
  subcommands =
    [
      CommandLine.HelpCommand::class,
      CreateMeasurement::class,
      ListMeasurements::class,
      GetMeasurement::class,
      CancelMeasurement::class,
    ],
)
private class Measurements {
  @ParentCommand
  lateinit var parentCommand: MeasurementSystem
    private set

  @Option(
    names = ["--api-key"],
    description = ["API authentication key for the MeasurementConsumer"],
    required = true,
  )
  lateinit var apiAuthenticationKey: String
    private set

  val measurementConsumerStub: MeasurementConsumersCoroutineStub by lazy {
    MeasurementConsumersCoroutineStub(parentCommand.kingdomChannel)
  }
  val measurementStub: MeasurementsCoroutineStub by lazy {
    MeasurementsCoroutineStub(parentCommand.kingdomChannel)
  }
  val dataProviderStub: DataProvidersCoroutineStub by lazy {
    DataProvidersCoroutineStub(parentCommand.kingdomChannel)
  }
  val certificateStub: CertificatesCoroutineStub by lazy {
    CertificatesCoroutineStub(parentCommand.kingdomChannel)
  }

  companion object {
    fun printState(measurement: Measurement) {
      if (measurement.state == Measurement.State.FAILED) {
        println(
          "State: FAILED - " + measurement.failure.reason + ": " + measurement.failure.message
        )
      } else {
        println("State: ${measurement.state}")
      }
    }
  }
}

@Command(name = "create", description = ["Creates a Single Measurement"])
class CreateMeasurement : Runnable {
  @ParentCommand private lateinit var parentCommand: Measurements

  @ArgGroup(exclusive = false, multiplicity = "1", heading = "Create Measurement Flags\n")
  lateinit var createMeasurementFlags: CreateMeasurementFlags

  private val secureRandom = SecureRandom.getInstance("SHA1PRNG")

  private fun getPopulationDataProviderEntry(
    populationDataProviderInput:
      CreateMeasurementFlags.MeasurementParams.PopulationMeasurementParams.PopulationDataProviderInput,
    populationMeasurementParams:
      CreateMeasurementFlags.MeasurementParams.PopulationMeasurementParams,
    measurementConsumerSigningKey: SigningKeyHandle,
    packedMeasurementEncryptionPublicKey: ProtoAny,
  ): Measurement.DataProviderEntry {
    return dataProviderEntry {
      val requisitionSpec = requisitionSpec {
        population =
          RequisitionSpecKt.population {
            interval = interval {
              startTime = populationMeasurementParams.populationInputs.startTime.toProtoTime()
              endTime = populationMeasurementParams.populationInputs.endTime.toProtoTime()
            }
            if (populationMeasurementParams.populationInputs.filter.isNotEmpty())
              filter = eventFilter {
                expression = populationMeasurementParams.populationInputs.filter
              }
          }
        measurementPublicKey = packedMeasurementEncryptionPublicKey
        // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting this
        // field.
        serializedMeasurementPublicKey = packedMeasurementEncryptionPublicKey.value

        nonce = secureRandom.nextLong()
      }

      key = populationDataProviderInput.name
      val dataProvider =
        runBlocking(parentCommand.parentCommand.rpcDispatcher) {
          parentCommand.dataProviderStub
            .withAuthenticationKey(parentCommand.apiAuthenticationKey)
            .getDataProvider(getDataProviderRequest { name = populationDataProviderInput.name })
        }
      value =
        DataProviderEntries.value {
          dataProviderCertificate = dataProvider.certificate
          dataProviderPublicKey = dataProvider.publicKey.message
          encryptedRequisitionSpec =
            encryptRequisitionSpec(
              signRequisitionSpec(requisitionSpec, measurementConsumerSigningKey),
              dataProvider.publicKey.unpack(),
            )
          nonceHash = Hashing.hashSha256(requisitionSpec.nonce)
        }
    }
  }

  private fun getEventDataProviderEntry(
    eventDataProviderInput:
      CreateMeasurementFlags.MeasurementParams.EventMeasurementParams.EventDataProviderInput,
    measurementConsumerSigningKey: SigningKeyHandle,
    packedMeasurementEncryptionPublicKey: ProtoAny,
  ): Measurement.DataProviderEntry {
    return dataProviderEntry {
      val requisitionSpec = requisitionSpec {
        val eventGroups =
          eventDataProviderInput.eventGroupInputs.map {
            eventGroupEntry {
              key = it.name
              value =
                EventGroupEntries.value {
                  collectionInterval = interval {
                    startTime = it.eventStartTime.toProtoTime()
                    endTime = it.eventEndTime.toProtoTime()
                  }
                  filter = eventFilter {
                    expression = eventDataProviderInput.eventFilters.single().eventFilter
                  }
                }
            }
          }
        events = RequisitionSpecKt.events { this.eventGroups += eventGroups }
        this.measurementPublicKey = packedMeasurementEncryptionPublicKey
        // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting this
        // field.
        serializedMeasurementPublicKey = packedMeasurementEncryptionPublicKey.value
        nonce = secureRandom.nextLong()
      }

      key = eventDataProviderInput.name
      val dataProvider =
        runBlocking(parentCommand.parentCommand.rpcDispatcher) {
          parentCommand.dataProviderStub
            .withAuthenticationKey(parentCommand.apiAuthenticationKey)
            .getDataProvider(getDataProviderRequest { name = eventDataProviderInput.name })
        }
      value =
        DataProviderEntries.value {
          dataProviderCertificate = dataProvider.certificate
          dataProviderPublicKey = dataProvider.publicKey.message
          encryptedRequisitionSpec =
            encryptRequisitionSpec(
              signRequisitionSpec(requisitionSpec, measurementConsumerSigningKey),
              dataProvider.publicKey.unpack(),
            )
          nonceHash = Hashing.hashSha256(requisitionSpec.nonce)
        }
    }
  }

  override fun run() {
    val measurementParams = createMeasurementFlags.measurementParams
    val measurementConsumer =
      runBlocking(parentCommand.parentCommand.rpcDispatcher) {
        parentCommand.measurementConsumerStub
          .withAuthenticationKey(parentCommand.apiAuthenticationKey)
          .getMeasurementConsumer(
            getMeasurementConsumerRequest { name = createMeasurementFlags.measurementConsumer }
          )
      }
    val measurementConsumerCertificate = readCertificate(measurementConsumer.certificateDer)
    val measurementConsumerPrivateKey =
      readPrivateKey(
        createMeasurementFlags.privateKeyDerFile.readByteString(),
        measurementConsumerCertificate.publicKey.algorithm,
      )
    val measurementConsumerSigningKey =
      SigningKeyHandle(measurementConsumerCertificate, measurementConsumerPrivateKey)
    val packedMeasurementEncryptionPublicKey = measurementConsumer.publicKey.message

    val measurement = measurement {
      this.measurementConsumerCertificate = measurementConsumer.certificate
      dataProviders +=
        if (measurementParams.populationMeasurementParams.selected) {
          listOf(
            getPopulationDataProviderEntry(
              measurementParams.populationMeasurementParams.populationDataProviderInput,
              measurementParams.populationMeasurementParams,
              measurementConsumerSigningKey,
              packedMeasurementEncryptionPublicKey,
            )
          )
        } else {
          measurementParams.eventMeasurementParams.eventDataProviderInputs.map {
            getEventDataProviderEntry(
              it,
              measurementConsumerSigningKey,
              packedMeasurementEncryptionPublicKey,
            )
          }
        }
      val unsignedMeasurementSpec = measurementSpec {
        measurementPublicKey = packedMeasurementEncryptionPublicKey
        // TODO(world-federation-of-advertisers/cross-media-measurement#1301): Stop setting this
        // field.
        serializedMeasurementPublicKey = packedMeasurementEncryptionPublicKey.value
        nonceHashes += this@measurement.dataProviders.map { it.value.nonceHash }
        if (!measurementParams.populationMeasurementParams.selected) {
          vidSamplingInterval = vidSamplingInterval {
            start = measurementParams.eventMeasurementParams.vidSamplingStart
            width = measurementParams.eventMeasurementParams.vidSamplingWidth
          }
          if (measurementParams.eventMeasurementParams.eventMeasurementTypeParams.reach.selected) {
            reach = createMeasurementFlags.getReach()
          } else if (
            measurementParams.eventMeasurementParams.eventMeasurementTypeParams.reachAndFrequency
              .selected
          ) {
            reachAndFrequency = createMeasurementFlags.getReachAndFrequency()
          } else if (
            measurementParams.eventMeasurementParams.eventMeasurementTypeParams.impression.selected
          ) {
            impression = createMeasurementFlags.getImpression()
          } else if (
            measurementParams.eventMeasurementParams.eventMeasurementTypeParams.duration.selected
          ) {
            duration = createMeasurementFlags.getDuration()
          }
        } else if (measurementParams.populationMeasurementParams.selected) {
          population = createMeasurementFlags.getPopulation()
        }
        if (createMeasurementFlags.modelLine.isNotEmpty())
          this.modelLine = createMeasurementFlags.modelLine
      }

      this.measurementSpec =
        signMeasurementSpec(unsignedMeasurementSpec, measurementConsumerSigningKey)
      measurementReferenceId = createMeasurementFlags.measurementReferenceId
    }

    val response =
      runBlocking(parentCommand.parentCommand.rpcDispatcher) {
        parentCommand.measurementStub
          .withAuthenticationKey(parentCommand.apiAuthenticationKey)
          .createMeasurement(
            createMeasurementRequest {
              parent = measurementConsumer.name
              this.measurement = measurement
              requestId = createMeasurementFlags.requestId
            }
          )
      }
    println("Measurement Name: ${response.name}")
  }
}

@Command(name = "list", description = ["Lists Measurements"])
class ListMeasurements : Runnable {
  @ParentCommand private lateinit var parentCommand: Measurements

  @Option(
    names = ["--measurement-consumer"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  override fun run() {
    val response =
      runBlocking(parentCommand.parentCommand.rpcDispatcher) {
        parentCommand.measurementStub
          .withAuthenticationKey(parentCommand.apiAuthenticationKey)
          .listMeasurements(listMeasurementsRequest { parent = measurementConsumerName })
      }

    response.measurementsList.forEach {
      if (it.state == Measurement.State.FAILED) {
        println(it.name + " FAILED - " + it.failure.reason + ": " + it.failure.message)
      } else {
        println(it.name + " " + it.state)
      }
    }
  }
}

@Command(name = "get", description = ["Gets a Single Measurement"])
class GetMeasurement : Runnable {
  @ParentCommand private lateinit var parentCommand: Measurements

  @Parameters(index = "0", description = ["API resource name of the Measurement"])
  private lateinit var measurementName: String

  @Option(
    names = ["--encryption-private-key-file"],
    description = ["MeasurementConsumer's EncryptionPrivateKey"],
    required = true,
  )
  private lateinit var privateKeyDerFile: File

  private val privateKeyHandle: PrivateKeyHandle by lazy { loadPrivateKey(privateKeyDerFile) }

  private fun getMeasurementResult(resultOutput: Measurement.ResultOutput): Measurement.Result {
    val certificate = runBlocking {
      parentCommand.certificateStub
        .withAuthenticationKey(parentCommand.apiAuthenticationKey)
        .getCertificate(getCertificateRequest { name = resultOutput.certificate })
    }

    val signedResult = decryptResult(resultOutput.encryptedResult, privateKeyHandle)
    val x509Certificate: X509Certificate = readCertificate(certificate.x509Der)
    val trustedIssuer =
      checkNotNull(
        parentCommand.parentCommand.trustedCertificates[
            checkNotNull(x509Certificate.authorityKeyIdentifier)]
      ) {
        "${certificate.name} not issued by trusted CA"
      }
    try {
      verifyResult(signedResult, x509Certificate, trustedIssuer)
    } catch (e: CertPathValidatorException) {
      throw Exception("Certificate path of ${certificate.name} is invalid", e)
    } catch (e: SignatureException) {
      throw Exception("Measurement result signature is invalid", e)
    }
    return signedResult.unpack()
  }

  private fun printMeasurementResult(result: Measurement.Result) {
    if (result.hasReach()) println("Reach - ${result.reach.value}")
    if (result.hasFrequency()) {
      println("Frequency - ")
      result.frequency.relativeFrequencyDistributionMap.toSortedMap().forEach {
        println("\t${it.key}  ${it.value}")
      }
    }
    if (result.hasImpression()) {
      println("Impression - ${result.impression.value}")
    }
    if (result.hasWatchDuration()) {
      println(
        "WatchDuration - " +
          "${result.watchDuration.value.seconds} seconds ${result.watchDuration.value.nanos} nanos"
      )
    }
    if (result.hasPopulation()) {
      println("Population - ${result.population.value}")
    }
  }

  override fun run() {
    val measurement =
      runBlocking(parentCommand.parentCommand.rpcDispatcher) {
        parentCommand.measurementStub
          .withAuthenticationKey(parentCommand.apiAuthenticationKey)
          .getMeasurement(getMeasurementRequest { name = measurementName })
      }

    Measurements.printState(measurement)
    if (measurement.state == Measurement.State.SUCCEEDED) {
      measurement.resultsList.forEach {
        val result = getMeasurementResult(it)
        printMeasurementResult(result)
      }
    }
  }
}

@Command(name = "cancel", description = ["Cancels a Measurement"])
class CancelMeasurement : Runnable {
  @ParentCommand private lateinit var parentCommand: Measurements

  @Parameters(index = "0", description = ["API resource name of the Measurement"])
  private lateinit var measurementName: String

  override fun run() {
    val measurement =
      runBlocking(parentCommand.parentCommand.rpcDispatcher) {
        parentCommand.measurementStub
          .withAuthenticationKey(parentCommand.apiAuthenticationKey)
          .cancelMeasurement(cancelMeasurementRequest { name = measurementName })
      }

    Measurements.printState(measurement)
  }
}

@Command(name = "data-providers", subcommands = [CommandLine.HelpCommand::class])
private class DataProviders {
  @ParentCommand
  lateinit var parentCommand: MeasurementSystem
    private set

  val dataProviderStub: DataProvidersCoroutineStub by lazy {
    DataProvidersCoroutineStub(parentCommand.kingdomChannel)
  }
  @Option(
    names = ["--name"],
    description = ["API resource name of the DataProvider"],
    required = true,
  )
  private lateinit var dataProviderName: String

  @Command(name = "replace-required-duchies", description = ["Replaces DataProvider's duchy list"])
  fun replaceRequiredDuchyList(
    @Option(
      names = ["--required-duchies"],
      description =
        [
          "The set of new duchies externals IDS that that will replace the old duchy list for this DataProvider"
        ],
      required = true,
    )
    requiredDuchies: List<String>
  ) {
    val request = replaceDataProviderRequiredDuchiesRequest {
      name = dataProviderName
      this.requiredDuchies += requiredDuchies
    }
    val outputDataProvider =
      runBlocking(parentCommand.rpcDispatcher) {
        dataProviderStub.replaceDataProviderRequiredDuchies(request)
      }

    println(
      "Data Provider ${outputDataProvider.name} duchy list replaced with ${outputDataProvider.requiredDuchiesList}"
    )
  }

  @Command(name = "update-capabilities", description = ["Updates DataProvider's capabilities"])
  fun updateCapabilities(
    @Option(
      names = ["--hmss-supported", "--honest-majority-share-shuffle-supported"],
      description = ["Whether the Honest Majority Share Shuffle (HMSS) protocol is supported"],
      required = false,
    )
    honestMajorityShareShuffleSupported: Boolean? = null
  ) {
    val capabilities: DataProvider.Capabilities =
      runBlocking(parentCommand.rpcDispatcher) {
          dataProviderStub.getDataProvider(getDataProviderRequest { name = dataProviderName })
        }
        .capabilities
    val request = replaceDataProviderCapabilitiesRequest {
      name = dataProviderName
      this.capabilities =
        capabilities.copy {
          if (honestMajorityShareShuffleSupported != null) {
            this.honestMajorityShareShuffleSupported = honestMajorityShareShuffleSupported
          }
        }
    }
    val dataProvider: DataProvider =
      runBlocking(parentCommand.rpcDispatcher) {
        dataProviderStub.replaceDataProviderCapabilities(request)
      }

    println(dataProvider.capabilities)
  }

  @Command(name = "get", description = ["gets a DataProvider"])
  fun getDataProvider() {
    val dataProvider = runBlocking {
      dataProviderStub.getDataProvider(getDataProviderRequest { name = dataProviderName })
    }
    println("Data Provider Resource: ${dataProvider.name}")
    println(dataProvider)
  }
}

@Command(name = "api-keys", subcommands = [CommandLine.HelpCommand::class])
private class ApiKeys {
  @ParentCommand
  lateinit var parentCommand: MeasurementSystem
    private set

  private val apiKeysClient: ApiKeysCoroutineStub by lazy {
    ApiKeysCoroutineStub(parentCommand.kingdomChannel)
  }

  @Option(names = ["--id-token"], paramLabel = "<idToken>")
  private var idTokenOption: String? = null

  @Command(description = ["Creates an ApiKey resource"])
  fun create(
    @Option(
      names = ["--measurement-consumer"],
      paramLabel = "<measurementConsumer>",
      description = ["API resource name of the MeasurementConsumer"],
      required = true,
    )
    measurementConsumer: String,
    @Option(names = ["--nickname"], paramLabel = "<nickname>", required = true) nickname: String,
    @Option(names = ["--description"], paramLabel = "<description>") description: String?,
  ) {
    // TODO(remkop/picocli#882): Use built-in Picocli functionality once available.
    val idToken: String = idTokenOption ?: String(System.console().readPassword("ID Token: "))
    val request = createApiKeyRequest {
      parent = measurementConsumer
      apiKey = apiKey {
        this.nickname = nickname
        if (description != null) {
          this.description = description
        }
      }
    }
    val response: ApiKey =
      runBlocking(parentCommand.rpcDispatcher) {
        apiKeysClient.withIdToken(idToken).createApiKey(request)
      }
    println(response)
  }
}

@Command(name = "model-lines", subcommands = [CommandLine.HelpCommand::class])
private class ModelLines {
  @ParentCommand
  lateinit var parentCommand: MeasurementSystem
    private set

  val modelLineStub: ModelLinesCoroutineStub by lazy {
    ModelLinesCoroutineStub(parentCommand.kingdomChannel)
  }

  @Command(description = ["Creates model line."])
  fun create(
    @Option(
      names = ["--parent"],
      description = ["API resource name of the parent ModelSuite."],
      required = true,
    )
    modelSuiteName: String,
    @Option(
      names = ["--display-name"],
      description = ["Model line display name."],
      required = false,
      defaultValue = "",
    )
    modelLineDisplayName: String,
    @Option(
      names = ["--description"],
      description = ["Model line description."],
      required = false,
      defaultValue = "",
    )
    modelLineDescription: String,
    @Option(
      names = ["--active-start-time"],
      description = ["Model line active start time in ISO 8601 format of UTC."],
      required = true,
    )
    modelLineActiveStartTime: Instant,
    @Option(
      names = ["--active-end-time"],
      description = ["Model line active end time in ISO 8601 format of UTC."],
      required = false,
    )
    modelLineActiveEndTime: Instant? = null,
    @Option(names = ["--type"], description = ["Model line type."], required = true)
    modelLineType: ModelLine.Type,
    @Option(
      names = ["--holdback-model-line"],
      description = ["Holdback model line."],
      required = false,
      defaultValue = "",
    )
    modelLineHoldbackModelLine: String,
  ) {
    val request = createModelLineRequest {
      parent = modelSuiteName
      modelLine = modelLine {
        displayName = modelLineDisplayName
        description = modelLineDescription
        activeStartTime = modelLineActiveStartTime.toProtoTime()
        if (modelLineActiveEndTime != null) {
          activeEndTime = modelLineActiveEndTime.toProtoTime()
        }
        type = modelLineType
        holdbackModelLine = modelLineHoldbackModelLine
      }
    }
    val outputModelLine =
      runBlocking(parentCommand.rpcDispatcher) { modelLineStub.createModelLine(request) }

    println("Model line ${outputModelLine.name} has been created.")
    printModelLine(outputModelLine)
  }

  @Command(description = ["Sets the holdback model line for a given model line."])
  fun setHoldbackModelLine(
    @Option(names = ["--name"], description = ["Model line name."], required = true)
    modelLineName: String,
    @Option(
      names = ["--holdback-model-line"],
      description = ["Holdback model line."],
      required = true,
    )
    modelLineHoldbackModelLine: String,
  ) {
    val request = setModelLineHoldbackModelLineRequest {
      name = modelLineName
      holdbackModelLine = modelLineHoldbackModelLine
    }
    val outputModelLine =
      runBlocking(parentCommand.rpcDispatcher) {
        modelLineStub.setModelLineHoldbackModelLine(request)
      }
    printModelLine(outputModelLine)
  }

  @Command(description = ["Sets the active end time for a given model line."])
  fun setActiveEndTime(
    @Option(names = ["--name"], description = ["Model line name."], required = true)
    modelLineName: String,
    @Option(
      names = ["--active-end-time"],
      description = ["Model line active end time in ISO 8601 format of UTC."],
      required = true,
    )
    modelLineActiveEndTime: Instant,
  ) {
    val request = setModelLineActiveEndTimeRequest {
      name = modelLineName
      activeEndTime = modelLineActiveEndTime.toProtoTime()
    }
    val outputModelLine =
      runBlocking(parentCommand.rpcDispatcher) { modelLineStub.setModelLineActiveEndTime(request) }
    printModelLine(outputModelLine)
  }

  @Command(description = ["Lists model lines for a model suite."])
  fun list(
    @Option(
      names = ["--parent"],
      description = ["API resource name of the parent ModelSuite."],
      required = true,
    )
    modelSuiteName: String,
    @Option(
      names = ["--page-size"],
      description = ["The maximum number of ModelLines to return."],
      required = false,
      defaultValue = "0",
    )
    listPageSize: Int,
    @Option(
      names = ["--page-token"],
      description =
        [
          "A page token, received from a previous `ListModelLinesRequest` call. Provide this to retrieve the subsequent page."
        ],
      required = false,
      defaultValue = "",
    )
    listPageToken: String,
    @Option(
      names = ["--types"],
      description = ["The list of types used to filter the result."],
      required = false,
    )
    modelLineTypes: List<ModelLine.Type>?,
  ) {
    val request = listModelLinesRequest {
      parent = modelSuiteName
      pageSize = listPageSize
      pageToken = listPageToken
      if (modelLineTypes != null) {
        filter = filter { typeIn += modelLineTypes }
      }
    }
    val response =
      runBlocking(parentCommand.rpcDispatcher) { modelLineStub.listModelLines(request) }
    response.modelLinesList.forEach { printModelLine(it) }
  }

  private fun printModelLine(modelLine: ModelLine) {
    println("NAME - ${modelLine.name}")
    if (modelLine.displayName.isNotBlank()) {
      println("DISPLAY NAME - ${modelLine.displayName}")
    }
    if (modelLine.description.isNotBlank()) {
      println("DESCRIPTION - ${modelLine.description}")
    }
    println("ACTIVE START TIME - ${modelLine.activeStartTime}")
    if (modelLine.hasActiveEndTime()) {
      println("ACTIVE END TIME - ${modelLine.activeEndTime}")
    }
    println("TYPE - ${modelLine.type}")
    if (modelLine.holdbackModelLine.isNotBlank()) {
      println("HOLDBACK MODEL LINE - ${modelLine.holdbackModelLine}")
    }
    println("CREATE TIME - ${modelLine.createTime}")
    println("UPDATE TIME - ${modelLine.updateTime}")
  }
}

@Command(name = "model-releases", subcommands = [CommandLine.HelpCommand::class])
private class ModelReleases {
  @ParentCommand
  lateinit var parentCommand: MeasurementSystem
    private set

  val modelReleaseStub: ModelReleasesCoroutineStub by lazy {
    ModelReleasesCoroutineStub(parentCommand.kingdomChannel)
  }

  @Command(description = ["Creates model release."])
  fun create(
    @Option(
      names = ["--parent"],
      description = ["API resource name of the parent ModelSuite."],
      required = true,
    )
    modelSuiteName: String
  ) {
    val request = createModelReleaseRequest {
      parent = modelSuiteName
      modelRelease = modelRelease {}
    }
    val outputModelRelease =
      runBlocking(parentCommand.rpcDispatcher) { modelReleaseStub.createModelRelease(request) }

    println("Model release ${outputModelRelease.name} has been created.")
    printModelRelease(outputModelRelease)
  }

  @Command(description = ["Gets model release."])
  fun get(
    @Option(names = ["--name"], description = ["Model release name."], required = true)
    modelReleaseName: String
  ) {
    val request = getModelReleaseRequest { name = modelReleaseName }
    val outputModelRelease =
      runBlocking(parentCommand.rpcDispatcher) { modelReleaseStub.getModelRelease(request) }
    printModelRelease(outputModelRelease)
  }

  @Command(description = ["Lists model releases for a model suite."])
  fun list(
    @Option(
      names = ["--parent"],
      description = ["API resource name of the parent ModelSuite."],
      required = true,
    )
    modelSuiteName: String,
    @Option(
      names = ["--page-size"],
      description = ["The maximum number of ModelReleases to return."],
      required = false,
      defaultValue = "0",
    )
    listPageSize: Int,
    @Option(
      names = ["--page-token"],
      description =
        [
          "A page token, received from a previous `ListModelReleasesRequest` call. Provide this to retrieve the subsequent page."
        ],
      required = false,
      defaultValue = "",
    )
    listPageToken: String,
  ) {
    val request = listModelReleasesRequest {
      parent = modelSuiteName
      pageSize = listPageSize
      pageToken = listPageToken
    }
    val response =
      runBlocking(parentCommand.rpcDispatcher) { modelReleaseStub.listModelReleases(request) }
    response.modelReleasesList.forEach { printModelRelease(it) }
  }

  private fun printModelRelease(modelRelease: ModelRelease) {
    println("NAME - ${modelRelease.name}")
    println("CREATE TIME - ${modelRelease.createTime}")
  }
}

@Command(name = "model-outages", subcommands = [CommandLine.HelpCommand::class])
private class ModelOutages {
  @ParentCommand
  lateinit var parentCommand: MeasurementSystem
    private set

  val modelOutageStub: ModelOutagesCoroutineStub by lazy {
    ModelOutagesCoroutineStub(parentCommand.kingdomChannel)
  }

  @Command(description = ["Creates model outage."])
  fun create(
    @Option(
      names = ["--parent"],
      description = ["API resource name of the parent ModelLine."],
      required = true,
    )
    modelLineName: String,
    @Option(
      names = ["--outage-start-time"],
      description = ["Start time of model outage in ISO 8601 format of UTC"],
      required = true,
    )
    outageStartTime: Instant,
    @Option(
      names = ["--outage-end-time"],
      description = ["End time of model outage in ISO 8601 format of UTC"],
      required = true,
    )
    outageEndTime: Instant,
  ) {
    val request = createModelOutageRequest {
      parent = modelLineName
      modelOutage = modelOutage {
        outageInterval = interval {
          startTime = outageStartTime.toProtoTime()
          endTime = outageEndTime.toProtoTime()
        }
      }
    }
    val outputModelOutage =
      runBlocking(parentCommand.rpcDispatcher) { modelOutageStub.createModelOutage(request) }

    println("Model outage ${outputModelOutage.name} has been created.")
    printModelOutage(outputModelOutage)
  }

  @Command(description = ["Lists model outages for a model line."])
  fun list(
    @Option(
      names = ["--parent"],
      description = ["API resource name of the parent ModelLine."],
      required = true,
    )
    modelLineName: String,
    @Option(
      names = ["--page-size"],
      description = ["The maximum number of ModelOutages to return."],
      required = false,
      defaultValue = "0",
    )
    listPageSize: Int,
    @Option(
      names = ["--page-token"],
      description =
        [
          "A page token, received from a previous `ListModelOutagesRequest` call. Provide this to retrieve the subsequent page."
        ],
      required = false,
      defaultValue = "",
    )
    listPageToken: String,
    @Option(
      names = ["--show-deleted"],
      description =
        ["A flag to specify whether to include ModelOutage in the DELETED state or not."],
      required = false,
      defaultValue = "false",
    )
    showDeletedOutages: Boolean,
    @Option(
      names = ["--interval-start-time"],
      description =
        ["Start time of interval for desired overlapping model outages in ISO 8601 format of UTC"],
      required = false,
    )
    outageStartTime: Instant? = null,
    @Option(
      names = ["--interval-end-time"],
      description =
        ["End time of interval for desired overlapping model outages in ISO 8601 format of UTC"],
      required = false,
    )
    outageEndTime: Instant? = null,
  ) {
    val request = listModelOutagesRequest {
      parent = modelLineName
      pageSize = listPageSize
      pageToken = listPageToken
      showDeleted = showDeletedOutages
      if (outageStartTime != null && outageEndTime != null) {
        filter =
          ListModelOutagesRequestKt.filter {
            outageIntervalOverlapping = interval {
              startTime = outageStartTime.toProtoTime()
              endTime = outageEndTime.toProtoTime()
            }
          }
      }
    }
    val response =
      runBlocking(parentCommand.rpcDispatcher) { modelOutageStub.listModelOutages(request) }
    response.modelOutagesList.forEach { printModelOutage(it) }
  }

  @Command(description = ["Deletes model outage."])
  fun delete(
    @Option(
      names = ["--name"],
      description = ["API resource name of the ModelOutage."],
      required = true,
    )
    modelOutageName: String
  ) {
    val request = deleteModelOutageRequest { name = modelOutageName }
    val outputModelOutage =
      runBlocking(parentCommand.rpcDispatcher) { modelOutageStub.deleteModelOutage(request) }

    println("Model outage ${outputModelOutage.name} has been deleted.")
    printModelOutage(outputModelOutage)
  }

  private fun printModelOutage(modelOutage: ModelOutage) {
    println("NAME - ${modelOutage.name}")
    println("OUTAGE INTERVAL - ${modelOutage.outageInterval}")
    println("STATE - ${modelOutage.stateValue}")
    println("CREATE TIME - ${modelOutage.createTime}")
    println("DELETE TIME - ${modelOutage.deleteTime}")
  }
}

@Command(name = "model-shards", subcommands = [CommandLine.HelpCommand::class])
private class ModelShards {
  @ParentCommand
  lateinit var parentCommand: MeasurementSystem
    private set

  val modelShardStub: ModelShardsCoroutineStub by lazy {
    ModelShardsCoroutineStub(parentCommand.kingdomChannel)
  }

  @Command(description = ["Creates model shard."])
  fun create(
    @Option(
      names = ["--parent"],
      description = ["API resource name of the parent Event Data Provider."],
      required = true,
    )
    dataProviderName: String,
    @Option(
      names = ["--model-release"],
      description = ["API Resource name of the ModelRelease that this is a shard of."],
      required = true,
    )
    shardModelRelease: String,
    @Option(
      names = ["--model-blob-path"],
      description = ["The path the model blob can be downloaded from."],
      required = true,
    )
    shardModelBlobPath: String,
  ) {
    val request = createModelShardRequest {
      parent = dataProviderName
      modelShard = modelShard {
        modelRelease = shardModelRelease
        modelBlob = modelBlob { modelBlobPath = shardModelBlobPath }
      }
    }
    val outputModelShard =
      runBlocking(parentCommand.rpcDispatcher) { modelShardStub.createModelShard(request) }

    println("Model shard ${outputModelShard.name} has been created.")
    printModelShard(outputModelShard)
  }

  @Command(description = ["Lists model shards for a data provider."])
  fun list(
    @Option(
      names = ["--parent"],
      description = ["API resource name of the parent Event Data Provider."],
      required = true,
    )
    dataProviderName: String,
    @Option(
      names = ["--page-size"],
      description = ["The maximum number of ModelShards to return."],
      required = false,
      defaultValue = "0",
    )
    listPageSize: Int,
    @Option(
      names = ["--page-token"],
      description =
        [
          "A page token, received from a previous `ListModelShardsRequest` call. Provide this to retrieve the subsequent page."
        ],
      required = false,
      defaultValue = "",
    )
    listPageToken: String,
  ) {
    val request = listModelShardsRequest {
      parent = dataProviderName
      pageSize = listPageSize
      pageToken = listPageToken
    }
    val response =
      runBlocking(parentCommand.rpcDispatcher) { modelShardStub.listModelShards(request) }
    response.modelShardsList.forEach { printModelShard(it) }
  }

  @Command(description = ["Deletes model shard."])
  fun delete(
    @Option(
      names = ["--name"],
      description = ["API resource name of the ModelShard."],
      required = true,
    )
    modelShardName: String
  ) {
    val request = deleteModelShardRequest { name = modelShardName }

    runBlocking(parentCommand.rpcDispatcher) { modelShardStub.deleteModelShard(request) }

    println("Model shard $modelShardName has been deleted.")
  }

  private fun printModelShard(modelShard: ModelShard) {
    println("NAME - ${modelShard.name}")
    println("MODEL RELEASE- ${modelShard.modelRelease}")
    println("MODEL BLOB - ${modelShard.modelBlob}")
    println("CREATE TIME - ${modelShard.createTime}")
  }
}

@Command(name = "model-rollouts", subcommands = [CommandLine.HelpCommand::class])
private class ModelRollouts {
  @ParentCommand
  lateinit var parentCommand: MeasurementSystem
    private set

  val modelRolloutStub: ModelRolloutsCoroutineStub by lazy {
    ModelRolloutsCoroutineStub(parentCommand.kingdomChannel)
  }

  @Command(description = ["Creates model rollout."])
  fun create(
    @Option(
      names = ["--parent"],
      description = ["API resource name of the parent ModelLine."],
      required = true,
    )
    modelLineName: String,
    @Option(
      names = ["--rollout-start-date"],
      description = ["Start date of model rollout in ISO 8601 format of UTC"],
      required = false,
    )
    rolloutStartDate: LocalDate? = null,
    @Option(
      names = ["--rollout-end-date"],
      description = ["End date of model rollout in ISO 8601 format of UTC"],
      required = false,
    )
    rolloutEndDate: LocalDate? = null,
    @Option(
      names = ["--instant-rollout-date"],
      description = ["Instant rollout date of model rollout in ISO 8601 format of UTC"],
      required = false,
    )
    instantRolloutDate: LocalDate? = null,
    @Option(
      names = ["--model-release"],
      description = ["The `ModelRelease` this model rollout refers to."],
      required = true,
    )
    modelRolloutRelease: String,
  ) {

    if (instantRolloutDate == null && (rolloutStartDate == null || rolloutEndDate == null)) {
      throw ParameterException(
        parentCommand.commandLine,
        "Both `rolloutStartDate` and `rolloutEndDate` must be set when `instantRolloutDate` is not.",
      )
    }

    val request = createModelRolloutRequest {
      parent = modelLineName
      modelRollout = modelRollout {
        if (instantRolloutDate != null) {
          this.instantRolloutDate = instantRolloutDate.toProtoDate()
        } else {
          gradualRolloutPeriod = dateInterval {
            this.startDate = rolloutStartDate!!.toProtoDate()
            this.endDate = rolloutEndDate!!.toProtoDate()
          }
        }
        modelRelease = modelRolloutRelease
      }
    }
    val outputModelRollout =
      runBlocking(parentCommand.rpcDispatcher) { modelRolloutStub.createModelRollout(request) }

    println("Model rollout ${outputModelRollout.name} has been created.")
    printModelRollout(outputModelRollout)
  }

  @Command(description = ["Lists model rollouts for a model line."])
  fun list(
    @Option(
      names = ["--parent"],
      description = ["API resource name of the parent ModelLine."],
      required = true,
    )
    modelLineName: String,
    @Option(
      names = ["--page-size"],
      description = ["The maximum number of ModelRollouts to return."],
      required = false,
      defaultValue = "0",
    )
    listPageSize: Int,
    @Option(
      names = ["--page-token"],
      description =
        [
          "A page token, received from a previous ListModelRolloutsRequest call. Provide this to retrieve the subsequent page."
        ],
      required = false,
      defaultValue = "",
    )
    listPageToken: String,
    @Option(
      names = ["--rollout-period-overlapping-start-date"],
      description =
        ["Start date of overlapping period for desired model rollouts in ISO 8601 format of UTC"],
      required = false,
    )
    rolloutPeriodOverlappingStartDate: LocalDate? = null,
    @Option(
      names = ["--rollout-period-overlapping-end-date"],
      description =
        ["End date of overlapping period for desired model rollouts in ISO 8601 format of UTC"],
      required = false,
    )
    rolloutPeriodOverlappingEndDate: LocalDate? = null,
  ) {
    val request = listModelRolloutsRequest {
      parent = modelLineName
      pageSize = listPageSize
      pageToken = listPageToken
      if (rolloutPeriodOverlappingStartDate != null && rolloutPeriodOverlappingEndDate != null) {
        filter =
          ListModelRolloutsRequestKt.filter {
            rolloutPeriodOverlapping = dateInterval {
              this.startDate = rolloutPeriodOverlappingStartDate.toProtoDate()
              this.endDate = rolloutPeriodOverlappingEndDate.toProtoDate()
            }
          }
      }
    }
    val response =
      runBlocking(parentCommand.rpcDispatcher) { modelRolloutStub.listModelRollouts(request) }
    response.modelRolloutsList.forEach { printModelRollout(it) }
  }

  @Command(description = ["Schedule model rollout freeze time."])
  fun schedule(
    @Option(
      names = ["--name"],
      description = ["API resource name of the ModelRollout."],
      required = true,
    )
    modelRolloutName: String,
    @Option(
      names = ["--freeze-time"],
      description = ["The rollout freeze time to be set in ISO 8601 format of UTC."],
      required = true,
    )
    freezeDate: LocalDate,
  ) {
    val request = scheduleModelRolloutFreezeRequest {
      name = modelRolloutName
      this.rolloutFreezeDate = freezeDate.toProtoDate()
    }
    val outputModelRollout =
      runBlocking(parentCommand.rpcDispatcher) {
        modelRolloutStub.scheduleModelRolloutFreeze(request)
      }

    println(
      "Freeze date ${outputModelRollout.rolloutFreezeDate} has been set for ${outputModelRollout.name}."
    )
  }

  @Command(description = ["Deletes model rollout."])
  fun delete(
    @Option(
      names = ["--name"],
      description = ["API resource name of the ModelRollout."],
      required = true,
    )
    modelRolloutName: String
  ) {
    val request = deleteModelRolloutRequest { name = modelRolloutName }
    runBlocking(parentCommand.rpcDispatcher) { modelRolloutStub.deleteModelRollout(request) }

    println("Model rollout $modelRolloutName has been deleted.")
  }

  private fun printModelRollout(modelRollout: ModelRollout) {
    println("NAME - ${modelRollout.name}")
    if (modelRollout.hasInstantRolloutDate()) {
      println("INSTANT ROLLOUT DATE- ${modelRollout.instantRolloutDate}")
    } else {
      println("GRADUAL ROLLOUT PERIOD- ${modelRollout.gradualRolloutPeriod}")
    }
    println("ROLLOUT FREEZE DATE - ${modelRollout.rolloutFreezeDate}")
    println("PREVIOUS MODEL ROLLOUT - ${modelRollout.previousModelRollout}")
    println("MODEL RELEASE - ${modelRollout.modelRelease}")
    println("CREATE TIME - ${modelRollout.createTime}")
    println("UPDATE TIME - ${modelRollout.updateTime}")
  }
}

@Command(name = "model-suites", subcommands = [CommandLine.HelpCommand::class])
private class ModelSuites {
  @ParentCommand
  lateinit var parentCommand: MeasurementSystem
    private set

  val modelSuiteStub: ModelSuitesCoroutineStub by lazy {
    ModelSuitesCoroutineStub(parentCommand.kingdomChannel)
  }

  @Command(description = ["Creates model suite."])
  fun create(
    @Option(
      names = ["--parent"],
      description = ["API resource name of the parent ModelProvider."],
      required = true,
    )
    modelProviderName: String,
    @Option(
      names = ["--display-name"],
      description = ["Model suite display name."],
      required = true,
    )
    modelSuiteDisplayName: String,
    @Option(
      names = ["--description"],
      description = ["Model suite description."],
      required = false,
      defaultValue = "",
    )
    modelSuiteDescription: String,
  ) {
    val request = createModelSuiteRequest {
      parent = modelProviderName
      modelSuite = modelSuite {
        displayName = modelSuiteDisplayName
        description = modelSuiteDescription
      }
    }
    val outputModelSuite =
      runBlocking(parentCommand.rpcDispatcher) { modelSuiteStub.createModelSuite(request) }

    printModelSuite(outputModelSuite)
  }

  @Command(description = ["Gets model suite."])
  fun get(
    @Option(names = ["--name"], description = ["Model suite name."], required = true)
    modelSuiteName: String
  ) {
    val request = getModelSuiteRequest { name = modelSuiteName }
    val outputModelSuite =
      runBlocking(parentCommand.rpcDispatcher) { modelSuiteStub.getModelSuite(request) }
    printModelSuite(outputModelSuite)
  }

  @Command(description = ["Lists model suites for a model provider."])
  fun list(
    @Option(
      names = ["--parent"],
      description = ["API resource name of the parent ModelProvider."],
      required = true,
    )
    modelProviderName: String,
    @Option(
      names = ["--page-size"],
      description = ["The maximum number of ModelSuites to return."],
      required = false,
      defaultValue = "0",
    )
    listPageSize: Int,
    @Option(
      names = ["--page-token"],
      description =
        [
          "A page token, received from a previous `ListModelSuitesRequest` call. Provide this to retrieve the subsequent page."
        ],
      required = false,
      defaultValue = "",
    )
    listPageToken: String,
  ) {
    val request = listModelSuitesRequest {
      parent = modelProviderName
      pageSize = listPageSize
      pageToken = listPageToken
    }
    val response =
      runBlocking(parentCommand.rpcDispatcher) { modelSuiteStub.listModelSuites(request) }
    response.modelSuitesList.forEach { printModelSuite(it) }
  }

  private fun printModelSuite(modelSuite: ModelSuite) {
    println("NAME - ${modelSuite.name}")
    println("DISPLAY NAME - ${modelSuite.displayName}")
    if (modelSuite.description.isNotBlank()) {
      println("DESCRIPTION - ${modelSuite.description}")
    }
    println("CREATE TIME - ${modelSuite.createTime}")
  }
}
