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
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import io.grpc.ManagedChannel
import java.io.File
import java.security.SecureRandom
import java.security.SignatureException
import java.security.cert.CertPathValidatorException
import java.security.cert.X509Certificate
import java.time.Clock
import java.time.Duration as systemDuration
import java.time.Instant
import kotlin.properties.Delegates
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.Account
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ApiKey
import org.wfanet.measurement.api.v2alpha.ApiKeysGrpcKt.ApiKeysCoroutineStub
import org.wfanet.measurement.api.v2alpha.Certificate
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumer
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKt.DataProviderEntryKt as DataProviderEntries
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.Duration
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.Impression
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.ReachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.duration
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.PublicKey
import org.wfanet.measurement.api.v2alpha.PublicKeysGrpcKt.PublicKeysCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.EventGroupEntryKt as EventGroupEntries
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.activateAccountRequest
import org.wfanet.measurement.api.v2alpha.apiKey
import org.wfanet.measurement.api.v2alpha.authenticateRequest
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.createApiKeyRequest
import org.wfanet.measurement.api.v2alpha.createCertificateRequest
import org.wfanet.measurement.api.v2alpha.createMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.publicKey
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.revokeCertificateRequest
import org.wfanet.measurement.api.v2alpha.signedData
import org.wfanet.measurement.api.v2alpha.timeInterval
import org.wfanet.measurement.api.v2alpha.updatePublicKeyRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.api.withIdToken
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.tink.PrivateJwkHandle
import org.wfanet.measurement.common.crypto.tink.SelfIssuedIdTokens
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.readByteString
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

private val CHANNEL_SHUTDOWN_TIMEOUT = systemDuration.ofSeconds(30)

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
    ]
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

@Command(
  name = "accounts",
  subcommands = [CommandLine.HelpCommand::class],
)
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
      description = ["Self-issued OpenID Provider key as a binary Tink keyset"]
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
    val keysetHandle = CleartextKeysetHandle.read(BinaryKeysetReader.withFile(siopKey))
    val idToken: String =
      SelfIssuedIdTokens.generateIdToken(
        PrivateJwkHandle(keysetHandle),
        response.authenticationRequestUri,
        Clock.systemUTC()
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

@Command(
  name = "certificates",
  subcommands = [CommandLine.HelpCommand::class],
)
private class Certificates {
  @ParentCommand
  lateinit var parentCommand: MeasurementSystem
    private set

  @Option(
    names = ["--api-key"],
    paramLabel = "<apiKey>",
    description = ["API authentication key. Required when parent type is MeasurementConsumer."]
  )
  var authenticationKey: String? = null

  private val certificatesClient: CertificatesCoroutineStub by lazy {
    val client = CertificatesCoroutineStub(parentCommand.kingdomChannel)
    if (authenticationKey == null) client else client.withAuthenticationKey(authenticationKey)
  }

  @Command(
    description = ["Creates a Certificate resource"],
  )
  fun create(
    @Option(
      names = ["--parent"],
      paramLabel = "<parent>",
      required = true,
      description = ["Name of parent resource"]
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
        "API authentication key is required when parent type is MeasurementConsumer"
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

  @Command(
    description = ["Revokes a Certificate"],
  )
  fun revoke(
    @Option(
      names = ["--revocation-state"],
      paramLabel = "<revocationState>",
      required = true,
    )
    revocationState: Certificate.RevocationState,
    @Parameters(index = "0", paramLabel = "<name>", description = ["Resource name"]) name: String,
  ) {
    if (authenticationKey == null && MeasurementConsumerCertificateKey.fromName(name) != null) {
      throw ParameterException(
        parentCommand.commandLine,
        "API authentication key is required when parent type is MeasurementConsumer"
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

@Command(
  name = "public-keys",
  subcommands = [CommandLine.HelpCommand::class],
)
private class PublicKeys {
  @ParentCommand
  lateinit var parentCommand: MeasurementSystem
    private set

  @Option(
    names = ["--api-key"],
    paramLabel = "<apiKey>",
    description = ["API authentication key. Required when parent type is MeasurementConsumer."]
  )
  var authenticationKey: String? = null

  private val publicKeysClient: PublicKeysCoroutineStub by lazy {
    val client = PublicKeysCoroutineStub(parentCommand.kingdomChannel)
    if (authenticationKey == null) client else client.withAuthenticationKey(authenticationKey)
  }

  @Command(
    description = ["Updates a PublicKey resource"],
  )
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
    val request = updatePublicKeyRequest {
      this.publicKey = publicKey {
        this.name = name
        this.publicKey = signedData {
          data = publicKeyFile.readByteString()
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

@Command(
  name = "measurement-consumers",
  subcommands = [CommandLine.HelpCommand::class],
)
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
      ],
  )
  fun create(
    @Option(
      names = ["--creation-token"],
      paramLabel = "<creationToken>",
      required = true,
    )
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
    @Option(
      names = ["--display-name"],
      paramLabel = "<displayName>",
      defaultValue = "",
    )
    displayName: String,
    @Option(
      names = ["--id-token"],
      paramLabel = "<idToken>",
    )
    idTokenOption: String? = null,
  ) {
    // TODO(remkop/picocli#882): Use built-in Picocli functionality once available.
    val idToken: String = idTokenOption ?: String(System.console().readPassword("ID Token: "))

    val certificate: X509Certificate = certificateFile.inputStream().use { readCertificate(it) }
    val request = createMeasurementConsumerRequest {
      measurementConsumer = measurementConsumer {
        measurementConsumerCreationToken = creationToken
        certificateDer = certificate.encoded.toByteString()
        publicKey = signedData {
          data = publicKeyFile.readByteString()
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
    ]
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
}

@Command(name = "create", description = ["Creates a Single Measurement"])
class CreateMeasurement : Runnable {
  @ParentCommand private lateinit var parentCommand: Measurements

  @Option(
    names = ["--measurement-consumer"],
    description = ["API resource name of the MeasurementConsumer"],
    required = true
  )
  private lateinit var measurementConsumer: String

  @Option(
    names = ["--private-key-der-file"],
    description = ["Private key for MeasurementConsumer"],
    required = true
  )
  private lateinit var privateKeyDerFile: File

  @Option(
    names = ["--measurement-ref-id"],
    description = ["Measurement reference id"],
    required = false,
    defaultValue = ""
  )
  private lateinit var measurementIdempotencyKey: String

  @set:Option(
    names = ["--vid-sampling-start"],
    description = ["Start point of vid sampling interval"],
    required = true,
  )
  var vidSamplingStart by Delegates.notNull<Float>()
    private set

  @set:Option(
    names = ["--vid-sampling-width"],
    description = ["Width of vid sampling interval"],
    required = true,
  )
  var vidSamplingWidth by Delegates.notNull<Float>()
    private set

  @ArgGroup(
    exclusive = true,
    multiplicity = "1",
    heading = "Specify one of the measurement types with its params\n"
  )
  lateinit var measurementTypeParams: MeasurementTypeParams

  class MeasurementTypeParams {
    class ReachAndFrequencyParams {
      @Option(
        names = ["--reach-and-frequency"],
        description = ["Measurement Type of ReachAndFrequency"],
        required = true,
      )
      var selected = false
        private set

      @set:Option(
        names = ["--reach-privacy-epsilon"],
        description = ["Epsilon value of reach privacy params"],
        required = true,
      )
      var reachPrivacyEpsilon by Delegates.notNull<Double>()
        private set

      @set:Option(
        names = ["--reach-privacy-delta"],
        description = ["Delta value of reach privacy params"],
        required = true,
      )
      var reachPrivacyDelta by Delegates.notNull<Double>()
        private set

      @set:Option(
        names = ["--frequency-privacy-epsilon"],
        description = ["Epsilon value of frequency privacy params"],
        required = true,
      )
      var frequencyPrivacyEpsilon by Delegates.notNull<Double>()
        private set

      @set:Option(
        names = ["--frequency-privacy-delta"],
        description = ["Epsilon value of frequency privacy params"],
        required = true,
      )
      var frequencyPrivacyDelta by Delegates.notNull<Double>()
        private set

      @set:Option(
        names = ["--reach-max-frequency"],
        description = ["Maximum frequency per user"],
        required = false,
        defaultValue = "10",
      )
      var maximumFrequencyPerUser by Delegates.notNull<Int>()
        private set
    }

    class ImpressionParams {
      @Option(
        names = ["--impression"],
        description = ["Measurement Type of Impression"],
        required = true,
      )
      var selected = false
        private set

      @set:Option(
        names = ["--impression-privacy-epsilon"],
        description = ["Epsilon value of impression privacy params"],
        required = true,
      )
      var privacyEpsilon by Delegates.notNull<Double>()
        private set

      @set:Option(
        names = ["--impression-privacy-delta"],
        description = ["Epsilon value of impression privacy params"],
        required = true,
      )
      var privacyDelta by Delegates.notNull<Double>()
        private set

      @set:Option(
        names = ["--impression-max-frequency"],
        description = ["Maximum frequency per user"],
        required = true,
      )
      var maximumFrequencyPerUser by Delegates.notNull<Int>()
        private set
    }

    class DurationParams {
      @Option(
        names = ["--duration"],
        description = ["Measurement Type of Duration"],
        required = true,
      )
      var selected = false
        private set

      @set:Option(
        names = ["--duration-privacy-epsilon"],
        description = ["Epsilon value of duration privacy params"],
        required = true,
      )
      var privacyEpsilon by Delegates.notNull<Double>()
        private set

      @set:Option(
        names = ["--duration-privacy-delta"],
        description = ["Epsilon value of duration privacy params"],
        required = true,
      )
      var privacyDelta by Delegates.notNull<Double>()
        private set

      @set:Option(
        names = ["--max-duration"],
        description = ["Maximum watch duration per user"],
        required = true,
      )
      var maximumWatchDurationPerUser by Delegates.notNull<Int>()
        private set
    }

    @ArgGroup(exclusive = false, heading = "Measurement type ReachAndFrequency and params\n")
    var reachAndFrequency = ReachAndFrequencyParams()
    @ArgGroup(exclusive = false, heading = "Measurement type Impression and params\n")
    var impression = ImpressionParams()
    @ArgGroup(exclusive = false, heading = "Measurement type Duration and params\n")
    var duration = DurationParams()
  }

  private fun getReachAndFrequency(): ReachAndFrequency {
    return reachAndFrequency {
      reachPrivacyParams = differentialPrivacyParams {
        epsilon = measurementTypeParams.reachAndFrequency.reachPrivacyEpsilon
        delta = measurementTypeParams.reachAndFrequency.reachPrivacyDelta
      }
      frequencyPrivacyParams = differentialPrivacyParams {
        epsilon = measurementTypeParams.reachAndFrequency.frequencyPrivacyEpsilon
        delta = measurementTypeParams.reachAndFrequency.frequencyPrivacyDelta
      }
      maximumFrequencyPerUser = measurementTypeParams.reachAndFrequency.maximumFrequencyPerUser
    }
  }

  private fun getImpression(): Impression {
    return impression {
      privacyParams = differentialPrivacyParams {
        epsilon = measurementTypeParams.impression.privacyEpsilon
        delta = measurementTypeParams.impression.privacyDelta
      }
      maximumFrequencyPerUser = measurementTypeParams.impression.maximumFrequencyPerUser
    }
  }

  private fun getDuration(): Duration {
    return duration {
      privacyParams = differentialPrivacyParams {
        epsilon = measurementTypeParams.duration.privacyEpsilon
        delta = measurementTypeParams.duration.privacyDelta
      }
      maximumWatchDurationPerUser = measurementTypeParams.duration.maximumWatchDurationPerUser
    }
  }

  @ArgGroup(exclusive = false, multiplicity = "1..*", heading = "Add DataProviders\n")
  private lateinit var dataProviderInputs: List<DataProviderInput>

  class DataProviderInput {
    @Option(
      names = ["--data-provider"],
      description = ["API resource name of the DataProvider"],
      required = true,
    )
    lateinit var name: String
      private set

    @ArgGroup(
      exclusive = false,
      multiplicity = "1..*",
      heading = "Add EventGroups for a DataProvider\n"
    )
    lateinit var eventGroupInputs: List<EventGroupInput>
      private set
  }

  class EventGroupInput {
    @Option(
      names = ["--event-group"],
      description = ["API resource name of the EventGroup"],
      required = true,
    )
    lateinit var name: String
      private set

    @Option(
      names = ["--event-filter"],
      description = ["Raw CEL expression of EventFilter"],
      required = false,
      defaultValue = ""
    )
    lateinit var eventFilter: String
      private set

    @Option(
      names = ["--event-start-time"],
      description = ["Start time of Event range in ISO 8601 format of UTC"],
      required = true,
    )
    lateinit var eventStartTime: Instant
      private set

    @Option(
      names = ["--event-end-time"],
      description = ["End time of Event range in ISO 8601 format of UTC"],
      required = true,
    )
    lateinit var eventEndTime: Instant
      private set
  }

  private val secureRandom = SecureRandom.getInstance("SHA1PRNG")

  private fun getDataProviderEntry(
    dataProviderInput: DataProviderInput,
    measurementConsumerSigningKey: SigningKeyHandle,
    measurementEncryptionPublicKey: ByteString
  ): Measurement.DataProviderEntry {
    return dataProviderEntry {
      val requisitionSpec = requisitionSpec {
        eventGroups +=
          dataProviderInput.eventGroupInputs.map {
            eventGroupEntry {
              key = it.name
              value =
                EventGroupEntries.value {
                  collectionInterval = timeInterval {
                    startTime = it.eventStartTime.toProtoTime()
                    endTime = it.eventEndTime.toProtoTime()
                  }
                  if (it.eventFilter.isNotEmpty())
                    filter = eventFilter { expression = it.eventFilter }
                }
            }
          }
        this.measurementPublicKey = measurementEncryptionPublicKey
        nonce = secureRandom.nextLong()
      }

      key = dataProviderInput.name
      val dataProvider =
        runBlocking(parentCommand.parentCommand.rpcDispatcher) {
          parentCommand.dataProviderStub
            .withAuthenticationKey(parentCommand.apiAuthenticationKey)
            .getDataProvider(getDataProviderRequest { name = dataProviderInput.name })
        }
      value =
        DataProviderEntries.value {
          dataProviderCertificate = dataProvider.certificate
          dataProviderPublicKey = dataProvider.publicKey
          encryptedRequisitionSpec =
            encryptRequisitionSpec(
              signRequisitionSpec(requisitionSpec, measurementConsumerSigningKey),
              EncryptionPublicKey.parseFrom(dataProvider.publicKey.data)
            )
          nonceHash = hashSha256(requisitionSpec.nonce)
        }
    }
  }

  override fun run() {
    val measurementConsumer =
      runBlocking(parentCommand.parentCommand.rpcDispatcher) {
        parentCommand.measurementConsumerStub
          .withAuthenticationKey(parentCommand.apiAuthenticationKey)
          .getMeasurementConsumer(getMeasurementConsumerRequest { name = measurementConsumer })
      }
    val measurementConsumerCertificate = readCertificate(measurementConsumer.certificateDer)
    val measurementConsumerPrivateKey =
      readPrivateKey(
        privateKeyDerFile.readByteString(),
        measurementConsumerCertificate.publicKey.algorithm
      )
    val measurementConsumerSigningKey =
      SigningKeyHandle(measurementConsumerCertificate, measurementConsumerPrivateKey)
    val measurementEncryptionPublicKey = measurementConsumer.publicKey.data

    val measurement = measurement {
      this.measurementConsumerCertificate = measurementConsumer.certificate
      dataProviders +=
        dataProviderInputs.map {
          getDataProviderEntry(it, measurementConsumerSigningKey, measurementEncryptionPublicKey)
        }
      val unsignedMeasurementSpec = measurementSpec {
        measurementPublicKey = measurementEncryptionPublicKey
        nonceHashes += this@measurement.dataProviders.map { it.value.nonceHash }
        vidSamplingInterval = vidSamplingInterval {
          start = vidSamplingStart
          width = vidSamplingWidth
        }
        if (measurementTypeParams.reachAndFrequency.selected) {
          reachAndFrequency = getReachAndFrequency()
        } else if (measurementTypeParams.impression.selected) {
          impression = getImpression()
        } else if (measurementTypeParams.duration.selected) {
          duration = getDuration()
        }
      }

      this.measurementSpec =
        signMeasurementSpec(unsignedMeasurementSpec, measurementConsumerSigningKey)
      measurementReferenceId = measurementIdempotencyKey
    }

    val response =
      runBlocking(parentCommand.parentCommand.rpcDispatcher) {
        parentCommand.measurementStub
          .withAuthenticationKey(parentCommand.apiAuthenticationKey)
          .createMeasurement(createMeasurementRequest { this.measurement = measurement })
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

    response.measurementList.forEach {
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

  @Parameters(
    index = "0",
    description = ["API resource name of the Measurement"],
  )
  private lateinit var measurementName: String

  @Option(
    names = ["--encryption-private-key-file"],
    description = ["MeasurementConsumer's EncryptionPrivateKey"],
    required = true
  )
  private lateinit var privateKeyDerFile: File

  private val privateKeyHandle: PrivateKeyHandle by lazy { loadPrivateKey(privateKeyDerFile) }

  private fun printMeasurementState(measurement: Measurement) {
    if (measurement.state == Measurement.State.FAILED) {
      println("State: FAILED - " + measurement.failure.reason + ": " + measurement.failure.message)
    } else {
      println("State: ${measurement.state}")
    }
  }

  private fun getMeasurementResult(
    resultPair: Measurement.ResultPair,
  ): Measurement.Result {
    val certificate = runBlocking {
      parentCommand.certificateStub
        .withAuthenticationKey(parentCommand.apiAuthenticationKey)
        .getCertificate(getCertificateRequest { name = resultPair.certificate })
    }

    val signedResult = decryptResult(resultPair.encryptedResult, privateKeyHandle)
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
    return Measurement.Result.parseFrom(signedResult.data)
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
  }

  override fun run() {
    val measurement =
      runBlocking(parentCommand.parentCommand.rpcDispatcher) {
        parentCommand.measurementStub
          .withAuthenticationKey(parentCommand.apiAuthenticationKey)
          .getMeasurement(getMeasurementRequest { name = measurementName })
      }

    printMeasurementState(measurement)
    if (measurement.state == Measurement.State.SUCCEEDED) {
      measurement.resultsList.forEach {
        val result = getMeasurementResult(it)
        printMeasurementResult(result)
      }
    }
  }
}

@Command(
  name = "api-keys",
  subcommands = [CommandLine.HelpCommand::class],
)
private class ApiKeys {
  @ParentCommand
  lateinit var parentCommand: MeasurementSystem
    private set

  private val apiKeysClient: ApiKeysCoroutineStub by lazy {
    ApiKeysCoroutineStub(parentCommand.kingdomChannel)
  }

  @Option(
    names = ["--id-token"],
    paramLabel = "<idToken>",
  )
  private var idTokenOption: String? = null

  @Command(
    description = ["Creates an ApiKey resource"],
  )
  fun create(
    @Option(
      names = ["--measurement-consumer"],
      paramLabel = "<measurementConsumer>",
      description = ["API resource name of the MeasurementConsumer"],
      required = true
    )
    measurementConsumer: String,
    @Option(
      names = ["--nickname"],
      paramLabel = "<nickname>",
      required = true,
    )
    nickname: String,
    @Option(
      names = ["--description"],
      paramLabel = "<description>",
    )
    description: String?,
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
