// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.api.v2alpha.tools

import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.protobuf.timestamp
import io.grpc.ManagedChannel
import java.io.File
import java.security.SecureRandom
import java.time.Duration as JavaDuration
import java.time.Instant
import kotlin.properties.Delegates
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKt.DataProviderEntryKt.value as dataProviderEntryValue
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.Duration
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.Impression
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.ReachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.duration
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.EventGroupEntryKt.value as eventGroupEntryValue
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.timeInterval
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.tink.testing.loadPrivateKey
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.verifyResult
import picocli.CommandLine

private class ApiFlags {
  @CommandLine.Option(
    names = ["--kingdom-public-api-target"],
    description = ["gRPC target (authority) of the Kingdom public API server"],
    required = true,
  )
  lateinit var apiTarget: String
    private set

  @CommandLine.Option(
    names = ["--kingdom-public-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Kingdom public API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --kingdom-public-api-target.",
      ],
    required = false,
  )
  var apiCertHost: String? = null
    private set
}

@CommandLine.Command(name = "create", description = ["Creates a Single Measurement"])
class CreateCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: SimpleReport

  @CommandLine.Option(
    names = ["--measurement-consumer"],
    description = ["API resource name of the MeasurementConsumer"],
    required = true
  )
  private lateinit var measurementConsumer: String

  @CommandLine.Option(
    names = ["--private-key-der-file"],
    description = ["Private key for MeasurementConsumer"],
    required = true
  )
  private lateinit var privateKeyDerFile: File

  @CommandLine.Option(
    names = ["--measurement-ref-id"],
    description = ["Measurement reference id"],
    required = false,
    defaultValue = ""
  )
  private lateinit var measurementReferenceId: String

  @set:CommandLine.Option(
    names = ["--vid-sampling-start"],
    description = ["Start point of vid sampling interval"],
    required = true,
  )
  var vidSamplingStart by Delegates.notNull<Float>()
    private set

  @set:CommandLine.Option(
    names = ["--vid-sampling-width"],
    description = ["Width of vid sampling interval"],
    required = true,
  )
  var vidSamplingWidth by Delegates.notNull<Float>()
    private set

  @CommandLine.ArgGroup(
    exclusive = true,
    multiplicity = "1",
    heading = "Specify one of the measurement types with its params\n"
  )
  lateinit var measurementTypeParams: MeasurementTypeParams

  class MeasurementTypeParams {
    class ReachAndFrequencyParams {
      @CommandLine.Option(
        names = ["--reach-and-frequency"],
        description = ["Measurement Type of ReachAndFrequency"],
        required = true,
      )
      var selected = false
        private set

      @set:CommandLine.Option(
        names = ["--reach-privacy-epsilon"],
        description = ["Epsilon value of reach privacy params"],
        required = true,
      )
      var reachPrivacyEpsilon by Delegates.notNull<Double>()
        private set

      @set:CommandLine.Option(
        names = ["--reach-privacy-delta"],
        description = ["Delta value of reach privacy params"],
        required = true,
      )
      var reachPrivacyDelta by Delegates.notNull<Double>()
        private set

      @set:CommandLine.Option(
        names = ["--frequency-privacy-epsilon"],
        description = ["Epsilon value of frequency privacy params"],
        required = true,
      )
      var frequencyPrivacyEpsilon by Delegates.notNull<Double>()
        private set

      @set:CommandLine.Option(
        names = ["--frequency-privacy-delta"],
        description = ["Epsilon value of frequency privacy params"],
        required = true,
      )
      var frequencyPrivacyDelta by Delegates.notNull<Double>()
        private set
    }

    class ImpressionParams {
      @CommandLine.Option(
        names = ["--impression"],
        description = ["Measurement Type of Impression"],
        required = true,
      )
      var selected = false
        private set

      @set:CommandLine.Option(
        names = ["--impression-privacy-epsilon"],
        description = ["Epsilon value of impression privacy params"],
        required = true,
      )
      var privacyEpsilon by Delegates.notNull<Double>()
        private set

      @set:CommandLine.Option(
        names = ["--impression-privacy-delta"],
        description = ["Epsilon value of impression privacy params"],
        required = true,
      )
      var privacyDelta by Delegates.notNull<Double>()
        private set

      @set:CommandLine.Option(
        names = ["--max-frequency"],
        description = ["Maximum frequency per user"],
        required = true,
      )
      var maximumFrequencyPerUser by Delegates.notNull<Int>()
        private set
    }

    class DurationParams {
      @CommandLine.Option(
        names = ["--duration"],
        description = ["Measurement Type of Duration"],
        required = true,
      )
      var selected = false
        private set

      @set:CommandLine.Option(
        names = ["--duration-privacy-epsilon"],
        description = ["Epsilon value of duration privacy params"],
        required = true,
      )
      var privacyEpsilon by Delegates.notNull<Double>()
        private set

      @set:CommandLine.Option(
        names = ["--duration-privacy-delta"],
        description = ["Epsilon value of duration privacy params"],
        required = true,
      )
      var privacyDelta by Delegates.notNull<Double>()
        private set

      @set:CommandLine.Option(
        names = ["--max-duration"],
        description = ["Maximum watch duration per user"],
        required = true,
      )
      var maximumWatchDurationPerUser by Delegates.notNull<Int>()
        private set
    }

    @CommandLine.ArgGroup(
      exclusive = false,
      heading = "Measurement type ReachAndFrequency and params\n"
    )
    var reachAndFrequency = ReachAndFrequencyParams()
    @CommandLine.ArgGroup(exclusive = false, heading = "Measurement type Impression and params\n")
    var impression = ImpressionParams()
    @CommandLine.ArgGroup(exclusive = false, heading = "Measurement type Duration and params\n")
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

  private fun convertToTimestamp(instant: Instant): Timestamp {
    return timestamp {
      seconds = instant.epochSecond
      nanos = instant.nano
    }
  }

  @CommandLine.ArgGroup(exclusive = false, multiplicity = "1..*", heading = "Add DataProviders\n")
  private lateinit var dataProviderInputs: List<DataProviderInput>

  class DataProviderInput {
    @CommandLine.Option(
      names = ["--data-provider"],
      description = ["API resource name of the DataProvider"],
      required = true,
    )
    lateinit var name: String
      private set

    @CommandLine.ArgGroup(
      exclusive = false,
      multiplicity = "1..*",
      heading = "Add EventGroups for a DataProvider\n"
    )
    lateinit var eventGroupInputs: List<EventGroupInput>
      private set
  }

  class EventGroupInput {
    @CommandLine.Option(
      names = ["--event-group"],
      description = ["API resource name of the EventGroup"],
      required = true,
    )
    lateinit var name: String
      private set

    @CommandLine.Option(
      names = ["--event-filter"],
      description = ["Raw CEL expression of EventFilter"],
      required = false,
      defaultValue = ""
    )
    lateinit var eventFilter: String
      private set

    @CommandLine.Option(
      names = ["--event-start-time"],
      description = ["Start time of Event range in ISO 8601 format of UTC"],
      required = true,
    )
    lateinit var eventStartTime: Instant
      private set

    @CommandLine.Option(
      names = ["--event-end-time"],
      description = ["End time of Event range in ISO 8601 format of UTC"],
      required = true,
    )
    lateinit var eventEndTime: Instant
      private set
  }

  private val secureRandom = SecureRandom.getInstance("SHA1PRNG")

  private fun getDataProviderEntry(
    dataProviderStub: DataProvidersCoroutineStub,
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
              value = eventGroupEntryValue {
                collectionInterval = timeInterval {
                  startTime = convertToTimestamp(it.eventStartTime)
                  endTime = convertToTimestamp(it.eventEndTime)
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
        runBlocking(Dispatchers.IO) {
          dataProviderStub
            .withAuthenticationKey(parent.apiAuthenticationKey)
            .getDataProvider(getDataProviderRequest { name = dataProviderInput.name })
        }
      value = dataProviderEntryValue {
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
    val measurementConsumerStub = MeasurementConsumersCoroutineStub(parent.channel)
    val measurementStub = MeasurementsCoroutineStub(parent.channel)
    val dataProviderStub = DataProvidersCoroutineStub(parent.channel)

    val measurementConsumer =
      runBlocking(Dispatchers.IO) {
        measurementConsumerStub
          .withAuthenticationKey(parent.apiAuthenticationKey)
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
          getDataProviderEntry(
            dataProviderStub,
            it,
            measurementConsumerSigningKey,
            measurementEncryptionPublicKey
          )
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
        } else duration = getDuration()
      }

      this.measurementSpec =
        signMeasurementSpec(unsignedMeasurementSpec, measurementConsumerSigningKey)
      measurementReferenceId = this@CreateCommand.measurementReferenceId
    }

    val response =
      runBlocking(Dispatchers.IO) {
        measurementStub
          .withAuthenticationKey(parent.apiAuthenticationKey)
          .createMeasurement(createMeasurementRequest { this.measurement = measurement })
      }
    print(response)
  }
}

@CommandLine.Command(name = "list", description = ["Lists Measurements"])
class ListCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: SimpleReport

  @CommandLine.Option(
    names = ["--measurement-consumer"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  override fun run() {
    val measurementStub = MeasurementsCoroutineStub(parent.channel)
    val response =
      runBlocking(Dispatchers.IO) {
        measurementStub
          .withAuthenticationKey(parent.apiAuthenticationKey)
          .listMeasurements(listMeasurementsRequest { parent = measurementConsumerName })
      }

    response.measurementList.map {
      if (it.state == Measurement.State.FAILED) {
        println(it.name + " FAILED - " + it.failure.reason + ": " + it.failure.message)
      } else {
        println(it.name + " " + it.state)
      }
    }
  }
}

@CommandLine.Command(name = "get", description = ["Gets a Single Measurement"])
class GetCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: SimpleReport

  @CommandLine.Option(
    names = ["--measurement"],
    description = ["API resource name of the Measurement"],
    required = true,
  )
  private lateinit var measurementName: String

  @CommandLine.Option(
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
    certificateStub: CertificatesCoroutineStub
  ): Measurement.Result {
    val certificate = runBlocking {
      certificateStub
        .withAuthenticationKey(parent.apiAuthenticationKey)
        .getCertificate(getCertificateRequest { name = resultPair.certificate })
    }

    val signedResult = decryptResult(resultPair.encryptedResult, privateKeyHandle)

    val result = Measurement.Result.parseFrom(signedResult.data)

    if (!verifyResult(signedResult.signature, result, readCertificate(certificate.x509Der))) {
      error("Signature of the result is invalid.")
    }
    return result
  }

  private fun printMeasurementResult(result: Measurement.Result) {
    if (result.hasReach()) println("Reach - ${result.reach.value}")
    if (result.hasFrequency()) {
      println("Frequency - ")
      result.frequency.relativeFrequencyDistributionMap.forEach {
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
    val measurementStub = MeasurementsCoroutineStub(parent.channel)
    val certificateStub = CertificatesCoroutineStub(parent.channel)
    val measurement =
      runBlocking(Dispatchers.IO) {
        measurementStub
          .withAuthenticationKey(parent.apiAuthenticationKey)
          .getMeasurement(getMeasurementRequest { name = measurementName })
      }

    printMeasurementState(measurement)
    if (measurement.state == Measurement.State.SUCCEEDED) {
      measurement.resultsList.map {
        val result = getMeasurementResult(it, certificateStub)
        printMeasurementResult(result)
      }
    }
  }
}

@CommandLine.Command(
  name = "SimpleReport",
  description = ["Simple report from Kingdom"],
  sortOptions = false,
  subcommands =
    [
      CommandLine.HelpCommand::class,
      CreateCommand::class,
      ListCommand::class,
      GetCommand::class,
    ]
)
class SimpleReport : Runnable {
  @CommandLine.Mixin private lateinit var tlsFlags: TlsFlags
  @CommandLine.Mixin private lateinit var apiFlags: ApiFlags

  @CommandLine.Option(
    names = ["--api-key"],
    description = ["API authentication key for the MeasurementConsumer"],
    required = true,
  )
  lateinit var apiAuthenticationKey: String
    private set

  val channel: ManagedChannel by lazy {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = tlsFlags.certFile,
        privateKeyFile = tlsFlags.privateKeyFile,
        trustedCertCollectionFile = tlsFlags.certCollectionFile
      )
    buildMutualTlsChannel(apiFlags.apiTarget, clientCerts, apiFlags.apiCertHost)
      .withShutdownTimeout(JavaDuration.ofSeconds(1))
  }
  override fun run() {}
}

/**
 * Create, list or get a `Measurement` by calling the public Kingdom API.
 *
 * Use the `help` command to see usage details.
 */
fun main(args: Array<String>) = commandLineMain(SimpleReport(), args)
