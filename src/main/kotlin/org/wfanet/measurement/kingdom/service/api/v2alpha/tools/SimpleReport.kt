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

package org.wfanet.measurement.kingdom.service.api.v2alpha.tools

import com.google.protobuf.ByteString
import com.google.protobuf.timestamp
import io.grpc.Channel
import java.io.File
import kotlin.random.Random
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
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
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.timeInterval
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
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.verifyResult
import picocli.CommandLine

class ApiFlags {
  @CommandLine.Option(
    names = ["--api-target"],
    description = ["gRPC target (authority) of the Kingdom public API server"],
    required = true,
  )
  lateinit var apiTarget: String
    private set

  @CommandLine.Option(
    names = ["--api-cert-host"],
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
    names = ["--measurement-consumer-name"],
    description = ["API resource name of the MeasurementConsumer"],
    required = true
  )
  private lateinit var measurementConsumerName: String

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
  )
  private var measurementRefId: String? = null

  // Measurement type params
  enum class MeasurementType {
    REACH_AND_FREQUENCY,
    IMPRESSION,
    DURATION,
  }

  class MeasurementParams {
    @CommandLine.Option(
      names = ["--type"],
      description = ["Measurement Type from one of (\${COMPLETION-CANDIDATES})"],
      required = true,
    )
    lateinit var type: MeasurementType

    @CommandLine.Option(
      names = ["--reach-privacy-epsilon"],
      description = ["Epsilon value of reach privacy params"],
      defaultValue = "1.0",
      required = false,
    )
    var reachPrivacyEpsilon: Double = 0.0

    @CommandLine.Option(
      names = ["--reach-privacy-delta"],
      defaultValue = "1.0",
      description = ["Delta value of reach privacy params"],
      required = false,
    )
    var reachPrivacyDelta: Double = 0.0

    @CommandLine.Option(
      names = ["--frequency-privacy-epsilon"],
      defaultValue = "1.0",
      description = ["Epsilon value of frequency privacy params"],
      required = false,
    )
    var frequencyPrivacyEpsilon: Double = 0.0

    @CommandLine.Option(
      names = ["--frequency-privacy-delta"],
      defaultValue = "1.0",
      description = ["Epsilon value of frequency privacy params"],
      required = false,
    )
    var frequencyPrivacyDelta: Double = 0.0

    @CommandLine.Option(
      names = ["--privacy-epsilon"],
      defaultValue = "1.0",
      description = ["Epsilon value of privacy params"],
      required = false,
    )
    var privacyEpsilon: Double = 0.0

    @CommandLine.Option(
      names = ["--privacy-delta"],
      defaultValue = "1.0",
      description = ["Epsilon value of privacy params"],
      required = false,
    )
    var privacyDelta: Double = 0.0

    @CommandLine.Option(
      names = ["--vid-sampling-start"],
      defaultValue = "0.0",
      description = ["Start point of vid sampling interval"],
      required = false,
    )
    var vidSamplingStart: Float = 0.0F

    @CommandLine.Option(
      names = ["--vid-sampling-width"],
      defaultValue = "1.0",
      description = ["Width of vid sampling interval"],
      required = false,
    )
    var vidSamplingWidth: Float = 0.0F

    @CommandLine.Option(
      names = ["--max-frequency"],
      defaultValue = "1",
      description = ["Maximum frequency per user"],
      required = false,
    )
    var maximumFrequencyPerUser: Int = 0

    @CommandLine.Option(
      names = ["--max-duration"],
      defaultValue = "1",
      description = ["Maximum watch duration per user"],
      required = false,
    )
    var maximumWatchDurationPerUser: Int = 0
  }
  @CommandLine.Mixin lateinit var measurementParams: MeasurementParams

  class EventGroupInput {
    @CommandLine.Option(
      names = ["--event-group-name"],
      description = ["API resource name of the EventGroup"],
      required = true,
    )
    lateinit var eventGroupName: String

    @CommandLine.Option(
      names = ["--event-filter-expression"],
      description = ["Raw CEL expression of EventFilter"],
      required = false,
    )
    lateinit var eventFilter: String

    @CommandLine.Option(
      names = ["--event-filter-start-time"],
      description = ["Start time of EventFilter range"],
      required = false,
    )
    var eventFilterStartTime: Long = 0L

    @CommandLine.Option(
      names = ["--event-filter-end-time"],
      description = ["End time of EventFilter range"],
      required = false,
    )
    var eventFilterEndTime: Long = 0L
  }

  class DataProviderInput {
    @CommandLine.Option(
      names = ["--data-provider-name"],
      description = ["API resource name of the DataProvider"],
      required = true,
    )
    lateinit var dataProviderName: String

    @CommandLine.ArgGroup(
      exclusive = false,
      multiplicity = "1..*",
      heading = "Add EventGroups for a DataProvider\n"
    )
    lateinit var eventGroupInputs: List<EventGroupInput>
  }

  @CommandLine.ArgGroup(exclusive = false, multiplicity = "1..*", heading = "Add DataProviders\n")
  private lateinit var dataProviderInputs: List<DataProviderInput>

  private fun getDataProviderEntry(
    dataProviderStub: DataProvidersCoroutineStub,
    it: DataProviderInput,
    measurementConsumerSigningKey: SigningKeyHandle,
    measurementEncryptionPublicKey: ByteString
  ): Measurement.DataProviderEntry {
    return dataProviderEntry {
      val requisitionSpec = requisitionSpec {
        eventGroups.addAll(
          it.eventGroupInputs.map {
            eventGroupEntry {
              key = it.eventGroupName
              value = eventGroupEntryValue {
                collectionInterval = timeInterval {
                  startTime = timestamp { seconds = it.eventFilterStartTime }
                  endTime = timestamp { seconds = it.eventFilterEndTime }
                }
                filter = eventFilter { expression = it.eventFilter }
              }
            }
          }
        )
        this.measurementPublicKey = measurementEncryptionPublicKey
        nonce = Random.Default.nextLong()
      }

      key = it.dataProviderName
      val dataProvider =
        runBlocking {
          dataProviderStub.getDataProvider(getDataProviderRequest { name = it.dataProviderName })
        }
      value = dataProviderEntryValue {
        dataProviderCertificate = dataProvider.certificate
        dataProviderPublicKey = dataProvider.publicKey
        dataProviderCertificate = "${it.dataProviderName}/certificates/1"
        encryptedRequisitionSpec =
          encryptRequisitionSpec(
            signRequisitionSpec(requisitionSpec, measurementConsumerSigningKey),
            encryptionPublicKey {
              format = EncryptionPublicKey.Format.TINK_KEYSET
              data = dataProvider.publicKey.data
            }
          )
        nonceHash = hashSha256(requisitionSpec.nonce)
      }
    }
  }

  private fun getReachAndFrequency(): ReachAndFrequency {
    return reachAndFrequency {
      reachPrivacyParams = differentialPrivacyParams {
        epsilon = measurementParams.reachPrivacyEpsilon
        delta = measurementParams.reachPrivacyDelta
      }
      frequencyPrivacyParams = differentialPrivacyParams {
        epsilon = measurementParams.frequencyPrivacyEpsilon
        delta = measurementParams.frequencyPrivacyDelta
      }
      vidSamplingInterval = vidSamplingInterval {
        start = measurementParams.vidSamplingStart
        width = measurementParams.vidSamplingWidth
      }
    }
  }

  private fun getImpression(): Impression {
    return impression {
      privacyParams = differentialPrivacyParams {
        epsilon = measurementParams.privacyEpsilon
        delta = measurementParams.privacyDelta
      }
      maximumFrequencyPerUser = measurementParams.maximumFrequencyPerUser
    }
  }

  private fun getDuration(): Duration {
    return duration {
      privacyParams = differentialPrivacyParams {
        epsilon = measurementParams.privacyEpsilon
        delta = measurementParams.privacyDelta
      }
      maximumWatchDurationPerUser = measurementParams.maximumWatchDurationPerUser
    }
  }

  override fun run() {
    val measurementConsumerStub = MeasurementConsumersCoroutineStub(parent.channel)
    val measurementStub = MeasurementsCoroutineStub(parent.channel)
    val dataProviderStub = DataProvidersCoroutineStub(parent.channel)

    val measurementConsumer =
      runBlocking {
        measurementConsumerStub.getMeasurementConsumer(
          getMeasurementConsumerRequest { name = measurementConsumerName }
        )
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
      dataProviders.addAll(
        dataProviderInputs.map {
          getDataProviderEntry(
            dataProviderStub,
            it,
            measurementConsumerSigningKey,
            measurementEncryptionPublicKey
          )
        }
      )
      val unsignedMeasurementSpec = measurementSpec {
        measurementPublicKey = measurementEncryptionPublicKey
        nonceHashes.addAll(this@measurement.dataProviders.map { it.value.nonceHash })
        when (measurementParams.type) {
          MeasurementType.REACH_AND_FREQUENCY -> {
            reachAndFrequency = getReachAndFrequency()
          }
          MeasurementType.IMPRESSION -> {
            impression = getImpression()
          }
          MeasurementType.DURATION -> {
            duration = getDuration()
          }
        }
      }
      this.measurementSpec =
        signMeasurementSpec(unsignedMeasurementSpec, measurementConsumerSigningKey)
      measurementReferenceId = measurementRefId ?: ""
    }

    val response =
      runBlocking {
        measurementStub.createMeasurement(
          createMeasurementRequest { this.measurement = measurement }
        )
      }
    print(response.toJson())
  }
}

@CommandLine.Command(name = "list", description = ["Lists Measurements"])
class ListCommand : Runnable {
  @CommandLine.ParentCommand private lateinit var parent: SimpleReport

  @CommandLine.Option(
    names = ["--mc-name"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  override fun run() {
    val measurementStub = MeasurementsCoroutineStub(parent.channel)
    val response =
      runBlocking {
        measurementStub.listMeasurements(
          listMeasurementsRequest { parent = measurementConsumerName }
        )
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
    names = ["--measurement-name"],
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

  private val privateKeyHandle: PrivateKeyHandle by lazy {
    loadPrivateKey(privateKeyDerFile)
  }


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
      certificateStub.getCertificate(getCertificateRequest { name = resultPair.certificate })
    }

    val signedResult =
      decryptResult(resultPair.encryptedResult, privateKeyHandle)

    val result = Measurement.Result.parseFrom(signedResult.data)

    if (!verifyResult(signedResult.signature, result, readCertificate(certificate.x509Der))) {
      error("Signature of the result is invalid.")
    }
    return result
  }

  private fun printMeasurementResult(result: Measurement.Result) {
    if (result.hasReach())  println("Reach - ${result.reach.value}")
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
      println("WatchDuration - ${result.watchDuration.value.seconds} seconds ${result.watchDuration.value.nanos} nanos")
    }
  }

  override fun run() {
    val measurementStub = MeasurementsCoroutineStub(parent.channel)
    val certificateStub = CertificatesCoroutineStub(parent.channel)
    val measurement = runBlocking {
      measurementStub.getMeasurement(getMeasurementRequest { name = measurementName })
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

  val channel: Channel by lazy {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = tlsFlags.certFile,
        privateKeyFile = tlsFlags.privateKeyFile,
        trustedCertCollectionFile = tlsFlags.certCollectionFile
      )
    buildMutualTlsChannel(apiFlags.apiTarget, clientCerts, apiFlags.apiCertHost)
      .withVerboseLogging(true)
  }
  override fun run() {}
}

/**
 * Creates resources within the Kingdom.
 *
 * Use the `help` command to see usage details.
 */
fun main(args: Array<String>) = commandLineMain(SimpleReport(), args)
