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
import io.grpc.Channel
import java.io.File
import java.time.Instant
import kotlin.random.Random
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EncryptionPublicKey
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKt.DataProviderEntryKt.value as dataProviderEntryValue
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.EventGroupEntryKt.value as eventGroupEntryValue
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.encryptionPublicKey
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.timeInterval
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.hashSha256
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import picocli.CommandLine

class ApiFlags {
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
  )
  private lateinit var measurementReferenceId: String

  @CommandLine.ArgGroup(exclusive = false, multiplicity = "1..*", heading = "Add DataProviders\n")
  private lateinit var dataProviderInputs: List<DataProviderInput>

  private fun convertToTimestamp(instant: Instant): Timestamp {
    return timestamp {
      seconds = instant.epochSecond
      nanos = instant.nano
    }
  }

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

  private fun getDataProviderEntry(
    dataProviderStub: DataProvidersCoroutineStub,
    it: DataProviderInput,
    measurementConsumerSigningKey: SigningKeyHandle,
    measurementEncryptionPublicKey: ByteString
  ): Measurement.DataProviderEntry {
    return dataProviderEntry {
      val requisitionSpec = requisitionSpec {
        eventGroups +=
          it.eventGroupInputs.map {
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
        nonce = Random.Default.nextLong()
      }

      key = it.name
      val dataProvider =
        runBlocking(Dispatchers.IO) {
          dataProviderStub.getDataProvider(getDataProviderRequest { name = it.name })
        }
      value = dataProviderEntryValue {
        dataProviderCertificate = dataProvider.certificate
        dataProviderPublicKey = dataProvider.publicKey
        dataProviderCertificate = "${it.name}/certificates/1"
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

  override fun run() {
    val measurementConsumerStub = MeasurementConsumersCoroutineStub(parent.channel)
    val measurementStub = MeasurementsCoroutineStub(parent.channel)
    val dataProviderStub = DataProvidersCoroutineStub(parent.channel)

    val measurementConsumer =
      runBlocking(Dispatchers.IO) {
        measurementConsumerStub.getMeasurementConsumer(
          getMeasurementConsumerRequest { name = measurementConsumer }
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
        reachAndFrequency = reachAndFrequency {
          reachPrivacyParams = differentialPrivacyParams {
            epsilon = 1.0
            delta = 1.0
          }
          frequencyPrivacyParams = differentialPrivacyParams {
            epsilon = 1.0
            delta = 1.0
          }
          vidSamplingInterval = vidSamplingInterval {
            start = 0.0f
            width = 1.0f
          }
        }
      }
      this.measurementSpec =
        signMeasurementSpec(unsignedMeasurementSpec, measurementConsumerSigningKey)
      measurementReferenceId = this@CreateCommand.measurementReferenceId
    }

    val response =
      runBlocking(Dispatchers.IO) {
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
    names = ["--measurement-consumer"],
    description = ["API resource name of the Measurement Consumer"],
    required = true,
  )
  private lateinit var measurementConsumerName: String

  override fun run() {
    val measurementStub = MeasurementsCoroutineStub(parent.channel)
    val response =
      runBlocking(Dispatchers.IO) {
        measurementStub.listMeasurements(
          listMeasurementsRequest { parent = measurementConsumerName }
        )
      }
    print(response.toJson())
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

  override fun run() {
    val measurementStub = MeasurementsCoroutineStub(parent.channel)
    val measurement =
      runBlocking(Dispatchers.IO) {
        measurementStub.getMeasurement(getMeasurementRequest { name = measurementName })
      }
    // print measurement
    print(measurement.toJson())
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
