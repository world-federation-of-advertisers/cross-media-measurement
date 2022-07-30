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
import java.lang.Thread
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
  dataProviderInput: DataProviderInput,
  measurementConsumerSigningKey: SigningKeyHandle,
  measurementEncryptionPublicKey: ByteString,
  secureRandom: SecureRandom,
  apiAuthenticationKey: String
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
              if (it.eventFilter.isNotEmpty()) filter = eventFilter { expression = it.eventFilter }
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
          .withAuthenticationKey(apiAuthenticationKey)
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

private fun convertToTimestamp(instant: Instant): Timestamp {
  return timestamp {
    seconds = instant.epochSecond
    nanos = instant.nano
  }
}

class MeasurementTypeParams {

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

private fun getReachAndFrequency(measurementTypeParams: MeasurementTypeParams): ReachAndFrequency {
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

private fun getImpression(measurementTypeParams: MeasurementTypeParams): Impression {
  return impression {
    privacyParams = differentialPrivacyParams {
      epsilon = measurementTypeParams.impression.privacyEpsilon
      delta = measurementTypeParams.impression.privacyDelta
    }
    maximumFrequencyPerUser = measurementTypeParams.impression.maximumFrequencyPerUser
  }
}

private fun getDuration(measurementTypeParams: MeasurementTypeParams): Duration {
  return duration {
    privacyParams = differentialPrivacyParams {
      epsilon = measurementTypeParams.duration.privacyEpsilon
      delta = measurementTypeParams.duration.privacyDelta
    }
    maximumWatchDurationPerUser = measurementTypeParams.duration.maximumWatchDurationPerUser
  }
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

  @CommandLine.ArgGroup(exclusive = false, multiplicity = "1..*", heading = "Add DataProviders\n")
  private lateinit var dataProviderInputs: List<DataProviderInput>

  private val secureRandom = SecureRandom.getInstance("SHA1PRNG")

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
            measurementEncryptionPublicKey,
            secureRandom,
            parent.apiAuthenticationKey
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
          reachAndFrequency = getReachAndFrequency(measurementTypeParams)
        } else if (measurementTypeParams.impression.selected) {
          impression = getImpression(measurementTypeParams)
        } else duration = getDuration(measurementTypeParams)
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
    print("Measurement Name: ${response.name}")
  }
}

private fun getMeasurementResult(
  resultPair: Measurement.ResultPair,
  certificateStub: CertificatesCoroutineStub,
  apiAuthenticationKey: String,
  privateKeyHandle: PrivateKeyHandle
): Measurement.Result {
  val certificate = runBlocking {
    certificateStub
      .withAuthenticationKey(apiAuthenticationKey)
      .getCertificate(getCertificateRequest { name = resultPair.certificate })
  }

  val signedResult = decryptResult(resultPair.encryptedResult, privateKeyHandle)

  val result = Measurement.Result.parseFrom(signedResult.data)

  if (!verifyResult(signedResult.signature, result, readCertificate(certificate.x509Der))) {
    error("Signature of the result is invalid.")
  }
  return result
}

@CommandLine.Command(
  name = "benchmark",
  description = ["Submits multiple identical measurements and generates list of responses"]
)
class BenchmarkCommand : Runnable {
	/** Benchmark submits a number of (nearly) identical requests to the
	 *  kingdom, collects the results, and then generates a CSV output
	 *  file with the measurements that were collected.
	 *
	 *  <p>Following is an example of the use of the benchmark command to
	 *  measure reach and frequency
	 *
	 *  <pre>
   *  bazel-bin/src/main/kotlin/org/wfanet/measurement/api/v2alpha/tools/SimpleReport \
   *    --tls-cert-file src/main/k8s/testing/secretfiles/mc_tls.pem \
   *    --tls-key-file src/main/k8s/testing/secretfiles/mc_tls.key \
   *    --cert-collection-file src/main/k8s/testing/secretfiles/kingdom_root.pem \
   *    --kingdom-public-api-target \
   *    localhost:8443 \
   *    --api-key ${HALO_MC_APIKEY} \
   *    benchmark \
   *    --measurement-consumer ${HALO_MC} \
   *    --private-key-der-file=src/main/k8s/testing/secretfiles/mc_cs_private.der \
   *    --encryption-private-key-file=src/main/k8s/testing/secretfiles/mc_enc_private.tink \
   *    --output-file=benchmark-results.csv \
   *    --timeout=600 \
   *    --reach-and-frequency \
   *    --reach-privacy-epsilon=0.0072 \
   *    --reach-privacy-delta=0.0000000001 \
   *    --frequency-privacy-epsilon=0.2015 \
   *    --frequency-privacy-delta=0.0000000001 \
   *    --vid-sampling-start=0.0 \
   *    --vid-sampling-width=0.016667 \
   *    --vid-bucket-count=1 \
   *    --repetition-count=5 \
   *    --data-provider "${HALO_DATA_PROVIDER}" \
   *    --event-group "${HALO_EVENT_GROUP}" \
   *    --event-start-time=2022-05-22T01:00:00.000Z \
   *    --event-end-time=2022-05-24T05:00:00.000Z
	 * </pre>
	 */

  @CommandLine.ParentCommand private lateinit var parent: SimpleReport

  @CommandLine.Option(
    names = ["--measurement-consumer"],
    description = ["API resource name of the MeasurementConsumer"],
    required = true
  )
  private lateinit var measurementConsumer: String

  @CommandLine.Option(
    names = ["--encryption-private-key-file"],
    description = ["MeasurementConsumer's EncryptionPrivateKey"],
    required = true
  )
  private lateinit var encryptionPrivateKeyFile: File

  private val privateKeyHandle: PrivateKeyHandle by lazy { loadPrivateKey(encryptionPrivateKeyFile) }

  @CommandLine.Option(
    names = ["--private-key-der-file"],
    description = ["Private key for MeasurementConsumer"],
    required = true
  )
  private lateinit var privateKeyDerFile: File

  @set:CommandLine.Option(
    names = ["--vid-sampling-start"],
    description = ["Start point of vid sampling interval for first VID bucket"],
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

  @set:CommandLine.Option(
    names = ["--vid-bucket-count"],
    description = ["Number of VID buckets to sample from"],
    defaultValue = "1",
  )
  var vidBucketCount by Delegates.notNull<Int>()
    private set

  @set:CommandLine.Option(
    names = ["--repetition-count"],
    description = ["Number of times the query should be repeated"],
    defaultValue = "1",
  )
  var repetitionCount by Delegates.notNull<Int>()
    private set

  @set:CommandLine.Option(
    names = ["--output-file"],
    description = ["Name of file where final results should be written"],
		required=true
  )
  var outputFile by Delegates.notNull<String>()
    private set

  @set:CommandLine.Option(
    names = ["--timeout"],
    description = ["Maximum time to wait in seconds for a request to complete"],
    defaultValue = "600",
  )
  var timeout by Delegates.notNull<Int>()
    private set

  @CommandLine.ArgGroup(
    exclusive = true,
    multiplicity = "1",
    heading = "Specify one of the measurement types with its params\n"
  )
  lateinit var measurementTypeParams: MeasurementTypeParams

  @CommandLine.ArgGroup(exclusive = false, multiplicity = "1..*", heading = "Add DataProviders\n")
  private lateinit var dataProviderInputs: List<DataProviderInput>

  private val secureRandom = SecureRandom.getInstance("SHA1PRNG")

	/** The following data structure is used to track the status
	 *  of each request and to store the results that were received.
	 */
  class MeasurementTask (val replicaId: Int, val requestTime: Instant){
		// Time at which we received an acknowledgment of the request.
		lateinit var ackTime: Instant

		// Time at which the response to the request was received.
		lateinit var responseTime: Instant

		// Elapsed time from when request was made to response was received.
		var elapsedTimeMillis: Long = 0

		// Name of the measurement, as generated by the benchmarker
		lateinit var referenceId: String

		// Name of the measurement, as generated by the Kingdom.
		lateinit var measurementName: String

		// Status of the response -- did an error occur?
		lateinit var status: String

		// Error message, if any
		lateinit var errorMessage: String

		// The measurement result that was obtained.
		lateinit var result: Measurement.Result
	}

  override fun run() {
    val measurementConsumerStub = MeasurementConsumersCoroutineStub(parent.channel)
    val measurementStub = MeasurementsCoroutineStub(parent.channel)
    val certificateStub = CertificatesCoroutineStub(parent.channel)
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
		val taskList: MutableList<MeasurementTask> = mutableListOf()

		val charPool = ('a'..'z').toList()
		val referenceIdBase = (1..10)
			.map { _ -> kotlin.random.Random.nextInt(0, charPool.size) }
			.map(charPool::get)
			.joinToString("");
		val firstInstant = Instant.now()

		for (replica in 1..repetitionCount) {
			val referenceId = "${referenceIdBase}-${replica}"
			val vidSamplingStartForMeasurement = vidSamplingStart + kotlin.random.Random.nextInt(0, vidBucketCount).toFloat() * vidSamplingWidth

			val measurement = measurement {
				this.measurementConsumerCertificate = measurementConsumer.certificate
				dataProviders +=
					dataProviderInputs.map {
						getDataProviderEntry(
							dataProviderStub,
							it,
							measurementConsumerSigningKey,
							measurementEncryptionPublicKey,
							secureRandom,
							parent.apiAuthenticationKey
						)
					}
				val unsignedMeasurementSpec = measurementSpec {
					measurementPublicKey = measurementEncryptionPublicKey
					nonceHashes += this@measurement.dataProviders.map { it.value.nonceHash }
					vidSamplingInterval = vidSamplingInterval {
						start = vidSamplingStartForMeasurement
						width = vidSamplingWidth
					}
					if (measurementTypeParams.reachAndFrequency.selected) {
						reachAndFrequency = getReachAndFrequency(measurementTypeParams)
					} else if (measurementTypeParams.impression.selected) {
						impression = getImpression(measurementTypeParams)
					} else duration = getDuration(measurementTypeParams)
				}

				this.measurementSpec =
					signMeasurementSpec(unsignedMeasurementSpec, measurementConsumerSigningKey)
				measurementReferenceId = referenceId
			}

			val task = MeasurementTask (replica, Instant.now())
			task.referenceId = referenceId

			val response =
				runBlocking(Dispatchers.IO) {
					measurementStub
						.withAuthenticationKey(parent.apiAuthenticationKey)
						.createMeasurement(createMeasurementRequest { this.measurement = measurement })
				}
			println("Measurement Name: ${response.name}")

			task.ackTime = Instant.now()
			task.measurementName = response.name
			taskList.add(task)
		}

		val completedTasks: MutableList<MeasurementTask> = mutableListOf()
		// TODO: Handle timeouts
		// TODO: Save an error message if a task failed
		// TODO: Set up a thread to handle querying for finished tasks
		while (taskList.size > 0) {
			val task = taskList.get(0)

			print("${(Instant.now().toEpochMilli() - firstInstant.toEpochMilli())/1000.0} ")
			println("Trying to retrieve ${task.referenceId} ${task.measurementName}...")
			val measurement =
				runBlocking(Dispatchers.IO) {
					measurementStub
						.withAuthenticationKey(parent.apiAuthenticationKey)
						.getMeasurement(getMeasurementRequest { name = task.measurementName })
				}

			if ((measurement.state == Measurement.State.SUCCEEDED) ||
					 (measurement.state == Measurement.State.FAILED)) {

				if (measurement.state == Measurement.State.SUCCEEDED) {
					val result = getMeasurementResult(measurement.resultsList.get(0),
																						certificateStub,
																						parent.apiAuthenticationKey,
																						privateKeyHandle)
					task.result = result
				}

				task.responseTime = Instant.now()
				task.elapsedTimeMillis = task.responseTime.toEpochMilli() - firstInstant.toEpochMilli()
				taskList.removeAt(0)
				completedTasks.add(task)
			} else {
				Thread.sleep(1000L)
			}
		}

		// TODO: Include errors for failed tasks
		// TODO: Formatting of time
		File(outputFile).printWriter().use { out ->
			out.print("replica,startTime,ackTime,endTime,")
      if (measurementTypeParams.reachAndFrequency.selected) {
				out.println("reach,freq1,freq2,freq3,freq4,freq5")
      } else if (measurementTypeParams.impression.selected) {
				out.println("impressions")
      } else {
				out.println("duration")
			}
			for (task in completedTasks) {
				out.print("${task.replicaId}")
				out.print(",${(task.requestTime.toEpochMilli() - firstInstant.toEpochMilli())/1000.0}")
				out.print(",${(task.ackTime.toEpochMilli() - firstInstant.toEpochMilli())/1000.0}")
					out.print(",${task.elapsedTimeMillis/1000.0},")
					if (measurementTypeParams.reachAndFrequency.selected) {
						var reach = 0L
						if (task.result.hasReach()) {
							reach = task.result.reach.value
						}
						out.print(reach)
						var frequencies = arrayOf(0.0, 0.0, 0.0, 0.0, 0.0)
						if (task.result.hasFrequency()) {
							task.result.frequency.relativeFrequencyDistributionMap.forEach {
								if (it.key <= frequencies.size) {
									frequencies[(it.key-1).toInt()] = it.value * reach.toDouble()
								}
							}
						}
						for (i in 1..frequencies.size) {
							out.print(",${frequencies[i-1]}")
						}
						out.println()
					} else if (measurementTypeParams.impression.selected) {
						out.println("${task.result.impression.value}")
					} else {
						out.println("${task.result.watchDuration.value.seconds}")
					}
				}
		}
  }
}

private class PageParams {
  @CommandLine.Option(
    names = ["--page-size"],
    description = ["The maximum number of items to return. The maximum value is 1000"],
    required = false,
  )
  var pageSize: Int = 1000
    private set

  @CommandLine.Option(
    names = ["--page-token"],
    description = ["Page token from a previous `list` call to retrieve the next page"],
    defaultValue = "",
    required = false,
  )
  lateinit var pageToken: String
    private set
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

  @CommandLine.Mixin private lateinit var pageParams: PageParams

  override fun run() {
    val measurementStub = MeasurementsCoroutineStub(parent.channel)

    val request = listMeasurementsRequest {
      parent = measurementConsumerName
      pageSize = pageParams.pageSize
      pageToken = pageParams.pageToken
    }

    val response =
      runBlocking(Dispatchers.IO) {
        measurementStub.withAuthenticationKey(parent.apiAuthenticationKey).listMeasurements(request)
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
        val result =
          getMeasurementResult(it, certificateStub, parent.apiAuthenticationKey, privateKeyHandle)
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
      BenchmarkCommand::class,
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
