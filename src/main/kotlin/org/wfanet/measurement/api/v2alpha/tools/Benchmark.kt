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

/**
 * Benchmark submits a number of (nearly) identical requests to the kingdom, collects the results,
 * and then generates a CSV output file with the measurements that were collected.
 *
 * <p>Following is an example of the use of the benchmark command to measure reach and frequency
 * <pre> bazel-bin/src/main/kotlin/org/wfanet/measurement/api/v2alpha/tools/SimpleReport \
 * ```
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
 * ```
 * </pre>
 */
package org.wfanet.measurement.api.v2alpha.tools

import com.google.protobuf.Any as ProtoAny
import com.google.type.Interval
import com.google.type.interval
import io.grpc.ManagedChannel
import java.io.File
import java.security.SecureRandom
import java.time.Clock
import java.time.Duration as JavaDuration
import java.time.Instant
import java.util.Collections
import java.util.UUID
import kotlin.properties.Delegates
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.jetbrains.annotations.VisibleForTesting
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKt.DataProviderEntryKt.value as dataProviderEntryValue
import org.wfanet.measurement.api.v2alpha.MeasurementKt.dataProviderEntry
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.EventGroupEntryKt.value as eventGroupEntryValue
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.Hashing
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.measurementconsumer.decryptResult
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
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

class BaseFlags {

  @CommandLine.Option(
    names = ["--encryption-private-key-file"],
    description = ["MeasurementConsumer's EncryptionPrivateKey"],
    required = true,
  )
  lateinit var encryptionPrivateKeyFile: File

  val privateKeyHandle: PrivateKeyHandle by lazy { loadPrivateKey(encryptionPrivateKeyFile) }

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
    required = true,
  )
  var outputFile by Delegates.notNull<String>()
    private set

  @set:CommandLine.Option(
    names = ["--timeout"],
    description = ["Maximum time to wait in seconds for a request to complete"],
    defaultValue = "600",
  )
  var timeout by Delegates.notNull<Long>()
    private set
}

private fun getPopulationDataProviderEntry(
  dataProviderStub: DataProvidersCoroutineStub,
  dataProviderInput:
    CreateMeasurementFlags.MeasurementParams.PopulationMeasurementParams.PopulationDataProviderInput,
  measurementParams: CreateMeasurementFlags.MeasurementParams.PopulationMeasurementParams,
  measurementConsumerSigningKey: SigningKeyHandle,
  packedMeasurementEncryptionPublicKey: ProtoAny,
  secureRandom: SecureRandom,
  apiAuthenticationKey: String,
): Measurement.DataProviderEntry {
  return dataProviderEntry {
    val requisitionSpec = requisitionSpec {
      population =
        RequisitionSpecKt.population {
          interval = interval {
            startTime = measurementParams.populationInputs.startTime.toProtoTime()
            endTime = measurementParams.populationInputs.endTime.toProtoTime()
          }
          if (measurementParams.populationInputs.filter.isNotEmpty())
            filter = eventFilter { expression = measurementParams.populationInputs.filter }
        }
      this.measurementPublicKey = packedMeasurementEncryptionPublicKey
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
  dataProviderStub: DataProvidersCoroutineStub,
  dataProviderInput:
    CreateMeasurementFlags.MeasurementParams.EventMeasurementParams.EventDataProviderInput,
  measurementConsumerSigningKey: SigningKeyHandle,
  packedMeasurementEncryptionPublicKey: ProtoAny,
  secureRandom: SecureRandom,
  apiAuthenticationKey: String,
  cumulativeCollectionInterval: Interval?,
  eventFilter: String,
): Measurement.DataProviderEntry {
  return dataProviderEntry {
    val requisitionSpec = requisitionSpec {
      events =
        RequisitionSpecKt.events {
          eventGroups +=
            dataProviderInput.eventGroupInputs.map {
              eventGroupEntry {
                key = it.name
                value = eventGroupEntryValue {
                  if (cumulativeCollectionInterval != null) {
                    check(cumulativeCollectionInterval.startTime.toInstant() == it.eventStartTime)
                    check(cumulativeCollectionInterval.endTime.toInstant() <= it.eventEndTime)
                    this.collectionInterval = cumulativeCollectionInterval
                  } else {
                    this.collectionInterval = interval {
                      startTime = it.eventStartTime.toProtoTime()
                      endTime = it.eventEndTime.toProtoTime()
                    }
                  }
                  filter = eventFilter { expression = eventFilter }
                }
              }
            }
        }
      this.measurementPublicKey = packedMeasurementEncryptionPublicKey
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

private fun getMeasurementResult(
  resultOutput: Measurement.ResultOutput,
  privateKeyHandle: PrivateKeyHandle,
): Measurement.Result {
  val signedResult = decryptResult(resultOutput.encryptedResult, privateKeyHandle)
  return signedResult.unpack()
}

class Benchmark(
  val flags: BaseFlags,
  private val createMeasurementFlags: CreateMeasurementFlags,
  val channel: ManagedChannel,
  val apiAuthenticationKey: String,
  val clock: Clock,
) {

  private val secureRandom = SecureRandom.getInstance("SHA1PRNG")

  private val eventMeasurementParams =
    createMeasurementFlags.measurementParams.eventMeasurementParams

  /**
   * The following data structure is used to track the status of each request and to store the
   * results that were received.
   */
  class MeasurementTask(val replicaId: Int, val requestTime: Instant) {
    // Time at which we received an acknowledgment of the request.
    lateinit var ackTime: Instant

    // Time at which the response to the request was received.
    lateinit var responseTime: Instant

    // Time at which requisitions are fulfilled. It is the best estimation when detect the
    // computation state changed into COMPUTING
    var requisitionFulfilledTime: Instant? = null

    // Elapsed time from when request was made to response was received.
    var elapsedTimeMillis: Long = 0

    // Name of the measurement, as generated by the benchmarker
    lateinit var referenceId: String

    // Name of the measurement, as generated by the Kingdom.
    lateinit var measurementName: String

    // Status of the response -- did an error occur?
    lateinit var status: String

    // Error message, if any
    var errorMessage = ""

    // The measurement result that was obtained.
    lateinit var result: Measurement.Result
  }

  /** List of tasks that have been submitted to the Kingdom. */
  private val taskList: MutableList<MeasurementTask> = Collections.synchronizedList(mutableListOf())

  /** List of tasks for which responses have been received or which have timed out. */
  private val completedTasks: MutableList<MeasurementTask> = mutableListOf()

  /** Creates list of requests and sends them to the Kingdom for PDPs. */
  private fun generatePopulationRequests(
    measurementConsumerStub: MeasurementConsumersCoroutineStub,
    measurementStub: MeasurementsCoroutineStub,
    dataProviderStub: DataProvidersCoroutineStub,
  ) {
    val measurementConsumer =
      runBlocking(Dispatchers.IO) {
        measurementConsumerStub
          .withAuthenticationKey(apiAuthenticationKey)
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
    val packedMeasurementEncryptionPublicKey: ProtoAny = measurementConsumer.publicKey.message
    val charPool = ('a'..'z').toList()
    val referenceIdBase =
      (1..10)
        .map { _ -> kotlin.random.Random.nextInt(0, charPool.size) }
        .map(charPool::get)
        .joinToString("")

    for (replica in 1..flags.repetitionCount) {
      val referenceId = "$referenceIdBase-$replica-${UUID.randomUUID()}"
      val measurement = measurement {
        this.measurementConsumerCertificate = measurementConsumer.certificate
        dataProviders +=
          getPopulationDataProviderEntry(
            dataProviderStub,
            createMeasurementFlags.measurementParams.populationMeasurementParams
              .populationDataProviderInput,
            createMeasurementFlags.measurementParams.populationMeasurementParams,
            measurementConsumerSigningKey,
            packedMeasurementEncryptionPublicKey,
            secureRandom,
            apiAuthenticationKey,
          )

        val unsignedMeasurementSpec = measurementSpec {
          measurementPublicKey = packedMeasurementEncryptionPublicKey
          nonceHashes += this@measurement.dataProviders.map { it.value.nonceHash }
          population = createMeasurementFlags.getPopulation()
          if (createMeasurementFlags.modelLine.isNotEmpty())
            modelLine = createMeasurementFlags.modelLine
        }

        this.measurementSpec =
          signMeasurementSpec(unsignedMeasurementSpec, measurementConsumerSigningKey)
        measurementReferenceId = referenceId
      }
      val task = MeasurementTask(replica, Instant.now(clock))
      task.referenceId = referenceId

      val response =
        runBlocking(Dispatchers.IO) {
          measurementStub
            .withAuthenticationKey(apiAuthenticationKey)
            .createMeasurement(
              createMeasurementRequest {
                parent = measurementConsumer.name
                this.measurement = measurement
              }
            )
        }
      println("Measurement Name: ${response.name}")

      task.ackTime = Instant.now(clock)
      task.measurementName = response.name
      taskList.add(task)
    }
  }

  /** Creates list of requests and sends them to the Kingdom for EDPs, in parallel. */
  suspend fun generateEventRequests(
    measurementConsumerStub: MeasurementConsumersCoroutineStub,
    measurementStub: MeasurementsCoroutineStub,
    dataProviderStub: DataProvidersCoroutineStub,
  ) = coroutineScope {
    val measurementConsumer =
      withContext(Dispatchers.IO) {
        measurementConsumerStub
          .withAuthenticationKey(apiAuthenticationKey)
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
    val packedMeasurementEncryptionPublicKey: ProtoAny = measurementConsumer.publicKey.message
    val charPool = ('a'..'z').toList()
    val referenceIdBase =
      (1..10)
        .map { _ -> kotlin.random.Random.nextInt(0, charPool.size) }
        .map(charPool::get)
        .joinToString("")

    val deferredTasks = mutableListOf<Deferred<Unit>>()

    for (replica in 1..flags.repetitionCount) {
      val overrideCollectionIntervalEndTimes =
        if (eventMeasurementParams.cumulative) {
          // getEventDataProviderEntry checks to make sure all entries are identical for cumulative
          val firstEntryStartTime =
            eventMeasurementParams.eventDataProviderInputs
              .first()
              .eventGroupInputs
              .first()
              .eventStartTime
          val firstEntryEndTime =
            eventMeasurementParams.eventDataProviderInputs
              .first()
              .eventGroupInputs
              .first()
              .eventEndTime
          val step = JavaDuration.ofDays(7)
          var current = firstEntryStartTime
          val endTimes = mutableListOf<Instant>()
          while (current.plus(step) < firstEntryEndTime) {
            current = current.plus(step)
            endTimes.add(current)
          }
          endTimes.add(firstEntryEndTime)
          endTimes
        } else {
          // Null means to not override the collection interval end time
          listOf(null)
        }

      for (overrideEndTime in overrideCollectionIntervalEndTimes) {
        val cumulativeCollectionInterval =
          if (eventMeasurementParams.cumulative) {
            interval {
              this.startTime =
                eventMeasurementParams.eventDataProviderInputs
                  .first()
                  .eventGroupInputs
                  .first()
                  .eventStartTime
                  .toProtoTime()
              this.endTime = overrideEndTime!!.toProtoTime()
            }
          } else {
            null
          }

        val eventDataProviderInputsList =
          if (
            eventMeasurementParams.directMeasurementParams.createDirect &&
              eventMeasurementParams.eventDataProviderInputs.size > 1
          ) {
            listOf(eventMeasurementParams.eventDataProviderInputs) +
              eventMeasurementParams.eventDataProviderInputs.map { listOf(it) }
          } else {
            listOf(eventMeasurementParams.eventDataProviderInputs)
          }

        for (eventDataProviderInputs in eventDataProviderInputsList) {
          val eventFilters: List<String> =
            eventDataProviderInputs.first().eventFilters.map { it.eventFilter }
          for (eventFilter in eventFilters) {
            val referenceId = "$referenceIdBase-$replica-${UUID.randomUUID()}"

            val vidSamplingStartForMeasurement =
              if (
                eventDataProviderInputs.size == 1 &&
                  eventMeasurementParams.directMeasurementParams.createDirect
              ) {
                kotlin.random.Random.nextInt(0, flags.vidBucketCount).toFloat() *
                  eventMeasurementParams.directMeasurementParams.directVidSamplingWidth
              } else {
                eventMeasurementParams.vidSamplingStart +
                  kotlin.random.Random.nextInt(0, flags.vidBucketCount).toFloat() *
                    eventMeasurementParams.vidSamplingWidth
              }
            val measurement = measurement {
              this.measurementConsumerCertificate = measurementConsumer.certificate
              dataProviders +=
                eventDataProviderInputs.map {
                  getEventDataProviderEntry(
                    dataProviderStub,
                    it,
                    measurementConsumerSigningKey,
                    packedMeasurementEncryptionPublicKey,
                    secureRandom,
                    apiAuthenticationKey,
                    cumulativeCollectionInterval,
                    eventFilter,
                  )
                }
              val unsignedMeasurementSpec = measurementSpec {
                measurementPublicKey = packedMeasurementEncryptionPublicKey
                nonceHashes += this@measurement.dataProviders.map { it.value.nonceHash }
                vidSamplingInterval = vidSamplingInterval {
                  if (
                    eventDataProviderInputs.size == 1 &&
                      eventMeasurementParams.directMeasurementParams.createDirect
                  ) {
                    start = 0.0f
                    width = eventMeasurementParams.directMeasurementParams.directVidSamplingWidth
                  } else {
                    start = vidSamplingStartForMeasurement
                    width = eventMeasurementParams.vidSamplingWidth
                  }
                }
                if (eventMeasurementParams.eventMeasurementTypeParams.reach.selected) {
                  reach = createMeasurementFlags.getReach()
                } else if (
                  eventMeasurementParams.eventMeasurementTypeParams.reachAndFrequency.selected
                ) {
                  reachAndFrequency = createMeasurementFlags.getReachAndFrequency()
                } else if (eventMeasurementParams.eventMeasurementTypeParams.impression.selected) {
                  impression = createMeasurementFlags.getImpression()
                } else if (eventMeasurementParams.eventMeasurementTypeParams.duration.selected) {
                  duration = createMeasurementFlags.getDuration()
                }
                if (createMeasurementFlags.modelLine.isNotEmpty())
                  modelLine = createMeasurementFlags.modelLine
                if (eventMeasurementParams.report.isNotEmpty())
                  reportingMetadata =
                    MeasurementSpecKt.reportingMetadata { report = eventMeasurementParams.report }
              }

              this.measurementSpec =
                signMeasurementSpec(unsignedMeasurementSpec, measurementConsumerSigningKey)
              measurementReferenceId = referenceId
            }

            val task = MeasurementTask(replica, Instant.now(clock))
            task.referenceId = referenceId

            // Launch each createMeasurement in parallel
            val deferred: Deferred<Unit> =
              async(Dispatchers.IO) {
                val response =
                  measurementStub
                    .withAuthenticationKey(apiAuthenticationKey)
                    .createMeasurement(
                      createMeasurementRequest {
                        parent = measurementConsumer.name
                        this.measurement = measurement
                      }
                    )
                println("Measurement Name: ${response.name}")
                task.ackTime = Instant.now(clock)
                task.measurementName = response.name
                taskList.add(task)
              }
            deferredTasks.add(deferred)
          }
        }
      }
    }

    // Await all createMeasurement calls in parallel
    try {
      deferredTasks.awaitAll()
    } catch (e: Exception) {
      throw e
    }
  }

  /** Collects responses from tasks that have completed. */
  private fun collectCompletedTasks(
    measurementStub: MeasurementsCoroutineStub,
    firstInstant: Instant,
  ) {
    var iTask = 0
    while (iTask < taskList.size) {
      val task = taskList[iTask]

      print("${(Instant.now(clock).toEpochMilli() - firstInstant.toEpochMilli()) / 1000.0} ")
      print("Trying to retrieve ${task.referenceId} ${task.measurementName}...")
      val measurement =
        runBlocking(Dispatchers.IO) {
          measurementStub
            .withAuthenticationKey(apiAuthenticationKey)
            .getMeasurement(getMeasurementRequest { name = task.measurementName })
        }

      val timeoutOccurred = task.requestTime.plusSeconds(flags.timeout).isBefore(Instant.now(clock))
      println("${measurement.state}")
      if (
        measurement.state == Measurement.State.COMPUTING && task.requisitionFulfilledTime == null
      ) {
        task.requisitionFulfilledTime = Instant.now(clock)
      }
      if (
        (measurement.state == Measurement.State.SUCCEEDED) ||
          (measurement.state == Measurement.State.FAILED) ||
          timeoutOccurred
      ) {
        when (measurement.state) {
          Measurement.State.SUCCEEDED -> {
            val result = getMeasurementResult(measurement.resultsList[0], flags.privateKeyHandle)
            task.result = result
            println("Got result for task $iTask\n$measurement\n-----\n$result")
            task.status = "success"
          }
          Measurement.State.FAILED -> {
            task.status = "failed"
            task.errorMessage = measurement.failure.message
          }
          else -> {
            task.status = "timeout"
          }
        }

        task.responseTime = Instant.now(clock)
        task.elapsedTimeMillis = task.responseTime.toEpochMilli() - firstInstant.toEpochMilli()
        taskList.removeAt(iTask)
        completedTasks.add(task)
      } else {
        iTask += 1
      }
    }
  }

  /** Writes a CSV file containing the benchmarking results. */
  private fun generateOutput(firstInstant: Instant) {
    File(flags.outputFile).printWriter().use { out ->
      out.print("replica,startTime,ackTime,computeTime,endTime,status,msg,")
      if (createMeasurementFlags.measurementParams.populationMeasurementParams.selected) {
        out.println("population")
      } else if (eventMeasurementParams.eventMeasurementTypeParams.reach.selected) {
        out.println("reach")
      } else if (eventMeasurementParams.eventMeasurementTypeParams.reachAndFrequency.selected) {
        out.println("reach,freq1,freq2,freq3,freq4,freq5")
      } else if (eventMeasurementParams.eventMeasurementTypeParams.impression.selected) {
        out.println("impressions")
      } else if (eventMeasurementParams.eventMeasurementTypeParams.duration.selected) {
        out.println("duration")
      }
      for (task in completedTasks) {
        out.print("${task.replicaId}")
        out.print(",${(task.requestTime.toEpochMilli() - firstInstant.toEpochMilli()) / 1000.0}")
        out.print(",${(task.ackTime.toEpochMilli() - firstInstant.toEpochMilli()) / 1000.0}")
        if (task.requisitionFulfilledTime != null) {
          out.print(
            ",${(task.requisitionFulfilledTime!!.toEpochMilli() - firstInstant.toEpochMilli()) / 1000.0}"
          )
        } else {
          out.print(",0.0")
        }
        out.print(",${task.elapsedTimeMillis / 1000.0},")
        out.print("${task.status},${task.errorMessage},")
        if (createMeasurementFlags.measurementParams.populationMeasurementParams.selected) {
          out.println("${task.result.population.value}")
        } else if (eventMeasurementParams.eventMeasurementTypeParams.reach.selected) {
          if (task.status == "success" && task.result.hasReach()) {
            out.print(task.result.reach.value)
          } else {
            out.print(-1)
          }
        } else if (eventMeasurementParams.eventMeasurementTypeParams.reachAndFrequency.selected) {
          var reach = 0L
          if (task.status == "success" && task.result.hasReach()) {
            reach = task.result.reach.value
          }
          out.print(reach)
          val frequencies = arrayOf(0.0, 0.0, 0.0, 0.0, 0.0)
          if (task.status == "success" && task.result.hasFrequency()) {
            task.result.frequency.relativeFrequencyDistributionMap.forEach {
              if (it.key <= frequencies.size) {
                frequencies[(it.key - 1).toInt()] = it.value * reach.toDouble()
              }
            }
          }
          for (i in 1..frequencies.size) {
            out.print(",${frequencies[i - 1]}")
          }
          out.println()
        } else if (eventMeasurementParams.eventMeasurementTypeParams.impression.selected) {
          out.println("${task.result.impression.value}")
        } else if (eventMeasurementParams.eventMeasurementTypeParams.duration.selected) {
          out.println("${task.result.watchDuration.value.seconds}")
        }
      }
    }
  }

  fun generateBenchmarkReport() {
    val measurementConsumerStub = MeasurementConsumersCoroutineStub(channel)
    val measurementStub = MeasurementsCoroutineStub(channel)
    val dataProviderStub = DataProvidersCoroutineStub(channel)

    val programStartTime = Instant.now(clock)

    var allRequestsSent = false
    runBlocking {
      launch {
        if (createMeasurementFlags.measurementParams.populationMeasurementParams.selected) {
          generatePopulationRequests(measurementConsumerStub, measurementStub, dataProviderStub)
        } else {
          generateEventRequests(measurementConsumerStub, measurementStub, dataProviderStub)
        }
        allRequestsSent = true
      }

      launch {
        while (taskList.size > 0 || !allRequestsSent) {
          collectCompletedTasks(measurementStub, programStartTime)
          delay(1000L)
        }
        generateOutput(programStartTime)
      }
    }
  }
}

@CommandLine.Command(
  name = "BenchmarkReport",
  description = ["Benchmark report from Kingdom"],
  sortOptions = false,
)
class BenchmarkReport private constructor(val clock: Clock = Clock.systemUTC()) : Runnable {
  @CommandLine.Mixin private lateinit var tlsFlags: TlsFlags
  @CommandLine.Mixin private lateinit var apiFlags: ApiFlags
  @CommandLine.Mixin private lateinit var baseFlags: BaseFlags
  @CommandLine.Mixin private lateinit var createMeasurementFlags: CreateMeasurementFlags

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
        trustedCertCollectionFile = tlsFlags.certCollectionFile,
      )
    buildMutualTlsChannel(apiFlags.apiTarget, clientCerts, apiFlags.apiCertHost)
      .withShutdownTimeout(JavaDuration.ofSeconds(1))
  }

  override fun run() {
    val benchmark =
      Benchmark(baseFlags, createMeasurementFlags, channel, apiAuthenticationKey, clock)
    benchmark.generateBenchmarkReport()
  }

  companion object {
    /**
     * Create a benchmarking report of the public Kingdom API.
     *
     * Use the `help` command to see usage details.
     */
    fun main(args: Array<String>) = commandLineMain(BenchmarkReport(), args)

    @VisibleForTesting
    internal fun main(args: Array<String>, clock: Clock) =
      commandLineMain(BenchmarkReport(clock), args)
  }
}
