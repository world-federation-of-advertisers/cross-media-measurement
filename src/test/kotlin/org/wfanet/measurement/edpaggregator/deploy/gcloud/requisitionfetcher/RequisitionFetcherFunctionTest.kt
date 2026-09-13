/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.deploy.gcloud.requisitionfetcher

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.kotlin.toByteString
import com.google.protobuf.timestamp
import com.google.type.interval
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.ServerInterceptors
import io.netty.handler.ssl.ClientAuth
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.nio.file.Path
import java.nio.file.Paths
import java.security.MessageDigest
import java.time.LocalDate
import java.util.Base64
import java.util.logging.Logger
import kotlin.random.Random
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.testing.loadSigningKey
import org.wfanet.measurement.common.crypto.tink.loadPublicKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.consent.client.common.toEncryptionPublicKey
import org.wfanet.measurement.consent.client.measurementconsumer.encryptRequisitionSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signMeasurementSpec
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupDetails
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.requisitionEntry
import org.wfanet.measurement.edpaggregator.v1alpha.QueueRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RegisterQueuedRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRequisitionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.groupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.listRequisitionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.registerQueuedRequisitionMetadataResponse
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.gcloud.testing.FunctionsFrameworkInvokerProcess
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.EnsureWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

/** Test class for the RequisitionFetcherFunction. */
class RequisitionFetcherFunctionTest {
  @Volatile private var ensureWorkItemRequest: EnsureWorkItemRequest? = null

  /** Temp folder to store Requisitions in test. */
  @Rule @JvmField val tempFolder = TemporaryFolder()

  /** Mutable config directory visible to the function process. */
  @Rule @JvmField val configFolder = TemporaryFolder()

  /** Mock of RequisitionsService. */
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { listRequisitions(any()) }
      .thenReturn(listRequisitionsResponse { requisitions += REQUISITION })
  }

  private val requisitionMetadataServiceMock: RequisitionMetadataServiceCoroutineImplBase =
    mockService {
      onBlocking { listRequisitionMetadata(any()) }.thenReturn(listRequisitionMetadataResponse {})
      onBlocking { batchCreateRequisitionMetadata(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<BatchCreateRequisitionMetadataRequest>(0)
          batchCreateRequisitionMetadataResponse {
            requisitionMetadata +=
              request.requestsList.mapIndexed { index, createRequest ->
                val source = createRequest.requisitionMetadata
                requisitionMetadata {
                  name = "$DATA_PROVIDER_NAME/requisitionMetadata/$index"
                  cmmsRequisition = source.cmmsRequisition
                  blobUri = source.blobUri
                  blobTypeUrl = source.blobTypeUrl
                  groupId = source.groupId
                  cmmsCreateTime = source.cmmsCreateTime
                  report = source.report
                  state = RequisitionMetadata.State.STORED
                  etag = "stored-etag-$index"
                }
              }
          }
        }
      onBlocking { registerQueuedRequisitionMetadata(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<RegisterQueuedRequisitionMetadataRequest>(0)
          registerQueuedRequisitionMetadataResponse {
            requisitionMetadata +=
              request.requestsList.mapIndexed { index, createRequest ->
                val source = createRequest.requisitionMetadata
                requisitionMetadata {
                  name = "$DATA_PROVIDER_NAME/requisitionMetadata/$index"
                  cmmsRequisition = source.cmmsRequisition
                  blobUri = source.blobUri
                  blobTypeUrl = source.blobTypeUrl
                  groupId = source.groupId
                  cmmsCreateTime = source.cmmsCreateTime
                  report = source.report
                  workItem = request.workItem
                  state = RequisitionMetadata.State.QUEUED
                  etag = "queued-etag-$index"
                }
              }
          }
        }
      onBlocking { queueRequisitionMetadata(any()) }
        .thenAnswer { invocation ->
          val request = invocation.getArgument<QueueRequisitionMetadataRequest>(0)
          requisitionMetadata {
            name = request.name
            workItem = request.workItem
            state = RequisitionMetadata.State.QUEUED
            etag = "queued-etag"
          }
        }
      onBlocking { refuseRequisitionMetadata(any()) }.thenReturn(requisitionMetadata {})
    }

  private val workItemsServiceMock: WorkItemsGrpcKt.WorkItemsCoroutineImplBase = mockService {
    onBlocking { ensureWorkItem(any()) }
      .thenAnswer { invocation ->
        val request = invocation.getArgument<EnsureWorkItemRequest>(0)
        ensureWorkItemRequest = request
        workItem {
          name = "workItems/${request.workItemId}"
          queue = request.workItem.queue
          workItemParams = request.workItem.workItemParams
        }
      }
  }

  private val eventGroupsServiceMock: EventGroupsGrpcKt.EventGroupsCoroutineImplBase = mockService {
    onBlocking { getEventGroup(any()) }
      .thenAnswer { invocation ->
        eventGroup {
          name = EVENT_GROUP_NAME
          eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID
        }
      }
  }

  private val traceparentMetadataKey =
    Metadata.Key.of("traceparent", Metadata.ASCII_STRING_MARSHALLER)
  @Volatile private var capturedTraceparent: String? = null

  private val traceContextCapturingInterceptor =
    object : ServerInterceptor {
      override fun <ReqT, RespT> interceptCall(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>,
      ): ServerCall.Listener<ReqT> {
        capturedTraceparent = headers.get(traceparentMetadataKey)
        return next.startCall(call, headers)
      }
    }

  /** Grpc server to handle calls to RequisitionService. */
  private lateinit var grpcServer: CommonServer

  /** Process for RequisitionFetcher Google cloud function. */
  private lateinit var functionProcess: FunctionsFrameworkInvokerProcess

  /** Sets up the infrastructure before each test. */
  @Before
  fun startInfra() {
    capturedTraceparent = null
    ensureWorkItemRequest = null
    copyConfig("requisition-fetcher-config.textproto")
    copyConfig(DIRECT_DISPATCH_CONFIG_BLOB_KEY)
    copyConfig("unknown-requisition-fetcher-direct-dispatch-config.textproto")

    /** Start gRPC server with mock Requisitions service */
    grpcServer =
      CommonServer.fromParameters(
          verboseGrpcLogging = true,
          certs = serverCerts,
          clientAuth = ClientAuth.REQUIRE,
          nameForLogging = "RequisitionsServiceServer",
          services =
            listOf(
              ServerInterceptors.intercept(
                requisitionsServiceMock.bindService(),
                traceContextCapturingInterceptor,
              ),
              eventGroupsServiceMock.bindService(),
              requisitionMetadataServiceMock.bindService(),
              workItemsServiceMock.bindService(),
            ),
        )
        .start()
    logger.info("Started gRPC server on port ${grpcServer.port}")

    startFunction(DIRECT_DISPATCH_CONFIG_BLOB_KEY)
  }

  private fun startFunction(directDispatchConfigBlobKey: String) {
    functionProcess =
      FunctionsFrameworkInvokerProcess(
        javaBinaryPath = FETCHER_BINARY_PATH,
        classTarget = GCF_TARGET,
      )
    runBlocking {
      val port =
        functionProcess.start(
          mapOf(
            "REQUISITION_FILE_SYSTEM_PATH" to tempFolder.root.path,
            "KINGDOM_TARGET" to "localhost:${grpcServer.port}",
            "METADATA_STORAGE_TARGET" to "localhost:${grpcServer.port}",
            "SECURE_COMPUTATION_CONTROL_PLANE_TARGET" to "localhost:${grpcServer.port}",
            "KINGDOM_CERT_HOST" to "localhost",
            "METADATA_STORAGE_CERT_HOST" to "localhost",
            "SECURE_COMPUTATION_CONTROL_PLANE_CERT_HOST" to "localhost",
            "PAGE_SIZE" to "10",
            "STORAGE_PATH_PREFIX" to STORAGE_PATH_PREFIX,
            "EDPA_CONFIG_STORAGE_BUCKET" to "file://${configFolder.root.toPath()}",
            "REQUISITION_FETCHER_DIRECT_DISPATCH_CONFIG_BLOB_KEY" to directDispatchConfigBlobKey,
            "GRPC_REQUEST_INTERVAL" to "1s",
            "OTEL_METRICS_EXPORTER" to "none",
            "OTEL_TRACES_EXPORTER" to "none",
            "OTEL_LOGS_EXPORTER" to "none",
          )
        )
      logger.info("Started RequisitionFetcher process on port $port")
    }
  }

  /** Cleans up resources after each test. */
  @After
  fun cleanUp() {
    functionProcess.close()
    grpcServer.shutdown()
  }

  @Test
  fun `service dispatches WorkItem when direct dispatch config exists`() {
    val url = "http://localhost:${functionProcess.port}"
    logger.info("Testing Cloud Function at: $url")
    val client = HttpClient.newHttpClient()
    val getRequest = HttpRequest.newBuilder().uri(URI.create(url)).GET().build()
    val getResponse = client.send(getRequest, BodyHandlers.ofString())
    logger.info("Response status: ${getResponse.statusCode()}")
    logger.info("Response body: ${getResponse.body()}")
    // Verify the function worked
    assertThat(getResponse.statusCode()).isEqualTo(200)
    val storageDir = tempFolder.root.toPath().resolve(DIRECT_STORAGE_PATH_PREFIX).toFile()

    val fileName: String? =
      storageDir.takeIf { it.exists() && it.isDirectory }?.listFiles()?.singleOrNull()?.name
    val storedRequisitionPath = Paths.get(DIRECT_STORAGE_PATH_PREFIX, fileName)
    val requisitionFile = tempFolder.root.toPath().resolve(storedRequisitionPath).toFile()
    assertThat(requisitionFile.exists()).isTrue()
    val anyMsg = Any.parseFrom(requisitionFile.readByteString())
    val groupedRequisitions: GroupedRequisitions = anyMsg.unpack(GroupedRequisitions::class.java)

    assertThat(groupedRequisitions.groupId).isNotEmpty()
    assertThat(
        groupedRequisitions.eventGroupMapList[0].details.collectionIntervalsList[0].startTime
      )
      .isEqualTo(EVENT_GROUP_ENTRY.value.collectionInterval.startTime)
    assertThat(groupedRequisitions.eventGroupMapList[0].details.collectionIntervalsList[0].endTime)
      .isEqualTo(EVENT_GROUP_ENTRY.value.collectionInterval.endTime)
    assertThat(groupedRequisitions.eventGroupMapList[0].details.eventGroupReferenceId)
      .isEqualTo(EVENT_GROUP_REFERENCE_ID)
    val workItemRequest = checkNotNull(ensureWorkItemRequest)
    assertThat(workItemRequest.workItemId)
      .isEqualTo("results-fulfiller-${groupedRequisitions.groupId}")
    assertThat(workItemRequest.workItem.queue).isEqualTo("results-fulfiller-queue")
  }

  @Test
  fun `service uses legacy dispatch when direct dispatch config is absent`() {
    functionProcess.close()
    ensureWorkItemRequest = null
    startFunction("missing-direct-dispatch-config.textproto")

    val response =
      HttpClient.newHttpClient()
        .send(
          HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:${functionProcess.port}"))
            .GET()
            .build(),
          BodyHandlers.ofString(),
        )

    assertThat(response.statusCode()).isEqualTo(200)
    val storageDir = tempFolder.root.toPath().resolve(STORAGE_PATH_PREFIX).toFile()
    assertThat(storageDir.listFiles()).isNotEmpty()
    assertThat(ensureWorkItemRequest).isNull()
  }

  @Test
  fun `service fails closed when direct dispatch config names unknown data provider`() {
    functionProcess.close()
    ensureWorkItemRequest = null
    startFunction("unknown-requisition-fetcher-direct-dispatch-config.textproto")

    val response =
      HttpClient.newHttpClient()
        .send(
          HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:${functionProcess.port}"))
            .GET()
            .build(),
          BodyHandlers.ofString(),
        )

    assertThat(response.statusCode()).isEqualTo(500)
    assertThat(response.body()).contains("direct-dispatch configuration")
    assertThat(ensureWorkItemRequest).isNull()
    assertThat(tempFolder.root.listFiles()).isEmpty()
  }

  @Test
  fun `service reloads direct dispatch config for activation and rollback`() {
    functionProcess.close()
    val mutableConfig = configFolder.root.toPath().resolve(MUTABLE_DIRECT_DISPATCH_CONFIG_BLOB_KEY)
    mutableConfig.toFile().writeText("")
    startFunction(MUTABLE_DIRECT_DISPATCH_CONFIG_BLOB_KEY)

    val legacyResponse = invokeFunction()

    assertThat(legacyResponse.statusCode()).isEqualTo(200)
    assertThat(ensureWorkItemRequest).isNull()
    assertThat(tempFolder.root.toPath().resolve(STORAGE_PATH_PREFIX).toFile().listFiles())
      .isNotEmpty()

    DIRECT_DISPATCH_CONFIG_SOURCE.toFile().copyTo(mutableConfig.toFile(), overwrite = true)
    ensureWorkItemRequest = null

    val directResponse = invokeFunction()

    assertThat(directResponse.statusCode()).isEqualTo(200)
    assertThat(ensureWorkItemRequest).isNotNull()
    assertThat(tempFolder.root.toPath().resolve(DIRECT_STORAGE_PATH_PREFIX).toFile().listFiles())
      .isNotEmpty()

    mutableConfig.toFile().writeText("")
    ensureWorkItemRequest = null

    val rollbackResponse = invokeFunction()

    assertThat(rollbackResponse.statusCode()).isEqualTo(200)
    assertThat(ensureWorkItemRequest).isNull()
  }

  @Test
  fun `trace context is propagated to outbound gRPC calls`() {
    val url = "http://localhost:${functionProcess.port}"
    val (expectedTraceId, traceparent) = newTraceparent()
    val client = HttpClient.newHttpClient()
    val getRequest =
      HttpRequest.newBuilder().uri(URI.create(url)).GET().header("traceparent", traceparent).build()

    val getResponse = client.send(getRequest, BodyHandlers.ofString())
    assertThat(getResponse.statusCode()).isEqualTo(200)

    val recordedTraceparent = capturedTraceparent
    assertThat(recordedTraceparent).isNotNull()
    val propagatedTraceId = traceIdFromTraceparent(recordedTraceparent!!)
    assertThat(propagatedTraceId).isEqualTo(expectedTraceId)
  }

  private fun invokeFunction(): java.net.http.HttpResponse<String> {
    return HttpClient.newHttpClient()
      .send(
        HttpRequest.newBuilder()
          .uri(URI.create("http://localhost:${functionProcess.port}"))
          .GET()
          .build(),
        BodyHandlers.ofString(),
      )
  }

  private fun copyConfig(fileName: String) {
    CONFIG_SOURCE_PATH.resolve(fileName)
      .toFile()
      .copyTo(configFolder.root.toPath().resolve(fileName).toFile(), overwrite = true)
  }

  companion object {
    private val FETCHER_BINARY_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "kotlin",
        "org",
        "wfanet",
        "measurement",
        "edpaggregator",
        "deploy",
        "gcloud",
        "requisitionfetcher",
        "testing",
        "InvokeRequisitionFetcherFunction",
      )
    private const val GCF_TARGET =
      "org.wfanet.measurement.edpaggregator.deploy.gcloud.requisitionfetcher.RequisitionFetcherFunction"
    private const val DIRECT_DISPATCH_CONFIG_BLOB_KEY =
      "requisition-fetcher-direct-dispatch-config.textproto"
    private const val MUTABLE_DIRECT_DISPATCH_CONFIG_BLOB_KEY =
      "mutable-requisition-fetcher-direct-dispatch-config.textproto"
    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
    private const val REQUISITION_NAME = "$DATA_PROVIDER_NAME/requisitions/foo"

    private const val EDP_DISPLAY_NAME = "edp7"
    private const val EDP_ID = "someDataProvider"
    private const val EDP_NAME = "dataProviders/$EDP_ID"

    private val SECRET_FILES_PATH: Path =
      checkNotNull(
        getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )
      )

    @JvmStatic
    protected val DATA_PROVIDER_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("${EDP_DISPLAY_NAME}_enc_public.tink").toFile())
        .toEncryptionPublicKey()

    private val LAST_EVENT_DATE = LocalDate.now()
    private val FIRST_EVENT_DATE = LAST_EVENT_DATE.minusDays(1)
    @JvmStatic
    protected val TIME_RANGE =
      OpenEndTimeRange.fromClosedDateRange(FIRST_EVENT_DATE..LAST_EVENT_DATE)

    protected const val EVENT_GROUP_NAME = "${EDP_NAME}/eventGroups/name"
    protected const val EVENT_GROUP_REFERENCE_ID = "some-event-group-reference-id"

    private val MC_PUBLIC_KEY =
      loadPublicKey(SECRET_FILES_PATH.resolve("mc_enc_public.tink").toFile())
        .toEncryptionPublicKey()

    private val DATA_PROVIDER_CERTIFICATE_KEY =
      DataProviderCertificateKey(EDP_ID, externalIdToApiId(8L))

    private val EDP_SIGNING_KEY =
      loadSigningKey("${EDP_DISPLAY_NAME}_cs_cert.der", "${EDP_DISPLAY_NAME}_cs_private.der")

    private val DATA_PROVIDER_CERTIFICATE = certificate {
      name = DATA_PROVIDER_CERTIFICATE_KEY.toName()
      x509Der = EDP_SIGNING_KEY.certificate.encoded.toByteString()
      subjectKeyIdentifier = EDP_SIGNING_KEY.certificate.subjectKeyIdentifier!!
    }

    private val EVENT_GROUP_ENTRY = eventGroupEntry {
      key = EVENT_GROUP_NAME
      value =
        RequisitionSpecKt.EventGroupEntryKt.value {
          collectionInterval = interval {
            startTime = TIME_RANGE.start.toProtoTime()
            endTime = TIME_RANGE.endExclusive.toProtoTime()
          }
          filter = eventFilter {}
        }
    }

    protected val REQUISITION_SPEC = requisitionSpec {
      events = RequisitionSpecKt.events { eventGroups += EVENT_GROUP_ENTRY }
      measurementPublicKey = MC_PUBLIC_KEY.pack()
      nonce = Random.Default.nextLong()
    }

    private fun loadSigningKey(
      certDerFileName: String,
      privateKeyDerFileName: String,
    ): SigningKeyHandle {
      return loadSigningKey(
        SECRET_FILES_PATH.resolve(certDerFileName).toFile(),
        SECRET_FILES_PATH.resolve(privateKeyDerFileName).toFile(),
      )
    }

    @JvmStatic protected val MC_SIGNING_KEY = loadSigningKey("mc_cs_cert.der", "mc_cs_private.der")

    fun createDeterministicId(requisition: Requisition): String {
      val digest = MessageDigest.getInstance("SHA-256").digest(requisition.name.toByteArray())
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest)
    }

    private val ENCRYPTED_REQUISITION_SPEC =
      encryptRequisitionSpec(
        signRequisitionSpec(REQUISITION_SPEC, MC_SIGNING_KEY),
        DATA_PROVIDER_PUBLIC_KEY,
      )

    private val MEASUREMENT_SPEC = measurementSpec {
      reportingMetadata = MeasurementSpecKt.reportingMetadata { report = "some-report" }
    }

    private val REQUISITION = requisition {
      name = REQUISITION_NAME
      measurementSpec = signMeasurementSpec(MEASUREMENT_SPEC, MC_SIGNING_KEY)
      encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
      dataProviderCertificate = DATA_PROVIDER_CERTIFICATE.name
      dataProviderPublicKey = DATA_PROVIDER_PUBLIC_KEY.pack()
      updateTime = timestamp { seconds = 100 }
    }

    private val GROUPED_REQUISITION = groupedRequisitions {
      eventGroupMap += eventGroupMapEntry {
        eventGroup = EVENT_GROUP_NAME
        details = eventGroupDetails {
          eventGroupReferenceId = EVENT_GROUP_REFERENCE_ID
          collectionIntervals += interval {
            startTime = EVENT_GROUP_ENTRY.value.collectionInterval.startTime
            endTime = EVENT_GROUP_ENTRY.value.collectionInterval.endTime
          }
        }
      }

      requisitions.add(requisitionEntry { requisition = Any.pack(REQUISITION) })
      groupId = createDeterministicId(REQUISITION)
    }

    private val STORAGE_PATH_PREFIX = "edp7"
    private val DIRECT_STORAGE_PATH_PREFIX = "edp7-v2"
    private val SECRETS_DIR: Path =
      getRuntimePath(
        Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
      )!!
    private val CONFIG_SOURCE_PATH =
      checkNotNull(
        getRuntimePath(
          Paths.get(
            "wfa_measurement_system",
            "src",
            "main",
            "kotlin",
            "org",
            "wfanet",
            "measurement",
            "edpaggregator",
            "deploy",
            "gcloud",
            "requisitionfetcher",
            "testing",
          )
        )
      )
    private val DIRECT_DISPATCH_CONFIG_SOURCE =
      CONFIG_SOURCE_PATH.resolve(DIRECT_DISPATCH_CONFIG_BLOB_KEY)
    private val serverCerts =
      SigningCerts.fromPemFiles(
        certificateFile = SECRETS_DIR.resolve("kingdom_tls.pem").toFile(),
        privateKeyFile = SECRETS_DIR.resolve("kingdom_tls.key").toFile(),
        trustedCertCollectionFile = SECRETS_DIR.resolve("edp7_root.pem").toFile(),
      )
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private fun newTraceparent(): Pair<String, String> {
      val traceId = randomHex(16)
      val spanId = randomHex(8)
      return traceId to "00-$traceId-$spanId-01"
    }

    private fun traceIdFromTraceparent(traceparent: String): String {
      val parts = traceparent.split("-")
      require(parts.size >= 4) { "Invalid traceparent header: $traceparent" }
      return parts[1]
    }

    private fun randomHex(numBytes: Int): String {
      val bytes = ByteArray(numBytes)
      Random.nextBytes(bytes)
      return bytes.toHex()
    }

    private fun ByteArray.toHex(): String {
      return buildString(size * 2) {
        for (byte in this@toHex) {
          append("%02x".format(byte.toInt() and 0xFF))
        }
      }
    }
  }
}
