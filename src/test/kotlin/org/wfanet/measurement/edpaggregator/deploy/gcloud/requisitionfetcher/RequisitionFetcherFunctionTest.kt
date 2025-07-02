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
import com.google.type.interval
import io.netty.handler.ssl.ClientAuth
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse.BodyHandlers
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDate
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
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.certificate
import org.wfanet.measurement.api.v2alpha.eventGroup
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.requisition
import org.wfanet.measurement.api.v2alpha.requisitionSpec
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
import org.wfanet.measurement.consent.client.measurementconsumer.signRequisitionSpec
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupDetails
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.eventGroupMapEntry
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitionsKt.requisitionEntry
import org.wfanet.measurement.edpaggregator.v1alpha.groupedRequisitions
import org.wfanet.measurement.gcloud.testing.FunctionsFrameworkInvokerProcess

/** Test class for the RequisitionFetcherFunction. */
class RequisitionFetcherFunctionTest {
  /** Temp folder to store Requisitions in test. */
  @Rule @JvmField val tempFolder = TemporaryFolder()

  /** Mock of RequisitionsService. */
  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { listRequisitions(any()) }
      .thenReturn(listRequisitionsResponse { requisitions += REQUISITION })
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

  /** Grpc server to handle calls to RequisitionService. */
  private lateinit var grpcServer: CommonServer

  /** Process for RequisitionFetcher Google cloud function. */
  private lateinit var functionProcess: FunctionsFrameworkInvokerProcess

  /** Sets up the infrastructure before each test. */
  @Before
  fun startInfra() {

    /** Start gRPC server with mock Requisitions service */
    grpcServer =
      CommonServer.fromParameters(
          verboseGrpcLogging = true,
          certs = serverCerts,
          clientAuth = ClientAuth.REQUIRE,
          nameForLogging = "RequisitionsServiceServer",
          services =
            listOf(requisitionsServiceMock.bindService(), eventGroupsServiceMock.bindService()),
        )
        .start()
    logger.info("Started gRPC server on port ${grpcServer.port}")

    /** Start the RequisitionFetcherFunction process */
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
            "KINGDOM_CERT_HOST" to "localhost",
            "PAGE_SIZE" to "10",
            "STORAGE_PATH_PREFIX" to STORAGE_PATH_PREFIX,
            "EDPA_CONFIG_STORAGE_BUCKET" to REQUISITION_CONFIG_FILE_SYSTEM_PATH,
            "GRPC_REQUEST_INTERVAL" to "1s",
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

  /** Tests the RequisitionFetcherFunction as a local process. */
  @Test
  fun `test RequisitionFetcherFunction as local process`() {
    val url = "http://localhost:${functionProcess.port}"
    logger.info("Testing Cloud Function at: $url")
    val client = HttpClient.newHttpClient()
    val getRequest = HttpRequest.newBuilder().uri(URI.create(url)).GET().build()
    val getResponse = client.send(getRequest, BodyHandlers.ofString())
    logger.info("Response status: ${getResponse.statusCode()}")
    logger.info("Response body: ${getResponse.body()}")
    // Verify the function worked
    assertThat(getResponse.statusCode()).isEqualTo(200)
    val storageDir = tempFolder.root.toPath().resolve(STORAGE_PATH_PREFIX).toFile()

    val fileName: String? =
      storageDir.takeIf { it.exists() && it.isDirectory }?.listFiles()?.singleOrNull()?.name
    val storedRequisitionPath = Paths.get(STORAGE_PATH_PREFIX, fileName)
    val requisitionFile = tempFolder.root.toPath().resolve(storedRequisitionPath).toFile()
    assertThat(requisitionFile.exists()).isTrue()
    val storedAny = Any.parseFrom(requisitionFile.readBytes())
    assertThat(requisitionFile.readByteString())
      .isEqualTo(Any.pack(GROUPED_REQUISITION).toByteString())
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

    private val ENCRYPTED_REQUISITION_SPEC =
      encryptRequisitionSpec(
        signRequisitionSpec(REQUISITION_SPEC, MC_SIGNING_KEY),
        DATA_PROVIDER_PUBLIC_KEY,
      )

    private val REQUISITION = requisition {
      name = REQUISITION_NAME
      encryptedRequisitionSpec = ENCRYPTED_REQUISITION_SPEC
      dataProviderCertificate = DATA_PROVIDER_CERTIFICATE.name
      dataProviderPublicKey = DATA_PROVIDER_PUBLIC_KEY.pack()
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
    }

    private val STORAGE_PATH_PREFIX = "edp7"
    private val SECRETS_DIR: Path =
      getRuntimePath(
        Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
      )!!
    private val REQUISITION_CONFIG_FILE_SYSTEM_PATH =
      "file://" +
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
        )!!
    private val serverCerts =
      SigningCerts.fromPemFiles(
        certificateFile = SECRETS_DIR.resolve("kingdom_tls.pem").toFile(),
        privateKeyFile = SECRETS_DIR.resolve("kingdom_tls.key").toFile(),
        trustedCertCollectionFile = SECRETS_DIR.resolve("edp7_root.pem").toFile(),
      )
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
