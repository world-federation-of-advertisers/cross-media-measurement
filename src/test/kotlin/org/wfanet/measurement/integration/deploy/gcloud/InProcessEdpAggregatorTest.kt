/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.wfanet.measurement.integration.deploy.gcloud

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.protobuf.Any
import io.netty.handler.ssl.ClientAuth
import java.nio.file.Path
import java.nio.file.Paths
import java.util.logging.Logger
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.gcloud.testing.FunctionsFrameworkInvokerProcess
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.api.v2alpha.requisition
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.gcloud.gcs.testing.GcsSubscribingStorageClient

@RunWith(JUnit4::class)
class InProcessEdpAggregatorTest() {

  private lateinit var mockKingdom: CommonServer
  // TODO(@Marco): Replace with actual in process Control Plane
  private lateinit var mockControlPlane: CommonServer
  private lateinit var dataWatcherFunctionProcess: FunctionsFrameworkInvokerProcess
  private lateinit var requisitionFetcherFunctionProcess: FunctionsFrameworkInvokerProcess

  private val requisitionsServiceMock: RequisitionsCoroutineImplBase = mockService {
    onBlocking { listRequisitions(any()) }
      .thenReturn(listRequisitionsResponse { requisitions += REQUISITION })
  }
  private val workItemsServiceMock: WorkItemsCoroutineImplBase = mockService {
    onBlocking { createWorkItem(any()) }.thenReturn(workItem { name = "some-work-item-name" })
  }

  @Before
  fun initStorageClient() {
    val storage = LocalStorageHelper.getOptions().service
    storageClient = GcsStorageClient(storage, BUCKET)
  }

  @Before
  fun startInfra() {
    mockControlPlane =
      CommonServer.fromParameters(
          verboseGrpcLogging = true,
          certs = serverCerts,
          clientAuth = ClientAuth.REQUIRE,
          nameForLogging = "WorkItemsServer",
          services = listOf(workItemsServiceMock.bindService()),
        )
        .start()
    logger.info("Started Mock Control Plane gRPC server on port ${mockKingdom.port}")
    mockKingdom =
      CommonServer.fromParameters(
          verboseGrpcLogging = true,
          certs = serverCerts,
          clientAuth = ClientAuth.REQUIRE,
          nameForLogging = "KingdomServer",
          services = listOf(requisitionsServiceMock.bindService()),
        )
        .start()
    logger.info("Started Mock Kingdom gRPC server on port ${mockKingdom.port}")

    dataWatcherFunctionProcess =
      FunctionsFrameworkInvokerProcess(
        javaBinaryPath = DATA_WATCHER_FUNCTION_BINARY_PATH,
        classTarget = DATA_WATCHER_GCF_TARGET,
      )
    requisitionFetcherFunctionProcess =
      FunctionsFrameworkInvokerProcess(
        javaBinaryPath = REQUISITION_FETCHER_BINARY_PATH,
        classTarget = REQUISITION_FETCHER_GCF_TARGET,
      )
  }

  /** Cleans up resources after each test. */
  @After
  fun cleanUp() {
    dataWatcherFunctionProcess.close()
    requisitionFetcherFunctionProcess.close()
    mockKingdom.shutdown()
    mockControlPlane.shutdown()
  }

  // TODO: Include Event Group Registration once that is more stable
  @Test
  fun `Fetches requistions, and runs TEE App`() {
    val requisitionFetcherPort = runBlocking {
      requisitionFetcherFunctionProcess.start(
        mapOf(
          "REQUISITION_FILE_SYSTEM_PATH" to tempFolder.root.path,
          "KINGDOM_TARGET" to "localhost:${mockKingdom.port}",
          "KINGDOM_CERT_HOST" to "localhost",
          "REQUISITIONS_GCS_PROJECT_ID" to "test-project-id",
          "REQUISITIONS_GCS_BUCKET" to "test-bucket",
          "DATA_PROVIDER_NAME" to DATA_PROVIDER_NAME,
          "PAGE_SIZE" to "10",
          "STORAGE_PATH_PREFIX" to STORAGE_PATH_PREFIX,
          "CERT_FILE_PATH" to SECRETS_DIR.resolve("edp1_tls.pem").toString(),
          "PRIVATE_KEY_FILE_PATH" to SECRETS_DIR.resolve("edp1_tls.key").toString(),
          "CERT_COLLECTION_FILE_PATH" to SECRETS_DIR.resolve("kingdom_root.pem").toString(),
        )
      )
    }
    logger.info("Started RequisitionFetcher process on port $requisitionFetcherPort")
    val dataWatcherPort = runBlocking {
      dataWatcherFunctionProcess.start(
        mapOf(
          "DATA_WATCHER_CONFIG_RUN_TIME_PATH" to
            Paths.get(
                "wfa_measurement_system",
                "src",
                "test",
                "kotlin",
                "org",
                "wfanet",
                "measurement",
                "securecomputation",
                "deploy",
                "gcloud",
                "datawatcher",
                "data_watcher_config.textproto",
              )
              .toString(),
          "CONTROL_PLANE_PROJECT_ID" to PROJECT_ID,
          "CONTROL_PLANE_TARGET" to "localhost:${mockKingdom.port}",
          "CONTROL_PLANE_CERT_HOST" to "localhost",
          "CONTROL_PLANE_CHANNEL_SHUTDOWN_DURATION_SECONDS" to "3",
          "CERT_FILE_PATH" to SECRETS_DIR.resolve("edp1_tls.pem").toString(),
          "PRIVATE_KEY_FILE_PATH" to SECRETS_DIR.resolve("edp1_tls.key").toString(),
          "CERT_COLLECTION_FILE_PATH" to SECRETS_DIR.resolve("kingdom_root.pem").toString(),
        )
      )
    }
    logger.info("Started DataWatcher process on port $dataWatcherPort")


    // Watch path that requisition fetcher will write to
    // Requisition Fetcher Fetch
    // Manually Trigger Data Watcher
    // TEE App Fulfills to Kingdom
  }

  companion object {
    private val DATA_WATCHER_FUNCTION_BINARY_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "test",
        "kotlin",
        "org",
        "wfanet",
        "measurement",
        "securecomputation",
        "deploy",
        "gcloud",
        "datawatcher",
        "InvokeDataWatcherFunction",
      )
    private val DATA_WATCHER_GCF_TARGET =
      "org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher.DataWatcherFunction"

    private val PROJECT_ID = "some-project-id"
    private val REQUISITION_FETCHER_BINARY_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "test",
        "kotlin",
        "org",
        "wfanet",
        "measurement",
        "edpaggregator",
        "deploy",
        "gcloud",
        "requisitionfetcher",
        "InvokeRequisitionFetcherFunction",
      )
    private const val REQUISITION_FETCHER_GCF_TARGET =
      "org.wfanet.measurement.edpaggregator.deploy.gcloud.requisitionfetcher.RequisitionFetcherFunction"
    private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
    private const val REQUISITION_NAME = "$DATA_PROVIDER_NAME/requisitions/foo"
    private val REQUISITION = requisition { name = REQUISITION_NAME }
    private val PACKED_REQUISITION = Any.pack(REQUISITION)
    private val STORAGE_PATH_PREFIX = "storage-path-prefix"
    private val SECRETS_DIR: Path =
      getRuntimePath(
        Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
      )!!
    private val serverCerts =
      SigningCerts.fromPemFiles(
        certificateFile = SECRETS_DIR.resolve("kingdom_tls.pem").toFile(),
        privateKeyFile = SECRETS_DIR.resolve("kingdom_tls.key").toFile(),
        trustedCertCollectionFile = SECRETS_DIR.resolve("edp1_root.pem").toFile(),
      )
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val BUCKET: String = "some-bucket"
  }
}
