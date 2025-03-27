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

package org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any
import com.google.protobuf.Int32Value
import com.google.protobuf.kotlin.toByteStringUtf8
import com.google.protobuf.kotlin.unpack
import io.netty.handler.ssl.ClientAuth
import java.nio.file.Path
import java.nio.file.Paths
import java.util.logging.Logger
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.gcloud.gcs.testing.GcsSubscribingStorageClient
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemConfig
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfigKt.controlPlaneConfig
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.dataWatcherConfig
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.dataWatcherConfigs

@RunWith(JUnit4::class)
class DataWatcherFunctionTest() {
  private lateinit var storageClient: GcsStorageClient
  private lateinit var grpcServer: CommonServer

  private val workItemsServiceMock: WorkItemsCoroutineImplBase = mockService {
    onBlocking { createWorkItem(any()) }.thenReturn(workItem { name = "some-work-item-name" })
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(workItemsServiceMock) }

  @Before
  fun initStorageClient() {
    val storage = LocalStorageHelper.getOptions().service
    storageClient = GcsStorageClient(storage, BUCKET)
  }

  @Before
  fun startInfra() {
    /** Start gRPC server with mock Requisitions service */
    grpcServer =
      CommonServer.fromParameters(
          verboseGrpcLogging = true,
          certs = serverCerts,
          clientAuth = ClientAuth.REQUIRE,
          nameForLogging = "WorkItemsServer",
          services = listOf(workItemsServiceMock.bindService()),
        )
        .start()
    logger.info("Started gRPC server on port ${grpcServer.port}")
    System.setProperty("CONTROL_PLANE_TARGET", "localhost:${grpcServer.port}")
    System.setProperty("CONTROL_PLANE_CERT_HOST", "localhost")
    System.setProperty("CERT_FILE_PATH", SECRETS_DIR.resolve("edp1_tls.pem").toString())
    System.setProperty("PRIVATE_KEY_FILE_PATH", SECRETS_DIR.resolve("edp1_tls.key").toString())
    System.setProperty(
      "CERT_COLLECTION_FILE_PATH",
      SECRETS_DIR.resolve("kingdom_root.pem").toString(),
    )
  }

  @After
  fun cleanUp() {
    grpcServer.shutdown()
  }

  @Test
  fun `creates WorkItem when path matches`() {
    runBlocking {
      val topicId = "test-topic-id"
      val subscribingStorageClient = GcsSubscribingStorageClient(storageClient)

      val dataWatcherConfigs = dataWatcherConfigs {
        configs += dataWatcherConfig {
          sourcePathRegex = "gs://$BUCKET/path-to-watch/(.*)"
          this.controlPlaneConfig = controlPlaneConfig {
            queueName = topicId
            appConfig = Any.pack(Int32Value.newBuilder().setValue(5).build())
          }
        }
      }
      System.setProperty("DATA_WATCHER_CONFIGS", dataWatcherConfigs.toString())

      val dataWatcher = DataWatcherFunction()
      subscribingStorageClient.subscribe(dataWatcher)

      subscribingStorageClient.writeBlob(
        "path-to-watch/some-data",
        flowOf("some-data".toByteStringUtf8()),
      )
      val createWorkItemRequestCaptor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsServiceMock, times(1)) {
        createWorkItem(createWorkItemRequestCaptor.capture())
      }
      assertThat(createWorkItemRequestCaptor.allValues.single().workItem.queue).isEqualTo(topicId)
      assertThat(
          createWorkItemRequestCaptor.allValues
            .single()
            .workItem
            .workItemParams
            .unpack(WorkItemConfig::class.java)
            .dataPath
        )
        .isEqualTo("gs://$BUCKET/path-to-watch/some-data")
    }
  }

  @Test
  fun `does not create WorkItem with path does not match`() {
    runBlocking {
      val topicId = "test-topic-id"
      val subscribingStorageClient = GcsSubscribingStorageClient(storageClient)

      val dataWatcherConfigs = dataWatcherConfigs {
        configs += dataWatcherConfig {
          sourcePathRegex = "gs://$BUCKET/path-to-watch/(.*)"
          this.controlPlaneConfig = controlPlaneConfig {
            queueName = topicId
            appConfig = Any.pack(Int32Value.newBuilder().setValue(5).build())
          }
        }
      }
      System.setProperty("DATA_WATCHER_CONFIGS", dataWatcherConfigs.toString())

      val dataWatcher = DataWatcherFunction()
      subscribingStorageClient.subscribe(dataWatcher)

      subscribingStorageClient.writeBlob(
        "some-other-path/some-data",
        flowOf("some-data".toByteStringUtf8()),
      )
      val createWorkItemRequestCaptor = argumentCaptor<CreateWorkItemRequest>()
      verify(workItemsServiceMock, times(0)).createWorkItem(createWorkItemRequestCaptor.capture())
    }
  }

  companion object {
    private const val BUCKET = "test-bucket"
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
  }
}
