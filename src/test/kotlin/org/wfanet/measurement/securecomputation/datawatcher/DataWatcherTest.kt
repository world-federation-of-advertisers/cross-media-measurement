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

package org.wfanet.measurement.securecomputation.datawatcher

import com.google.common.truth.Truth.assertThat
import com.google.gson.Gson
import com.google.protobuf.Any
import com.google.protobuf.Int32Value
import com.google.protobuf.Struct
import com.google.protobuf.Value
import com.google.protobuf.kotlin.unpack
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.ServerSocket
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.config.securecomputation.DataWatcherConfigKt.controlPlaneConfig
import org.wfanet.measurement.config.securecomputation.DataWatcherConfigKt.webhookConfig
import org.wfanet.measurement.config.securecomputation.dataWatcherConfig
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.DataPathDetails
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineImplBase
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub

@RunWith(JUnit4::class)
class DataWatcherTest() {

  private val workItemsServiceMock: WorkItemsCoroutineImplBase = mockService {}

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(workItemsServiceMock) }

  private val workItemsStub: WorkItemsCoroutineStub by lazy {
    WorkItemsCoroutineStub(grpcTestServerRule.channel)
  }

  @Test
  fun `sends to control plane sink when path matches`() {
    runBlocking {
      val topicId = "test-topic-id"
      val appConfig = Int32Value.newBuilder().setValue(5).build()

      val dataWatcherConfig = dataWatcherConfig {
        sourcePathRegex = "test-schema://test-bucket/path-to-watch/(.*)"
        this.controlPlaneConfig = controlPlaneConfig {
          queue = topicId
          this.appConfig = Any.pack(appConfig)
        }
      }

      val dataWatcher = DataWatcher(workItemsStub, listOf(dataWatcherConfig))

      dataWatcher.receivePath("test-schema://test-bucket/path-to-watch/some-data")
      val createWorkItemRequestCaptor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsServiceMock, times(1)) {
        createWorkItem(createWorkItemRequestCaptor.capture())
      }
      assertThat(createWorkItemRequestCaptor.allValues.single().workItem.queue).isEqualTo(topicId)
      val workItemParams = createWorkItemRequestCaptor.allValues.single().workItem.workItemParams
      assertThat(workItemParams.config.unpack<DataPathDetails>().dataPath)
        .isEqualTo("test-schema://test-bucket/path-to-watch/some-data")
      val workItemAppConfig =
        workItemParams.config.unpack<DataPathDetails>().config.unpack<Int32Value>()
      assertThat(workItemAppConfig).isEqualTo(appConfig)
    }
  }

  @Test
  fun `sends to webhook sink when path matches`() {
    runBlocking {
      val appConfig =
        Struct.newBuilder()
          .putFields("some-key", Value.newBuilder().setStringValue("some-value").build())
          .build()
      val localPort = ServerSocket(0).use { it.localPort }
      val dataWatcherConfig = dataWatcherConfig {
        sourcePathRegex = "test-schema://test-bucket/path-to-watch/(.*)"
        this.webhookConfig = webhookConfig {
          endpointUri = "http://localhost:$localPort"
          this.appConfig = appConfig
        }
      }
      val server = TestServer()
      server.start(localPort)

      val dataWatcher = DataWatcher(workItemsStub, listOf(dataWatcherConfig))

      dataWatcher.receivePath("test-schema://test-bucket/path-to-watch/some-data")
      val createWorkItemRequestCaptor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsServiceMock, times(0)) {
        createWorkItem(createWorkItemRequestCaptor.capture())
      }
      val gson = Gson()
      assertThat(gson.fromJson(server.getLastRequest(), Map::class.java))
        .isEqualTo(mapOf("some-key" to "some-value"))
      server.stop()
    }
  }

  @Test
  fun `does not create WorkItem with path does not match`() {
    runBlocking {
      val topicId = "test-topic-id"

      val dataWatcherConfig = dataWatcherConfig {
        sourcePathRegex = "test-schema://test-bucket/path-to-watch/(.*)"
        this.controlPlaneConfig = controlPlaneConfig {
          queue = topicId
          appConfig = Any.pack(Int32Value.newBuilder().setValue(5).build())
        }
      }

      val dataWatcher = DataWatcher(workItemsStub, listOf(dataWatcherConfig))
      dataWatcher.receivePath("test-schema://test-bucket/some-other-path/some-data")

      val createWorkItemRequestCaptor = argumentCaptor<CreateWorkItemRequest>()
      verifyBlocking(workItemsServiceMock, times(0)) {
        createWorkItem(createWorkItemRequestCaptor.capture())
      }
    }
  }
}

private class TestServer() {
  private lateinit var handler: ServerHandler
  private lateinit var httpServer: HttpServer

  fun start(port: Int): HttpServer {
    httpServer = HttpServer.create(InetSocketAddress(port), 0)
    handler = ServerHandler()
    httpServer.createContext("/", handler)
    httpServer.setExecutor(null)
    httpServer.start()
    return httpServer
  }

  fun stop() {
    httpServer.stop(0)
  }

  fun getLastRequest(): String {
    return handler.requestBody
  }

  class ServerHandler : HttpHandler {
    lateinit var requestBody: String

    override fun handle(t: HttpExchange) {
      val requestBodyBytes = t.requestBody.readBytes()
      requestBody = requestBodyBytes.toString(Charsets.UTF_8)
      val response = "Success"
      t.sendResponseHeaders(200, response.length.toLong())
      val os = t.responseBody
      os.write(response.toByteArray())
      os.close()
    }
  }
}
