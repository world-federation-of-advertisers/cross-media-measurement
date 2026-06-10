/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.validation

import com.google.auth.oauth2.IdToken
import com.google.auth.oauth2.IdTokenProvider
import com.google.common.truth.Truth.assertThat
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.time.Duration
import kotlin.test.assertFailsWith
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.DataProviderImpressionQueryRequest
import org.wfanet.measurement.api.v2alpha.DataProviderImpressionQueryResponseKt.impressionCount
import org.wfanet.measurement.api.v2alpha.dataProviderImpressionQueryRequest
import org.wfanet.measurement.api.v2alpha.dataProviderImpressionQueryResponse

@RunWith(JUnit4::class)
class ValidationCloudFunctionClientTest {

  private lateinit var fakeCloudFunction: FakeCloudFunction
  private lateinit var endpointUri: String

  private val client = ValidationCloudFunctionClient(idTokenProvider = FakeIdTokenProvider())

  @Before
  fun startServer() {
    val port = ServerSocket(0).use { it.localPort }
    fakeCloudFunction = FakeCloudFunction()
    fakeCloudFunction.start(port)
    endpointUri = "http://localhost:$port"
  }

  @After
  fun stopServer() {
    fakeCloudFunction.stop()
  }

  @Test
  fun `call returns parsed response on success`() {
    fakeCloudFunction.responseBody =
      dataProviderImpressionQueryResponse {
          requestId = REQUEST_ID
          result = impressionCount { value = 12_345L }
        }
        .toByteArray()

    val response = client.call(endpointUri, REQUEST, TIMEOUT)

    assertThat(response.hasResult()).isTrue()
    assertThat(response.result.value).isEqualTo(12_345L)
    // The request was transmitted to the endpoint as binary protobuf.
    assertThat(DataProviderImpressionQueryRequest.parseFrom(fakeCloudFunction.lastRequestBody))
      .isEqualTo(REQUEST)
  }

  @Test
  fun `call sends bearer authorization header`() {
    fakeCloudFunction.responseBody =
      dataProviderImpressionQueryResponse {
          requestId = REQUEST_ID
          result = impressionCount { value = 1L }
        }
        .toByteArray()

    client.call(endpointUri, REQUEST, TIMEOUT)

    assertThat(fakeCloudFunction.lastAuthorizationHeader).isEqualTo("Bearer $JWT_TOKEN")
  }

  @Test
  fun `call throws with http status on server error`() {
    fakeCloudFunction.responseStatus = 500

    val exception =
      assertFailsWith<CloudFunctionException> { client.call(endpointUri, REQUEST, TIMEOUT) }
    assertThat(exception.httpStatus).isEqualTo(500)
  }

  @Test
  fun `call throws with http status on client error`() {
    fakeCloudFunction.responseStatus = 404

    val exception =
      assertFailsWith<CloudFunctionException> { client.call(endpointUri, REQUEST, TIMEOUT) }
    assertThat(exception.httpStatus).isEqualTo(404)
  }

  @Test
  fun `call throws with http status on unparseable response body`() {
    // A lone field tag with no payload is a truncated, malformed protobuf message.
    fakeCloudFunction.responseBody = byteArrayOf(0x0A)

    val exception =
      assertFailsWith<CloudFunctionException> { client.call(endpointUri, REQUEST, TIMEOUT) }
    assertThat(exception.httpStatus).isEqualTo(200)
  }

  @Test
  fun `call throws without http status when endpoint is unreachable`() {
    val unusedPort = ServerSocket(0).use { it.localPort }

    val exception =
      assertFailsWith<CloudFunctionException> {
        client.call("http://localhost:$unusedPort", REQUEST, TIMEOUT)
      }
    assertThat(exception.httpStatus).isNull()
  }

  companion object {
    private const val REQUEST_ID = "f47ac10b-58cc-4372-a567-0e02b2c3d479"
    private val TIMEOUT: Duration = Duration.ofSeconds(10)

    // Sample (expired) JWT used only to satisfy IdToken parsing in tests.
    private const val JWT_TOKEN =
      "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMiwiZXhwIjoxNTE2MjQyNjIyfQ.KMUFsIDTnFmyG3nMiGM6H9FNFUROf3wh7SmqJp-QV30"

    private val REQUEST: DataProviderImpressionQueryRequest = dataProviderImpressionQueryRequest {
      requestId = REQUEST_ID
      dataProvider = "dataProviders/edp-1"
    }
  }
}

/** [IdTokenProvider] that returns a fixed sample token without contacting Google. */
private class FakeIdTokenProvider : IdTokenProvider {
  override fun idTokenWithAudience(
    targetAudience: String,
    options: MutableList<IdTokenProvider.Option>?,
  ): IdToken = IdToken.create(JWT_TOKEN)

  companion object {
    private const val JWT_TOKEN =
      "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMiwiZXhwIjoxNTE2MjQyNjIyfQ.KMUFsIDTnFmyG3nMiGM6H9FNFUROf3wh7SmqJp-QV30"
  }
}

/** In-process HTTP server standing in for a DataProvider's validation cloud function. */
private class FakeCloudFunction {
  private lateinit var server: HttpServer

  /** HTTP status code to return. */
  var responseStatus: Int = 200

  /** Response body bytes to return. */
  var responseBody: ByteArray = ByteArray(0)

  /** Body of the most recently received request. */
  var lastRequestBody: ByteArray = ByteArray(0)
    private set

  /** `Authorization` header of the most recently received request. */
  var lastAuthorizationHeader: String? = null
    private set

  fun start(port: Int) {
    server = HttpServer.create(InetSocketAddress(port), 0)
    server.createContext("/") { exchange ->
      lastRequestBody = exchange.requestBody.readBytes()
      lastAuthorizationHeader = exchange.requestHeaders.getFirst("Authorization")
      val contentLength = if (responseBody.isEmpty()) -1L else responseBody.size.toLong()
      exchange.sendResponseHeaders(responseStatus, contentLength)
      exchange.responseBody.use { it.write(responseBody) }
    }
    server.executor = null
    server.start()
  }

  fun stop() {
    server.stop(0)
  }
}
