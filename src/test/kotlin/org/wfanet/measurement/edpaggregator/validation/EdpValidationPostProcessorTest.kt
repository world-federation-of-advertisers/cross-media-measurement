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

package org.wfanet.measurement.edpaggregator.validation

import com.google.auth.oauth2.IdToken
import com.google.auth.oauth2.IdTokenProvider
import com.google.common.truth.Truth.assertThat
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.ServerSocket
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.DataProviderImpressionQueryResponse
import org.wfanet.measurement.api.v2alpha.DataProviderImpressionQueryResponseKt.impressionCount
import org.wfanet.measurement.api.v2alpha.DataProviderImpressionQueryResponseKt.skipDetail
import org.wfanet.measurement.api.v2alpha.dataProviderImpressionQueryResponse
import org.wfanet.measurement.config.edpaggregator.dataProviderValidationConfig
import org.wfanet.measurement.config.edpaggregator.dataProviderValidationConfigs
import org.wfanet.measurement.config.edpaggregator.httpEndpointSink
import org.wfanet.measurement.config.edpaggregator.toleranceBand
import org.wfanet.measurement.config.edpaggregator.toleranceConfig

@RunWith(JUnit4::class)
class EdpValidationPostProcessorTest {

  private lateinit var fakeCloudFunction: FakeCloudFunction
  private lateinit var endpointUri: String

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

  private fun postProcessor() =
    EdpValidationPostProcessor(
      configs =
        dataProviderValidationConfigs {
          configs += dataProviderValidationConfig {
            dataProvider = DATA_PROVIDER
            endpoint = httpEndpointSink {
              endpointUri = this@EdpValidationPostProcessorTest.endpointUri
            }
            tolerance = DEFAULT_TOLERANCE
          }
        },
      client = ValidationCloudFunctionClient(idTokenProvider = FakeIdTokenProvider()),
    )

  @Test
  fun `validate returns PASSED when deviation is within tolerance`() {
    fakeCloudFunction.responseBody = resultResponse(10_000_000L)

    val result = postProcessor().validate(listOf(row(reportedImpressions = 10_000_000L)))

    assertThat(result).isEqualTo(EdpValidationPostProcessor.ReportValidationResult.PASSED)
  }

  @Test
  fun `validate returns FAILED when deviation exceeds failure threshold`() {
    fakeCloudFunction.responseBody = resultResponse(10_000_000L)

    // Reported is 15% below the publisher count, exceeding the 10% failure threshold.
    val result = postProcessor().validate(listOf(row(reportedImpressions = 8_500_000L)))

    assertThat(result).isEqualTo(EdpValidationPostProcessor.ReportValidationResult.FAILED)
  }

  @Test
  fun `validate returns SKIPPED when cloud function call fails`() {
    fakeCloudFunction.responseStatus = 500

    // A cloud function error never fails the report; with no verdict produced the result is
    // SKIPPED.
    val result = postProcessor().validate(listOf(row(reportedImpressions = 8_500_000L)))

    assertThat(result).isEqualTo(EdpValidationPostProcessor.ReportValidationResult.SKIPPED)
  }

  @Test
  fun `validate returns SKIPPED when response is skipped`() {
    fakeCloudFunction.responseBody =
      dataProviderImpressionQueryResponse {
          requestId = REQUEST_ID
          skipped = skipDetail {
            reason = DataProviderImpressionQueryResponse.SkipDetail.SkipReason.ENTITY_NOT_FOUND
            detail = "entity not found"
          }
        }
        .toByteArray()

    val result = postProcessor().validate(listOf(row(reportedImpressions = 8_500_000L)))

    assertThat(result).isEqualTo(EdpValidationPostProcessor.ReportValidationResult.SKIPPED)
  }

  @Test
  fun `validate returns NOT_CONFIGURED when no row matches a config`() {
    val result =
      postProcessor()
        .validate(
          listOf(row(dataProviderName = "dataProviders/unconfigured", reportedImpressions = 1L))
        )

    assertThat(result).isEqualTo(EdpValidationPostProcessor.ReportValidationResult.NOT_CONFIGURED)
    // An unconfigured DataProvider is never queried.
    assertThat(fakeCloudFunction.requestCount).isEqualTo(0)
  }

  @Test
  fun `validate returns SKIPPED when publisher count is below the minimum`() {
    // Publisher count 500 is below the configured minimum of 1000, so no comparison is made.
    fakeCloudFunction.responseBody = resultResponse(500L)

    val result = postProcessor().validate(listOf(row(reportedImpressions = 500L)))

    assertThat(result).isEqualTo(EdpValidationPostProcessor.ReportValidationResult.SKIPPED)
  }

  @Test
  fun `validate scales the publisher count by the VID sampling width`() {
    fakeCloudFunction.responseBody = resultResponse(10_000_000L)

    // With a 0.1 sampling width the expected count is 1,000,000, matching the reported count, so a
    // naive unscaled comparison (1M vs 10M) would FAIL but the scaled comparison PASSES.
    val result =
      postProcessor()
        .validate(listOf(row(reportedImpressions = 1_000_000L, vidSamplingWidth = 0.1)))

    assertThat(result).isEqualTo(EdpValidationPostProcessor.ReportValidationResult.PASSED)
  }

  private fun row(
    dataProviderName: String = DATA_PROVIDER,
    reportedImpressions: Long,
    vidSamplingWidth: Double = 1.0,
  ): EdpValidationPostProcessor.ImpressionDataRow =
    EdpValidationPostProcessor.ImpressionDataRow(
      dataProviderName = dataProviderName,
      entityType = "campaign",
      entityId = "campaign-1",
      startTimeSeconds = 1_700_000_000L,
      endTimeSeconds = 1_700_086_400L,
      reportedImpressions = reportedImpressions,
      vidSamplingWidth = vidSamplingWidth,
    )

  private fun resultResponse(value: Long): ByteArray =
    dataProviderImpressionQueryResponse {
        requestId = REQUEST_ID
        result = impressionCount { this.value = value }
      }
      .toByteArray()

  companion object {
    private const val DATA_PROVIDER = "dataProviders/edp-1"
    private const val REQUEST_ID = "f47ac10b-58cc-4372-a567-0e02b2c3d479"

    private val DEFAULT_TOLERANCE = toleranceConfig {
      warning = toleranceBand {
        thresholdFraction = 0.02
        minimumAbsoluteDeviation = 1
      }
      failure = toleranceBand {
        thresholdFraction = 0.10
        minimumAbsoluteDeviation = 1
      }
      minimumImpressionCount = 1000
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

  /** Number of requests received. */
  var requestCount: Int = 0
    private set

  fun start(port: Int) {
    server = HttpServer.create(InetSocketAddress(port), 0)
    server.createContext("/") { exchange ->
      requestCount++
      exchange.requestBody.readBytes()
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
