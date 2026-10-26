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

package org.wfanet.measurement.loadtest.reporting

import com.google.common.truth.Truth.assertThat
import java.io.IOException
import java.time.Duration
import kotlin.test.assertFailsWith
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
import org.junit.Test
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.reporting.v2alpha.BasicReport

class BasicReportHttpClientTest {
  @Test
  fun `getBasicReport retries transient gateway errors`() = runTest {
    val interceptor =
      ScriptedInterceptor(
        listOf(
          Result.success(response(500, "temporary outage")),
          Result.success(response(200, "{\"state\":\"SUCCEEDED\"}")),
        )
      )

    val report = createClient(interceptor).getBasicReport(REQUEST)

    assertThat(report.state).isEqualTo(BasicReport.State.SUCCEEDED)
    assertThat(interceptor.calls).isEqualTo(2)
  }

  @Test
  fun `getBasicReport retries each transient HTTP status`() = runTest {
    for (code in listOf(429, 500, 502, 503, 504)) {
      val interceptor =
        ScriptedInterceptor(
          listOf(
            Result.success(response(code, "temporary outage")),
            Result.success(response(200, "{\"state\":\"SUCCEEDED\"}")),
          )
        )
      assertThat(createClient(interceptor).getBasicReport(REQUEST).state)
        .isEqualTo(BasicReport.State.SUCCEEDED)
      assertThat(interceptor.calls).isEqualTo(2)
    }
  }

  @Test
  fun `getBasicReport retries connection failure`() = runTest {
    val interceptor =
      ScriptedInterceptor(
        listOf(
          Result.failure(IOException("connection reset")),
          Result.success(response(200, "{\"state\":\"SUCCEEDED\"}")),
        )
      )
    assertThat(createClient(interceptor).getBasicReport(REQUEST).state)
      .isEqualTo(BasicReport.State.SUCCEEDED)
    assertThat(interceptor.calls).isEqualTo(2)
  }

  @Test
  @OptIn(ExperimentalCoroutinesApi::class)
  fun `getBasicReport stops after bounded transient failures`() = runTest {
    val interceptor = ScriptedInterceptor(List(3) { Result.success(response(503, "unavailable")) })
    assertFailsWith<IOException> { createClient(interceptor).getBasicReport(REQUEST) }
    assertThat(interceptor.calls).isEqualTo(3)
    assertThat(testScheduler.currentTime).isEqualTo(3000L)
  }

  @Test
  fun `getBasicReport rejects permanent HTTP errors without retry`() = runTest {
    for (code in listOf(400, 401, 403, 404)) {
      val interceptor = ScriptedInterceptor(listOf(Result.success(response(code, "permanent"))))
      assertFailsWith<IllegalStateException> { createClient(interceptor).getBasicReport(REQUEST) }
      assertThat(interceptor.calls).isEqualTo(1)
    }
  }

  @Test
  fun `getBasicReport rejects malformed successful response without retry`() = runTest {
    val interceptor = ScriptedInterceptor(listOf(Result.success(response(200, "not json"))))
    assertFailsWith<com.google.protobuf.InvalidProtocolBufferException> {
      createClient(interceptor).getBasicReport(REQUEST)
    }
    assertThat(interceptor.calls).isEqualTo(1)
  }

  @Test
  fun `getBasicReport returns terminal failed report without retry`() = runTest {
    val interceptor =
      ScriptedInterceptor(listOf(Result.success(response(200, "{\"state\":\"FAILED\"}"))))
    assertThat(createClient(interceptor).getBasicReport(REQUEST).state)
      .isEqualTo(BasicReport.State.FAILED)
    assertThat(interceptor.calls).isEqualTo(1)
  }

  @Test
  fun `getBasicReport rejects creation requests`() = runTest {
    val interceptor = ScriptedInterceptor(emptyList())
    val request = REQUEST.newBuilder().post("{}".toRequestBody()).build()
    assertFailsWith<IllegalArgumentException> { createClient(interceptor).getBasicReport(request) }
    assertThat(interceptor.calls).isEqualTo(0)
  }

  private fun createClient(interceptor: Interceptor): BasicReportHttpClient =
    BasicReportHttpClient(
      httpClient = OkHttpClient.Builder().addInterceptor(interceptor).build(),
      retryBackoff =
        ExponentialBackoff(
          initialDelay = Duration.ofSeconds(1),
          multiplier = 2.0,
          randomnessFactor = 0.0,
        ),
      maximumRetryDelay = Duration.ofSeconds(2),
      maxAttempts = 3,
    )

  private fun response(code: Int, body: String): Response =
    Response.Builder()
      .request(REQUEST)
      .protocol(Protocol.HTTP_1_1)
      .code(code)
      .message("test")
      .body(body.toResponseBody())
      .build()

  private class ScriptedInterceptor(private val responses: List<Result<Response>>) : Interceptor {
    var calls = 0
      private set

    override fun intercept(chain: Interceptor.Chain): Response = responses[calls++].getOrThrow()
  }

  companion object {
    private val REQUEST =
      Request.Builder()
        .url("https://example.test/v2alpha/measurementConsumers/test/basicReports/test")
        .build()
  }
}
