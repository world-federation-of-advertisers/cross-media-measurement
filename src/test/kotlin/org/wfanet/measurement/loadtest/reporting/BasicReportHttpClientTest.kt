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
import java.io.InterruptedIOException
import java.net.ProtocolException
import java.net.SocketTimeoutException
import java.time.Duration
import javax.net.ssl.SSLHandshakeException
import javax.net.ssl.SSLPeerUnverifiedException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import okhttp3.Interceptor
import okhttp3.MediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okhttp3.ResponseBody
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
  fun `getBasicReport retries connection failures`() = runTest {
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
  fun `getBasicReport retries socket timeouts`() = runTest {
    val interceptor =
      ScriptedInterceptor(
        listOf(
          Result.failure(SocketTimeoutException("read timeout")),
          Result.success(response(200, "{\"state\":\"SUCCEEDED\"}")),
        )
      )
    assertThat(createClient(interceptor).getBasicReport(REQUEST).state)
      .isEqualTo(BasicReport.State.SUCCEEDED)
    assertThat(interceptor.calls).isEqualTo(2)
  }

  @Test
  fun `getBasicReport retries truncated successful response bodies`() = runTest {
    val interceptor =
      ScriptedInterceptor(
        listOf(
          Result.success(response(200, UnreadableResponseBody(IOException("truncated response")))),
          Result.success(response(200, "{\"state\":\"SUCCEEDED\"}")),
        )
      )
    assertThat(createClient(interceptor).getBasicReport(REQUEST).state)
      .isEqualTo(BasicReport.State.SUCCEEDED)
    assertThat(interceptor.calls).isEqualTo(2)
  }

  @Test
  @OptIn(ExperimentalCoroutinesApi::class)
  fun `getBasicReport shares attempt limit across HTTP and transport failures`() = runTest {
    val finalFailure = SocketTimeoutException("final timeout")
    val interceptor =
      ScriptedInterceptor(
        listOf(
          Result.failure(IOException("connection reset")),
          Result.success(response(503, "unavailable")),
          Result.failure(finalFailure),
        )
      )
    assertThat(
        assertFailsWith<SocketTimeoutException> {
          createClient(interceptor).getBasicReport(REQUEST)
        }
      )
      .isSameInstanceAs(finalFailure)
    assertThat(interceptor.calls).isEqualTo(3)
    assertThat(testScheduler.currentTime).isEqualTo(3000L)
  }

  @Test
  fun `getBasicReport rejects TLS and protocol failures without retry`() = runTest {
    for (failure in
      listOf(
        SSLHandshakeException("invalid certificate"),
        SSLPeerUnverifiedException("untrusted peer"),
        ProtocolException("invalid HTTP response"),
        InterruptedIOException("interrupted"),
      )) {
      val interceptor = ScriptedInterceptor(listOf(Result.failure(failure)))
      val actual =
        assertFailsWith<IOException> { createClient(interceptor).getBasicReport(REQUEST) }
      assertThat(actual).isInstanceOf(failure.javaClass)
      assertThat(interceptor.calls).isEqualTo(1)
    }
  }

  @Test
  fun `getBasicReport propagates cancellation without retry`() = runTest {
    val interceptor =
      ScriptedInterceptor(listOf(Result.failure(CancellationException("cancelled"))))
    assertFailsWith<CancellationException> { createClient(interceptor).getBasicReport(REQUEST) }
    assertThat(interceptor.calls).isEqualTo(1)
  }

  @Test
  @OptIn(ExperimentalCoroutinesApi::class)
  fun `getBasicReport stops after bounded transient failures`() = runTest {
    val interceptor =
      ScriptedInterceptor(
        listOf(
          Result.success(response(503, "first outage")),
          Result.success(response(503, "second outage")),
          Result.success(response(503, "final outage")),
        )
      )
    val failure = assertFailsWith<IOException> { createClient(interceptor).getBasicReport(REQUEST) }
    assertThat(failure.message).contains("503 test")
    assertThat(failure.message).contains("final outage")
    assertThat(failure.message).doesNotContain("first outage")
    assertThat(failure.message).doesNotContain("second outage")
    assertThat(interceptor.calls).isEqualTo(3)
    assertThat(testScheduler.currentTime).isEqualTo(3000L)
  }

  @Test
  fun `getBasicReport rejects permanent HTTP errors without retry`() = runTest {
    for (code in listOf(400, 401, 403, 404)) {
      val interceptor = ScriptedInterceptor(listOf(Result.success(response(code, "permanent"))))
      val failure =
        assertFailsWith<IllegalStateException> { createClient(interceptor).getBasicReport(REQUEST) }
      assertThat(failure.message).contains("$code test")
      assertThat(failure.message).contains("permanent")
      assertThat(interceptor.calls).isEqualTo(1)
    }
  }

  @Test
  fun `getBasicReport preserves permanent HTTP error when body is unreadable`() = runTest {
    val bodyReadFailure = IOException("truncated error body")
    val interceptor =
      ScriptedInterceptor(
        listOf(Result.success(response(403, UnreadableResponseBody(bodyReadFailure))))
      )

    val failure =
      assertFailsWith<IllegalStateException> { createClient(interceptor).getBasicReport(REQUEST) }

    assertThat(failure.message).contains("403 test")
    assertThat(generateSequence<Throwable>(failure) { it.cause }.last())
      .isSameInstanceAs(bodyReadFailure)
    assertThat(interceptor.calls).isEqualTo(1)
  }

  @Test
  @OptIn(ExperimentalCoroutinesApi::class)
  fun `getBasicReport preserves exhausted HTTP error when body is unreadable`() = runTest {
    val bodyReadFailure = IOException("truncated error body")
    val interceptor =
      ScriptedInterceptor(
        listOf(
          Result.success(response(503, "first outage")),
          Result.success(response(503, "second outage")),
          Result.success(response(503, UnreadableResponseBody(bodyReadFailure))),
        )
      )

    val failure = assertFailsWith<IOException> { createClient(interceptor).getBasicReport(REQUEST) }

    assertThat(failure.message).contains("503 test")
    assertThat(failure.message).doesNotContain("first outage")
    assertThat(failure.message).doesNotContain("second outage")
    assertThat(generateSequence<Throwable>(failure) { it.cause }.last())
      .isSameInstanceAs(bodyReadFailure)
    assertThat(interceptor.calls).isEqualTo(3)
    assertThat(testScheduler.currentTime).isEqualTo(3000L)
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

  private fun response(code: Int, body: String): Response = response(code, body.toResponseBody())

  private fun response(code: Int, body: ResponseBody): Response =
    Response.Builder()
      .request(REQUEST)
      .protocol(Protocol.HTTP_1_1)
      .code(code)
      .message("test")
      .body(body)
      .build()

  private class UnreadableResponseBody(private val failure: IOException) : ResponseBody() {
    override fun contentType(): MediaType? = null

    override fun contentLength(): Long = -1L

    override fun source(): Nothing = throw failure

    override fun close() {}
  }

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
