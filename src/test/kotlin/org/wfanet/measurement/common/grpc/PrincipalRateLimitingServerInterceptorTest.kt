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

package org.wfanet.measurement.common.grpc

import com.google.common.truth.Truth.assertThat
import com.google.longrunning.CancelOperationRequest
import com.google.longrunning.DeleteOperationRequest
import com.google.longrunning.OperationsGrpc
import com.google.longrunning.OperationsGrpcKt
import com.google.protobuf.Empty
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.ratelimit.RateLimiter

@RunWith(JUnit4::class)
class PrincipalRateLimitingServerInterceptorTest {
  private val serviceMock =
    mockService<OperationsGrpcKt.OperationsCoroutineImplBase> {
      onBlocking { cancelOperation(any()) } doReturn Empty.getDefaultInstance()
      onBlocking { deleteOperation(any()) } doReturn Empty.getDefaultInstance()
    }
  private val createRateLimiterMock = mock<(principalIdentifier: String?) -> RateLimiter>()
  private var principalIdentifier: String? = null
  private val interceptor =
    PrincipalRateLimitingServerInterceptor(
      mapOf(OperationsGrpcKt.deleteOperationMethod.fullMethodName to DELETE_OPERATION_COST),
      { principalIdentifier },
      createRateLimiterMock,
    )

  @get:Rule
  val testServer = GrpcTestServerRule { addService(serviceMock.withInterceptor(interceptor)) }

  private lateinit var stub: OperationsGrpc.OperationsBlockingStub

  @Before
  fun initStub() {
    stub = OperationsGrpc.newBlockingStub(testServer.channel)
  }

  @Test
  fun `forwards call when not rate limited`() {
    whenever(createRateLimiterMock.invoke(anyOrNull())).thenReturn(RateLimiter.Unlimited)

    stub.cancelOperation(CancelOperationRequest.getDefaultInstance())

    verifyBlocking(serviceMock, times(1)) {
      cancelOperation(CancelOperationRequest.getDefaultInstance())
    }
  }

  @Test
  fun `closes call when rate limited`() {
    whenever(createRateLimiterMock.invoke(anyOrNull())).thenReturn(RateLimiter.Blocked)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        stub.cancelOperation(CancelOperationRequest.getDefaultInstance())
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAVAILABLE)
    verifyBlocking(serviceMock, never()) { cancelOperation(any()) }
  }

  @Test
  fun `applies rate limit per principal`() {
    whenever(createRateLimiterMock.invoke("alice")).thenReturn(RateLimiter.Unlimited)
    whenever(createRateLimiterMock.invoke("bob")).thenReturn(RateLimiter.Blocked)

    principalIdentifier = "alice"
    stub.cancelOperation(CancelOperationRequest.getDefaultInstance())
    stub.cancelOperation(CancelOperationRequest.getDefaultInstance())

    principalIdentifier = "bob"
    val exception =
      assertFailsWith<StatusRuntimeException> {
        stub.cancelOperation(CancelOperationRequest.getDefaultInstance())
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAVAILABLE)
    verifyBlocking(serviceMock, times(2)) { cancelOperation(any()) }
  }

  @Test
  fun `applies cost per method`() {
    val rateLimiterSpy = spy(RateLimiter.Unlimited)
    whenever(createRateLimiterMock.invoke(anyOrNull())).thenReturn(rateLimiterSpy)

    stub.deleteOperation(DeleteOperationRequest.getDefaultInstance())

    verify(rateLimiterSpy).tryAcquire(DELETE_OPERATION_COST)
  }

  companion object {
    private const val DELETE_OPERATION_COST = 3
  }
}
