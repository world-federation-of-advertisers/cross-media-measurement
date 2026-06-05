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

package org.wfanet.measurement.reporting.mcp.grpc

import com.google.common.truth.Truth.assertThat
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.GetEventGroupRequest

private val AUTH_KEY: Metadata.Key<String> =
  Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)

@RunWith(JUnit4::class)
class BearerPassthroughCallCredentialsTest {

  @Test
  fun callCredentialsAttachBearerTokenToGrpcCall() {
    var capturedAuthHeader: String? = null

    val interceptor =
      object : ServerInterceptor {
        override fun <ReqT, RespT> interceptCall(
          call: ServerCall<ReqT, RespT>,
          headers: Metadata,
          next: ServerCallHandler<ReqT, RespT>,
        ): ServerCall.Listener<ReqT> {
          capturedAuthHeader = headers.get(AUTH_KEY)
          return next.startCall(call, headers)
        }
      }

    val serverName = InProcessServerBuilder.generateName()
    val server =
      InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .intercept(interceptor)
        .addService(
          object : EventGroupsGrpcKt.EventGroupsCoroutineImplBase() {
            override suspend fun getEventGroup(request: GetEventGroupRequest): EventGroup =
              EventGroup.getDefaultInstance()
          }
        )
        .build()
        .start()

    try {
      val channel = InProcessChannelBuilder.forName(serverName).directExecutor().build()
      val credentials = BearerPassthroughCallCredentials("my-secret-token")
      val stub =
        EventGroupsGrpcKt.EventGroupsCoroutineStub(channel).withCallCredentials(credentials)

      runBlocking { stub.getEventGroup(GetEventGroupRequest.getDefaultInstance()) }

      assertThat(capturedAuthHeader).isEqualTo("Bearer my-secret-token")
    } finally {
      server.shutdownNow()
    }
  }
}
