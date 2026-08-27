/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import com.google.longrunning.OperationsGrpcKt
import com.google.protobuf.Empty
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.Status
import io.grpc.StatusException
import io.netty.handler.ssl.ClientAuth
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertFailsWith
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import picocli.CommandLine

/**
 * Verifies that [ServiceFlags.executor], wired into [CommonServer.fromFlags] the same way real
 * server binaries (e.g. V2alphaPublicApiServer) do, actually gets protected by
 * OverloadAwareServerInterceptor -- i.e. that the common-jvm plumbing is connected correctly on
 * this side, not just tested in isolation on the common-jvm side.
 */
@RunWith(JUnit4::class)
class ServiceFlagsCommonServerTest {
  private val startedLatch = CountDownLatch(1)
  private val releaseLatch = CountDownLatch(1)

  private val serviceFlags =
    ServiceFlags().apply { CommandLine(this).parseArgs("--grpc-thread-pool-size=1") }

  private val service =
    object :
      OperationsGrpcKt.OperationsCoroutineImplBase(serviceFlags.executor.asCoroutineDispatcher()) {
      override suspend fun cancelOperation(request: CancelOperationRequest): Empty {
        startedLatch.countDown()
        releaseLatch.await()
        return Empty.getDefaultInstance()
      }
    }

  private val server: CommonServer =
    CommonServer.fromParameters(
        verboseGrpcLogging = false,
        certs = null,
        clientAuth = ClientAuth.NONE,
        nameForLogging = "ServiceFlagsCommonServerTest",
        services = listOf(service.bindService()),
        executor = serviceFlags.executor,
      )
      .start()

  private val channel: ManagedChannel =
    ManagedChannelBuilder.forAddress("localhost", server.port).usePlaintext().build()

  @After
  fun tearDown() {
    releaseLatch.countDown()
    channel.shutdownNow()
    server.close()
  }

  @Test
  fun `serviceFlags executor saturation surfaces as RESOURCE_EXHAUSTED through CommonServer`() =
    runBlocking {
      val stub = OperationsGrpcKt.OperationsCoroutineStub(channel)

      val holderJob =
        launch(Dispatchers.IO) {
          runCatching {
            stub
              .withDeadlineAfter(60, TimeUnit.SECONDS)
              .cancelOperation(CancelOperationRequest.getDefaultInstance())
          }
        }
      assertThat(startedLatch.await(5, TimeUnit.SECONDS)).isTrue()

      val start = System.nanoTime()
      val thrown =
        assertFailsWith<StatusException> {
          withTimeout(5_000) {
            stub
              .withDeadlineAfter(5, TimeUnit.SECONDS)
              .cancelOperation(CancelOperationRequest.getDefaultInstance())
          }
        }
      val elapsedMillis = (System.nanoTime() - start) / 1_000_000

      assertThat(thrown.status.code).isEqualTo(Status.Code.RESOURCE_EXHAUSTED)
      assertThat(elapsedMillis).isLessThan(2_000)

      releaseLatch.countDown()
      holderJob.join()
    }
}
