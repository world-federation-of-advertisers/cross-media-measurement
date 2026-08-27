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
import com.google.longrunning.cancelOperationRequest
import com.google.protobuf.Empty
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.Status
import io.grpc.StatusException
import io.netty.handler.ssl.ClientAuth
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit
import kotlin.coroutines.resume
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.Instrumentation
import picocli.CommandLine

/**
 * Verifies that [ServiceFlags.executor], wired into [CommonServer.fromFlags] the same way real
 * server binaries (e.g. V2alphaPublicApiServer) do, actually gets protected by
 * OverloadAwareServerInterceptor -- i.e. that the common-jvm plumbing is connected correctly on
 * this side, not just tested in isolation on the common-jvm side.
 *
 * ServiceFlags.executor is backed by an unbounded queue, so a saturated pool just queues work
 * rather than rejecting it -- the only rejection it can produce is if it's already been shut down.
 * As of this writing, nothing in this codebase actually shuts ServiceFlags.executor down during
 * normal server operation, so this protection is not yet load-bearing on any real path; these tests
 * exist so the wiring is verified and ready if/when that changes.
 */
@RunWith(JUnit4::class)
class ServiceFlagsCommonServerTest {
  private val startedLatch = CountDownLatch(1)
  private val releaseLatch = CountDownLatch(1)
  private var pendingResume: CancellableContinuation<Unit>? = null

  private val serviceFlags =
    ServiceFlags().apply { CommandLine(this).parseArgs("--grpc-thread-pool-size=1") }

  private val service =
    object :
      OperationsGrpcKt.OperationsCoroutineImplBase(serviceFlags.executor.asCoroutineDispatcher()) {
      override suspend fun cancelOperation(request: CancelOperationRequest): Empty {
        when (request.name) {
          "in-flight" ->
            // The continuation is captured, and the latch only counted down, once this
            // suspension is genuinely committed -- unlike a CompletableDeferred the test
            // completes, there's no window where resuming races ahead of the suspend actually
            // taking effect and turns into a same-thread no-op instead of a real redispatch.
            suspendCancellableCoroutine<Unit> { cont ->
              pendingResume = cont
              startedLatch.countDown()
            }
          else -> {
            startedLatch.countDown()
            releaseLatch.await()
          }
        }
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
    Instrumentation.resetForTest()
  }

  @Test
  fun `executor shut down before a call starts surfaces as UNAVAILABLE`() = runBlocking {
    releaseLatch.countDown()
    (serviceFlags.executor as ExecutorService).shutdown()

    val stub = OperationsGrpcKt.OperationsCoroutineStub(channel)
    // A hang would blow through withTimeout and throw TimeoutCancellationException instead of
    // StatusException, failing this assertion -- so a pass already implies a prompt rejection.
    val thrown =
      assertFailsWith<StatusException> {
        withTimeout(10_000) {
          stub
            .withDeadlineAfter(30, TimeUnit.SECONDS)
            .cancelOperation(CancelOperationRequest.getDefaultInstance())
        }
      }

    assertThat(thrown.status.code).isEqualTo(Status.Code.UNAVAILABLE)
  }

  @Test
  fun `executor shut down while a call is already in flight surfaces as INTERNAL`() = runBlocking {
    releaseLatch.countDown()
    val stub = OperationsGrpcKt.OperationsCoroutineStub(channel)

    supervisorScope {
      val callDeferred =
        async(Dispatchers.IO) {
          stub
            .withDeadlineAfter(30, TimeUnit.SECONDS)
            .cancelOperation(cancelOperationRequest { name = "in-flight" })
        }
      assertThat(startedLatch.await(5, TimeUnit.SECONDS)).isTrue()

      // The call has already started and is genuinely suspended -- shutting down the executor now
      // and only then resuming means the rejection lands on a redispatch after the handler
      // already ran some code, not on the call's initial dispatch.
      (serviceFlags.executor as ExecutorService).shutdown()
      pendingResume!!.resume(Unit)

      val thrown = assertFailsWith<StatusException> { withTimeout(10_000) { callDeferred.await() } }
      assertThat(thrown.status.code).isEqualTo(Status.Code.INTERNAL)
    }
  }

  @Test
  fun `unsaturated serviceFlags executor still serves calls normally through CommonServer`() =
    runBlocking {
      releaseLatch.countDown()
      val stub = OperationsGrpcKt.OperationsCoroutineStub(channel)
      val response = stub.cancelOperation(CancelOperationRequest.getDefaultInstance())
      assertThat(response).isEqualTo(Empty.getDefaultInstance())
    }
}
