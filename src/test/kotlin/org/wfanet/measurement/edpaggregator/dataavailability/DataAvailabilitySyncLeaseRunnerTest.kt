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

package org.wfanet.measurement.edpaggregator.dataavailability

import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLease
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.dataAvailabilitySyncLease

@RunWith(JUnit4::class)
class DataAvailabilitySyncLeaseRunnerTest {
  @Test
  fun `run holds lease until synchronization completes`() = runTest {
    val client = FakeLeaseClient()
    val entered = CompletableDeferred<Unit>()
    val complete = CompletableDeferred<Unit>()
    val runner = runner(client, renewalIntervalSeconds = 3_600)

    val job = launch {
      runner.run(DATA_PROVIDER) { _ ->
        entered.complete(Unit)
        complete.await()
      }
    }
    entered.await()

    assertThat(client.active).isTrue()
    assertThat(client.canEvict).isFalse()
    complete.complete(Unit)
    job.join()
    assertThat(client.active).isFalse()
    assertThat(client.canEvict).isTrue()
    assertThat(client.events).containsExactly("acquire", "release").inOrder()
  }

  @Test
  @OptIn(ExperimentalCoroutinesApi::class)
  fun `run renews until synchronization completes`() = runTest {
    val client = FakeLeaseClient()
    val entered = CompletableDeferred<Unit>()
    val complete = CompletableDeferred<Unit>()
    val runner = runner(client, renewalIntervalSeconds = 1)

    val job = launch {
      runner.run(DATA_PROVIDER) { _ ->
        entered.complete(Unit)
        complete.await()
      }
    }
    entered.await()
    advanceTimeBy(1.seconds)
    runCurrent()
    assertThat(client.renewCount).isEqualTo(1)

    complete.complete(Unit)
    job.join()

    assertThat(client.releasedEtag).isEqualTo("etag-1")
  }

  @Test
  fun `run releases lease when synchronization fails`() = runTest {
    val client = FakeLeaseClient()
    val runner = runner(client, renewalIntervalSeconds = 3_600)

    val error =
      assertFailsWith<IllegalStateException> {
        runner.run(DATA_PROVIDER) { _ -> error("sync failed") }
      }

    assertThat(error).hasMessageThat().contains("sync failed")
    assertThat(client.active).isFalse()
    assertThat(client.events).containsExactly("acquire", "release").inOrder()
  }

  @Test
  fun `run does not synchronize when lease acquisition is blocked`() = runTest {
    val client = FakeLeaseClient(acquireFailure = Status.UNAVAILABLE.asRuntimeException())
    val runner = runner(client, renewalIntervalSeconds = 3_600)
    var synchronized = false

    val error =
      assertFailsWith<StatusRuntimeException> {
        runner.run(DATA_PROVIDER) { _ -> synchronized = true }
      }

    assertThat(error.status.code).isEqualTo(Status.Code.UNAVAILABLE)
    assertThat(synchronized).isFalse()
    assertThat(client.events).containsExactly("acquire")
  }

  @Test
  @OptIn(ExperimentalCoroutinesApi::class)
  fun `run releases lease when renewal fails`() = runTest {
    val client =
      FakeLeaseClient(
        renewFailure = Status.UNAVAILABLE.withDescription("renew failed").asRuntimeException()
      )
    val runner = runner(client, renewalIntervalSeconds = 1)
    supervisorScope {
      val result = async { runner.run(DATA_PROVIDER) { kotlinx.coroutines.awaitCancellation() } }

      advanceTimeBy(1.seconds)
      runCurrent()

      val error = assertFailsWith<StatusRuntimeException> { result.await() }
      assertThat(error.status.code).isEqualTo(Status.Code.UNAVAILABLE)
    }
    assertThat(client.active).isFalse()
    assertThat(client.events).containsExactly("acquire", "renew", "release").inOrder()
  }

  @Test
  @OptIn(ExperimentalCoroutinesApi::class)
  fun `run completes in-flight renewal before release`() = runTest {
    val client = FakeLeaseClient(blockRenewal = true)
    val synchronize = CompletableDeferred<Unit>()
    val runner = runner(client, renewalIntervalSeconds = 1)
    val result = async { runner.run(DATA_PROVIDER) { synchronize.await() } }

    advanceTimeBy(1.seconds)
    runCurrent()
    client.renewalStarted.await()
    synchronize.complete(Unit)
    runCurrent()
    assertThat(result.isActive).isTrue()

    client.finishRenewal.complete(Unit)
    result.await()

    assertThat(client.releasedEtag).isEqualTo("etag-1")
    assertThat(client.events).containsExactly("acquire", "renew", "release").inOrder()
  }

  private fun runner(
    client: DataAvailabilitySyncLeaseClient,
    renewalIntervalSeconds: Int,
  ): DataAvailabilitySyncLeaseRunner {
    val ids =
      generateSequence(1) { it + 1 }
        .map { "00000000-0000-4000-8000-${it.toString().padStart(12, '0')}" }
        .iterator()
    return DataAvailabilitySyncLeaseRunner(
      client,
      renewalIntervalSeconds.seconds,
      uuidGenerator = ids::next,
    )
  }

  private class FakeLeaseClient(
    private val acquireFailure: StatusRuntimeException? = null,
    private val renewFailure: StatusRuntimeException? = null,
    private val blockRenewal: Boolean = false,
  ) : DataAvailabilitySyncLeaseClient {
    val events = mutableListOf<String>()
    var active = false
      private set

    var renewCount = 0
      private set

    var releasedEtag = ""
      private set

    val renewalStarted = CompletableDeferred<Unit>()
    val finishRenewal = CompletableDeferred<Unit>()

    val canEvict: Boolean
      get() = !active

    override suspend fun acquire(
      parent: String,
      attemptId: String,
      requestId: String,
    ): DataAvailabilitySyncLease {
      events += "acquire"
      acquireFailure?.let { throw it }
      active = true
      return dataAvailabilitySyncLease {
        name = "$parent/dataAvailabilitySyncLeases/$attemptId"
        state = DataAvailabilitySyncLease.State.ACTIVE
        etag = "etag-0"
      }
    }

    override suspend fun renew(
      lease: DataAvailabilitySyncLease,
      requestId: String,
    ): DataAvailabilitySyncLease {
      events += "renew"
      renewFailure?.let { throw it }
      if (blockRenewal) {
        renewalStarted.complete(Unit)
        finishRenewal.await()
      }
      renewCount++
      return lease.copy { etag = "etag-$renewCount" }
    }

    override suspend fun validate(lease: DataAvailabilitySyncLease): DataAvailabilitySyncLease {
      events += "validate"
      return lease
    }

    override suspend fun release(
      lease: DataAvailabilitySyncLease,
      requestId: String,
    ): DataAvailabilitySyncLease {
      events += "release"
      active = false
      releasedEtag = lease.etag
      return lease.copy { state = DataAvailabilitySyncLease.State.RELEASED }
    }
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/data-provider"
  }
}
