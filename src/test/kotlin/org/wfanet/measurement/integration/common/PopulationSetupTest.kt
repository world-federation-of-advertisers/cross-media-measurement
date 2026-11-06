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

package org.wfanet.measurement.integration.common

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.util.Durations
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ConnectivityState
import io.grpc.ForwardingClientCall
import io.grpc.ManagedChannel
import io.grpc.Metadata
import io.grpc.MethodDescriptor
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.serviceconfig.copy
import io.grpc.stub.StreamObserver
import io.grpc.testing.GrpcCleanupRule
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import kotlin.test.assertFailsWith
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.CreatePopulationRequest
import org.wfanet.measurement.api.v2alpha.Population
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.PopulationsGrpc
import org.wfanet.measurement.api.v2alpha.population
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule

class PopulationSetupTest {
  private val requests = mutableListOf<CreatePopulationRequest>()
  private var failuresRemaining = 0
  private var failureStatus = Status.UNAVAILABLE

  private val serverName = InProcessServerBuilder.generateName()
  private val serviceConfig =
    PopulationSetup.SERVICE_CONFIG.copy {
      val methods = methodConfig.toList()
      methodConfig.clear()
      methodConfig +=
        methods.map { method ->
          method.copy {
            retryPolicy =
              method.retryPolicy.copy {
                initialBackoff = Durations.fromNanos(1)
                maxBackoff = Durations.fromNanos(1)
              }
          }
        }
    }

  @get:Rule val cleanup = GrpcCleanupRule()

  // In-process channels disable retries by default; explicitly exercise the shared policy.
  private val channel: ManagedChannel =
    cleanup.register(
      InProcessChannelBuilder.forName(serverName)
        .directExecutor()
        .enableRetry()
        .defaultServiceConfig(serviceConfig.asMap())
        .build()
    )

  @get:Rule
  val server =
    GrpcTestServerRule(customServerName = serverName) {
      addService(
        object : PopulationsGrpc.PopulationsImplBase() {
          override fun createPopulation(
            request: CreatePopulationRequest,
            responseObserver: StreamObserver<Population>,
          ) {
            requests += request
            if (failuresRemaining-- > 0) {
              responseObserver.onError(failureStatus.asRuntimeException())
            } else {
              responseObserver.onNext(population { name = "$PARENT/populations/population" })
              responseObserver.onCompleted()
            }
          }
        }
      )
    }

  @Test
  fun `SERVICE_CONFIG changes only CreatePopulation`() {
    val methodConfigs = PopulationSetup.SERVICE_CONFIG.message.methodConfigList
    assertThat(methodConfigs).hasSize(2)
    val defaultConfig =
      methodConfigs.single { config ->
        config.nameList.single().let { it.service.isEmpty() && it.method.isEmpty() }
      }
    val createPopulationConfig =
      methodConfigs.single { config ->
        config.nameList.single().let {
          it.service == PopulationsGrpc.SERVICE_NAME && it.method == "CreatePopulation"
        }
      }

    assertThat(defaultConfig.timeout).isEqualTo(Durations.fromSeconds(30))
    assertThat(defaultConfig.waitForReady.value).isFalse()
    assertThat(createPopulationConfig.timeout).isEqualTo(Durations.fromSeconds(120))
    assertThat(createPopulationConfig.waitForReady.value).isTrue()
    assertThat(createPopulationConfig.retryPolicy).isEqualTo(defaultConfig.retryPolicy)
  }

  @Test
  fun `ensurePopulation reuses the request ID for the same population`() {
    val spec = PopulationSpec.getDefaultInstance()
    val first = PopulationSetup.ensurePopulation(channel, PARENT, spec, description = "")
    val second = PopulationSetup.ensurePopulation(channel, PARENT, spec, description = "")
    assertThat(first).isEqualTo(second)
    assertThat(requests[0].requestId).isNotEmpty()
    assertThat(requests[1].requestId).isEqualTo(requests[0].requestId)
  }

  @Test
  fun `ensurePopulation uses a different request ID when the spec changes`() {
    PopulationSetup.ensurePopulation(
      channel,
      PARENT,
      PopulationSpec.getDefaultInstance(),
      description = "",
    )
    PopulationSetup.ensurePopulation(
      channel,
      PARENT,
      populationSpec {
        subpopulations +=
          PopulationSpecKt.subPopulation {
            vidRanges +=
              PopulationSpecKt.vidRange {
                startVid = 1
                endVidInclusive = 10
              }
          }
      },
      description = "",
    )
    assertThat(requests[1].requestId).isNotEqualTo(requests[0].requestId)
  }

  @Test(timeout = 10_000)
  fun `ensurePopulation waits for its connection to recover`() {
    val recoveringServerName = InProcessServerBuilder.generateName()
    val requestStarted = CountDownLatch(1)
    val recoveringChannel =
      cleanup.register(
        InProcessChannelBuilder.forName(recoveringServerName)
          .directExecutor()
          // Isolate connection readiness from application-level retries.
          .disableRetry()
          .defaultServiceConfig(serviceConfig.asMap())
          .intercept(
            object : ClientInterceptor {
              override fun <ReqT : Any, RespT : Any> interceptCall(
                method: MethodDescriptor<ReqT, RespT>,
                callOptions: CallOptions,
                next: Channel,
              ): ClientCall<ReqT, RespT> {
                return object :
                  ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                    next.newCall(method, callOptions)
                  ) {
                  override fun start(listener: Listener<RespT>, headers: Metadata) {
                    super.start(listener, headers)
                    requestStarted.countDown()
                  }
                }
              }
            }
          )
          .build()
      )
    val unavailable = CompletableFuture<Unit>()
    fun observeConnection() {
      val state = recoveringChannel.getState(true)
      if (state == ConnectivityState.TRANSIENT_FAILURE) {
        unavailable.complete(Unit)
      } else {
        recoveringChannel.notifyWhenStateChanged(state) { observeConnection() }
      }
    }
    observeConnection()
    unavailable.get()

    val executor = Executors.newSingleThreadExecutor()
    try {
      val result =
        executor.submit<Population> {
          PopulationSetup.ensurePopulation(
            recoveringChannel,
            PARENT,
            PopulationSpec.getDefaultInstance(),
            description = "",
          )
        }
      requestStarted.await()
      cleanup.register(
        InProcessServerBuilder.forName(recoveringServerName)
          .directExecutor()
          .addService(
            object : PopulationsGrpc.PopulationsImplBase() {
              override fun createPopulation(
                request: CreatePopulationRequest,
                responseObserver: StreamObserver<Population>,
              ) {
                responseObserver.onNext(population { name = "$PARENT/populations/recovered" })
                responseObserver.onCompleted()
              }
            }
          )
          .build()
          .start()
      )
      recoveringChannel.resetConnectBackoff()
      assertThat(result.get().name).isEqualTo("$PARENT/populations/recovered")
    } finally {
      recoveringChannel.shutdownNow()
      executor.shutdownNow()
    }
  }

  @Test
  fun `ensurePopulation retries UNAVAILABLE with the same request`() {
    failuresRemaining = 1
    val result =
      PopulationSetup.ensurePopulation(
        channel,
        PARENT,
        PopulationSpec.getDefaultInstance(),
        description = "",
      )
    assertThat(result.name).isEqualTo("$PARENT/populations/population")
    assertThat(requests).hasSize(2)
    assertThat(requests[1]).isEqualTo(requests[0])
  }

  @Test
  fun `ensurePopulation preserves a permanent error with provider context`() {
    failuresRemaining = 1
    failureStatus = Status.PERMISSION_DENIED
    val error =
      assertFailsWith<IllegalStateException> {
        PopulationSetup.ensurePopulation(
          channel,
          PARENT,
          PopulationSpec.getDefaultInstance(),
          description = "",
        )
      }
    assertThat(error.message).contains(PARENT)
    assertThat((error.cause as StatusRuntimeException).status.code)
      .isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(requests).hasSize(1)
  }

  companion object {
    private const val PARENT = "dataProviders/provider"
  }
}
