package org.wfanet.measurement.service.internal.duchy.worker

import com.google.common.truth.Truth
import com.google.common.truth.extensions.proto.ProtoTruth
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.testing.GrpcCleanupRule
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.client.internal.duchy.worker.WorkerClient
import org.wfanet.measurement.internal.duchy.TraceResponse
import org.wfanet.measurement.internal.duchy.WorkerServiceGrpcKt
import kotlin.test.assertFailsWith

@RunWith(JUnit4::class)
class WorkerServiceImplTest {
  @get:Rule
  val grpcCleanup = GrpcCleanupRule()

  lateinit var clients: List<WorkerClient>

  val namesForLogging = listOf("Alsace", "Bavaria", "Carinthia")

  @Before
  fun setup() {
    val names = namesForLogging.map {
      Name(InProcessServerBuilder.generateName(), it)
    }
    clients = names.map { (serverName, _) ->
      val channel = grpcCleanup.register(
        InProcessChannelBuilder.forName(serverName).directExecutor().build()
      )
      val stub = WorkerServiceGrpcKt.WorkerServiceCoroutineStub(channel)
      WorkerClient(channel, stub)
    }
    (names zip clients.slice(IntRange(1, namesForLogging.size - 1) + IntRange(0, 0)))
      .forEach { (name, client) ->
        grpcCleanup.register(
          InProcessServerBuilder.forName(name.serverName)
            .directExecutor()
            .addService(WorkerServiceImpl(client, name.nameForLogging))
            .build()
            .start()
        )
      }
  }

  @Test
  fun `trace length 1`() {
    val expected = TraceResponse.newBuilder()
      .addHop(
        TraceResponse.Hop.newBuilder()
          .setName(namesForLogging.first())
          .setCountdown(0)
          .build()
      )
      .build()

    val response = runBlocking { clients[0].trace(0) }

    ProtoTruth.assertThat(response)
      .isEqualTo(
        expected
      )
  }

  @Test
  fun `trace length 6`() {
    val expected = TraceResponse.newBuilder()
      .addAllHop(((namesForLogging + namesForLogging) zip (5 downTo 0))
        .map { (nameForLogging, countdown) ->
          TraceResponse.Hop.newBuilder().setName(nameForLogging).setCountdown(countdown).build()
        })
      .build()

    val response = runBlocking { clients[0].trace(5) }

    ProtoTruth.assertThat(response)
      .isEqualTo(
        expected
      )
  }

  @Test
  fun `trace length 7 throws`() {
    val e = assertFailsWith(StatusException::class) {
      runBlocking { clients[0].trace(6) }
    }

    Truth.assertThat(e.status.code).isEqualTo(Status.INVALID_ARGUMENT.code)
  }
}

data class Name(val serverName: String, val nameForLogging: String)
