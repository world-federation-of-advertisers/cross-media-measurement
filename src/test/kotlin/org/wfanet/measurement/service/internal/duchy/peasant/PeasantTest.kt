package org.wfanet.measurement.service.internal.duchy.peasant

import com.google.common.truth.extensions.proto.ProtoTruth
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.testing.GrpcCleanupRule
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.db.duchy.SketchAggregationComputationManager
import org.wfanet.measurement.db.duchy.SketchAggregationStageDetails
import org.wfanet.measurement.db.duchy.SketchAggregationStages
import org.wfanet.measurement.db.duchy.testing.FakeComputationStorage
import org.wfanet.measurement.db.duchy.testing.FakeComputationsBlobDb
import org.wfanet.measurement.db.duchy.testing.FakeComputationsRelationalDatabase
import org.wfanet.measurement.internal.duchy.TransmitNoisedSketchResponse
import org.wfanet.measurement.internal.duchy.WorkerServiceGrpcKt.WorkerServiceCoroutineStub
import org.wfanet.measurement.service.internal.duchy.worker.WorkerServiceImpl

@RunWith(JUnit4::class)
@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class PeasantTest {
  @get:Rule
  val grpcCleanup = GrpcCleanupRule()

  private val duchyNames = listOf("Alsace", "Bavaria", "Carinthia")
  private lateinit var peasants: List<Peasant>

  // TODO Use the ComputationManager to determine what work the peasant needs to do.

  @Before
  fun setup() {
    val workerServiceMap = duchyNames.associateWith { setupWorkerService() }
    peasants = duchyNames.map { _ ->
      Peasant(workerServiceMap, 1000)
    }
  }

  @Test
  fun `peasant polls for work 3 times`() = runBlocking {
    val expected = List(3) {
      TransmitNoisedSketchResponse.getDefaultInstance()
    }

    val responses = peasants.first()
      .pollForWork()
      .take(3).toList()

    ProtoTruth.assertThat(responses).containsExactlyElementsIn(expected).inOrder()
  }

  private fun setupWorkerService(): WorkerServiceCoroutineStub {
    val serverName = InProcessServerBuilder.generateName()
    val client = WorkerServiceCoroutineStub(
      grpcCleanup.register(
        InProcessChannelBuilder.forName(serverName).directExecutor().build()
      )
    )
    grpcCleanup.register(
      InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(
          WorkerServiceImpl(
            SketchAggregationComputationManager(
              FakeComputationsRelationalDatabase(
                FakeComputationStorage(),
                SketchAggregationStages,
                SketchAggregationStageDetails(duchyNames.subList(1, duchyNames.size))
              ),
              FakeComputationsBlobDb(mutableMapOf()),
              duchyNames.size
            )
          )
        )
        .build()
        .start()
    )
    return client
  }
}
