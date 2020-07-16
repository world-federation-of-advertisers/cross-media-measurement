package org.wfanet.measurement.service.internal.duchy.mill

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
import org.wfanet.measurement.internal.duchy.HandleNoisedSketchResponse
import org.wfanet.measurement.internal.duchy.ComputationControlServiceGrpcKt.ComputationControlServiceCoroutineStub
import org.wfanet.measurement.service.internal.duchy.computationcontrol.ComputationControlServiceImpl

@RunWith(JUnit4::class)
@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class MillTest {
  @get:Rule
  val grpcCleanup = GrpcCleanupRule()

  private val duchyNames = listOf("Alsace", "Bavaria", "Carinthia")
  private lateinit var mills: List<Mill>

  // TODO Use the ComputationManager to determine what work the mill needs to do.

  @Before
  fun setup() {
    val workerServiceMap = duchyNames.associateWith { setupComputationControlService() }
    mills = duchyNames.map { _ ->
      Mill(workerServiceMap, 1000)
    }
  }

  @Test
  fun `peasant polls for work 3 times`() = runBlocking {
    // TODO: Should be a real test of something.
  }

  private fun setupComputationControlService(): ComputationControlServiceCoroutineStub {
    val serverName = InProcessServerBuilder.generateName()
    val client = ComputationControlServiceCoroutineStub(
      grpcCleanup.register(
        InProcessChannelBuilder.forName(serverName).directExecutor().build()
      )
    )
    grpcCleanup.register(
      InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(
          ComputationControlServiceImpl(
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
