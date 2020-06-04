package org.wfanet.measurement.service.testing

import io.grpc.BindableService
import io.grpc.ManagedChannel
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.testing.GrpcCleanupRule
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement

class GrpcTestServerRule(
  private val servicesFactory: (ManagedChannel) -> List<BindableService>
) : TestRule {
  val grpcCleanupRule: GrpcCleanupRule = GrpcCleanupRule()

  private val serverName = InProcessServerBuilder.generateName()

  val channel: ManagedChannel =
    grpcCleanupRule.register(
      InProcessChannelBuilder
        .forName(serverName)
        .directExecutor()
        .build()
    )

  override fun apply(base: Statement?, description: Description?): Statement {
    val serverBuilder =
      InProcessServerBuilder.forName(serverName)
        .directExecutor()

    servicesFactory(channel).forEach { serverBuilder.addService(it) }
    grpcCleanupRule.register(serverBuilder.build().start())

    return object : Statement() {
      override fun evaluate() {
        grpcCleanupRule.apply(base, description)
      }
    }
  }
}
