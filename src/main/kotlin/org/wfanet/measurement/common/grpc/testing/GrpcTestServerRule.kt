// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.common.grpc.testing

import io.grpc.BindableService
import io.grpc.Channel
import io.grpc.ServerServiceDefinition
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.testing.GrpcCleanupRule
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.grpc.LoggingServerInterceptor

class GrpcTestServerRule(
  customServerName: String? = null,
  private val logAllRequests: Boolean = false,
  private val addServices: Builder.() -> Unit
) : TestRule {
  class Builder(val channel: Channel, private val serverBuilder: InProcessServerBuilder) {
    fun addService(service: BindableService) {
      serverBuilder.addService(service)
    }
    fun addService(service: ServerServiceDefinition) {
      serverBuilder.addService(service)
    }
  }

  private val grpcCleanupRule: GrpcCleanupRule = GrpcCleanupRule()
  private val serverName = customServerName ?: InProcessServerBuilder.generateName()

  val channel: Channel =
    grpcCleanupRule.register(
      InProcessChannelBuilder
        .forName(serverName)
        .directExecutor()
        .build()
    )

  override fun apply(base: Statement, description: Description): Statement {
    val newStatement = object : Statement() {
      override fun evaluate() {
        val serverBuilder =
          InProcessServerBuilder.forName(serverName)
            .directExecutor()

        if (logAllRequests) {
          serverBuilder.intercept(LoggingServerInterceptor())
        }

        Builder(channel, serverBuilder).addServices()
        grpcCleanupRule.register(serverBuilder.build().start())
        base.evaluate()
      }
    }

    return grpcCleanupRule.apply(newStatement, description)
  }
}
