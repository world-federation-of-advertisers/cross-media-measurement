// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.common

import io.grpc.Channel
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.Services
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.InternalApiServices

/** TestRule that starts and stops all Control Plane services. */
class InProcessSecureComputationPublicApi(
  internalServicesProvider: () -> InternalApiServices,
  val verboseGrpcLogging: Boolean = true,
  private val serviceContext: CoroutineContext = EmptyCoroutineContext,
) : TestRule {

  private val internalServices: InternalApiServices by lazy { internalServicesProvider() }

  private val internalApiServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      logger.info("Building Control Plane's internal API services")
      internalServices.build(serviceContext).toList().forEach {
        logger.info("Adding service $it")
        addService(it)
      }
    }

  private val publicApiServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      logger.info("Building Control Plane's public API services")
      Services.build(internalApiChannel, serviceContext).toList().forEach {
        logger.info("Adding service $it")
        addService(it)
      }
    }

  /** Provides a gRPC channel to the Control Plane's public API. */
  val publicApiChannel: Channel
    get() = publicApiServer.channel

  /** Provides a gRPC channel to the Control Plane's internal API. */
  private val internalApiChannel: Channel by lazy { internalApiServer.channel }

  override fun apply(statement: Statement, description: Description): Statement {
    return chainRulesSequentially(internalApiServer, publicApiServer).apply(statement, description)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
