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
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub as InternalWorkItemAttemptsCoroutineStub
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemsGrpcKt.WorkItemsCoroutineStub as InternalWorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsService
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsService
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.InternalApiServices
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.WorkItemPublisher

/** TestRule that starts and stops all Control Plane gRPC services. */
class InProcessSecureComputationControlPlane(
  workItemPublisher: WorkItemPublisher,
  databaseClient: AsyncDatabaseClient,
  queueMapping: QueueMapping,
  val verboseGrpcLogging: Boolean = true,
) : TestRule {
  private val internalServices =
    InternalApiServices.build(workItemPublisher, databaseClient, queueMapping)

  private val internalApiServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      logger.info("Building Control Plane's internal API services")
      internalServices.toList().forEach { addService(it) }
    }
  private val publicApiServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      logger.info("Building Control Plane's public API services")

      listOf(
          WorkItemsService(internalWorkItemsClient),
          WorkItemAttemptsService(internalWorkItemAttemptsClient),
        )
        .forEach { addService(it) }
    }

  private val internalWorkItemsClient = InternalWorkItemsCoroutineStub(internalApiChannel)
  private val internalWorkItemAttemptsClient =
    InternalWorkItemAttemptsCoroutineStub(internalApiChannel)

  /** Provides a gRPC channel to the Control Plane's public API. */
  val publicApiChannel: Channel
    get() = publicApiServer.channel

  /** Provides a gRPC channel to the Control Plane's internal API. */
  private val internalApiChannel: Channel
    get() = internalApiServer.channel

  override fun apply(statement: Statement, description: Description): Statement {
    return chainRulesSequentially(internalApiServer, publicApiServer).apply(statement, description)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
