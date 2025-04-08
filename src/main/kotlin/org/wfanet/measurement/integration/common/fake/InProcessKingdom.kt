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

package org.wfanet.measurement.integration.common.fake

import io.grpc.Channel
import java.util.logging.Logger
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.chainRulesSequentially

/**
 * TestRule that starts a Data Provider-facing mock Kingdom. Currently, only mocks
 * EventGroupsService and RequisitionsService
 *
 * @requisitions A list of [Requisition]s
 * @eventGroups A list of [EventGroup]s
 */
class InProcessMockKindom(
  private val requisitions: List<Requisition>,
  private val eventGroups: List<EventGroup>,
) : TestRule {

  private val publicApiServer =
    GrpcTestServerRule() {
      logger.info("Building Mock Kingdom's public API services")

      listOf(EventGroupsService(eventGroups), RequisitionsService(requisitions)).forEach {
        addService(it)
      }
    }

  /** Provides a gRPC channel to the Kingdom's public API. */
  val publicApiChannel: Channel
    get() = publicApiServer.channel

  override fun apply(statement: Statement, description: Description): Statement {
    return chainRulesSequentially(publicApiServer).apply(statement, description)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
