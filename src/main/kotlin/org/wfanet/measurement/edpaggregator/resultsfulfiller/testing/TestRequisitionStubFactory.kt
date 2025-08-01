/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.resultsfulfiller.testing

import io.grpc.Channel
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.edpaggregator.resultsfulfiller.RequisitionStubFactory
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams

/** A Test [RequisitionStubFactory] that ignores params and uses a test stub. */
class TestRequisitionStubFactory(private val cmmsChannel: Channel) : RequisitionStubFactory {

  override fun buildRequisitionsStub(
    fulfillerParams: ResultsFulfillerParams
  ): RequisitionsCoroutineStub {
    return RequisitionsCoroutineStub(cmmsChannel).withPrincipalName(fulfillerParams.dataProvider)
  }
}
