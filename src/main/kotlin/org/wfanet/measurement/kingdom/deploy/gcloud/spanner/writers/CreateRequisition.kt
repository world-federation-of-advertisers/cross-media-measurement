// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers

import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.kingdom.Requisition

class CreateRequisition(
  private val externalDataProviderId: ExternalId,
  private val measurementId: InternalId,
  private val measurementConsumerId: InternalId
) : SpannerWriter<Requisition, Requisition>() {

  override suspend fun TransactionScope.runTransaction(): Requisition {
    return Requisition.newBuilder().build()
  }

  override fun ResultScope<Requisition>.buildResult(): Requisition {
    return Requisition.newBuilder().build()
  }
}
