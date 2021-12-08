// Copyright 2020 The Cross-Media Measurement Authors
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

import com.google.cloud.spanner.Value
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.set
import org.wfanet.measurement.internal.kingdom.OpenIdRequestParams
import org.wfanet.measurement.internal.kingdom.openIdRequestParams

class GenerateOpenIdRequestParams(private val validSeconds: Long) :
  SimpleSpannerWriter<OpenIdRequestParams>() {
  override suspend fun TransactionScope.runTransaction(): OpenIdRequestParams {
    val internalOpenIdRequestParamsId = idGenerator.generateInternalId()
    val externalOpenIdRequestParamsId = idGenerator.generateExternalId()
    val nonce = idGenerator.generateExternalId()

    transactionContext.bufferInsertMutation("OpenIdRequestParams") {
      set("OpenIdRequestParamsId" to internalOpenIdRequestParamsId)
      set("ExternalOpenIdRequestParamsId" to externalOpenIdRequestParamsId)
      set("Nonce" to nonce)
      set("CreateTime" to Value.COMMIT_TIMESTAMP)
      set("ValidSeconds" to validSeconds)
    }

    return openIdRequestParams {
      state = externalOpenIdRequestParamsId.value
      this.nonce = nonce.value
    }
  }
}
