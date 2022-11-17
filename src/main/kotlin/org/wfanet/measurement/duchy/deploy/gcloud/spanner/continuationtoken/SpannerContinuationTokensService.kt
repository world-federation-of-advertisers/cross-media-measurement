// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner.continuationtoken

import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineImplBase
import org.wfanet.measurement.internal.duchy.GetContinuationTokenRequest
import org.wfanet.measurement.internal.duchy.GetContinuationTokenResponse
import org.wfanet.measurement.internal.duchy.UpdateContinuationTokenRequest
import org.wfanet.measurement.internal.duchy.UpdateContinuationTokenResponse
import org.wfanet.measurement.internal.duchy.getContinuationTokenResponse

private const val EMPTY_STRING = ""

class SpannerContinuationTokensService(private val client: AsyncDatabaseClient) :
  ContinuationTokensCoroutineImplBase() {
  override suspend fun getContinuationToken(
    request: GetContinuationTokenRequest
  ): GetContinuationTokenResponse {
    val continuationToken =
      ContinuationTokenReader().execute(client).singleOrNull()?.continuationToken ?: EMPTY_STRING
    return getContinuationTokenResponse { token = continuationToken }
  }

  override suspend fun updateContinuationToken(
    request: UpdateContinuationTokenRequest
  ): UpdateContinuationTokenResponse {
    UpdateContinuationToken(request.token).execute(client)
    return UpdateContinuationTokenResponse.getDefaultInstance()
  }
}
