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

import com.google.protobuf.InvalidProtocolBufferException
import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineImplBase
import org.wfanet.measurement.internal.duchy.GetContinuationTokenRequest
import org.wfanet.measurement.internal.duchy.GetContinuationTokenResponse
import org.wfanet.measurement.internal.duchy.SetContinuationTokenRequest
import org.wfanet.measurement.internal.duchy.SetContinuationTokenResponse
import org.wfanet.measurement.internal.duchy.getContinuationTokenResponse

class SpannerContinuationTokensService(
  private val client: AsyncDatabaseClient,
  coroutineContext: CoroutineContext,
) : ContinuationTokensCoroutineImplBase(coroutineContext) {
  override suspend fun getContinuationToken(
    request: GetContinuationTokenRequest
  ): GetContinuationTokenResponse {
    val result: ContinuationTokenReaderResult =
      ContinuationTokenReader().execute(client).singleOrNull()
        ?: return GetContinuationTokenResponse.getDefaultInstance()

    return getContinuationTokenResponse { token = result.continuationToken }
  }

  override suspend fun setContinuationToken(
    request: SetContinuationTokenRequest
  ): SetContinuationTokenResponse {
    try {
      SetContinuationToken(request.token).execute(client)
    } catch (e: InvalidContinuationTokenException) {
      failGrpc(Status.FAILED_PRECONDITION) { e.message ?: "Invalid continuation token." }
    } catch (e: InvalidProtocolBufferException) {
      failGrpc(Status.INVALID_ARGUMENT) { e.message ?: "Malformed continuation token." }
    }
    return SetContinuationTokenResponse.getDefaultInstance()
  }
}
