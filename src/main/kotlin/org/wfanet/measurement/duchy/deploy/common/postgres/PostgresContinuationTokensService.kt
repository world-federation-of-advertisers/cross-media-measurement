// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.common.postgres

import io.grpc.Status
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.duchy.deploy.common.postgres.readers.ContinuationTokenReader
import org.wfanet.measurement.duchy.deploy.common.postgres.writers.SetContinuationToken
import org.wfanet.measurement.duchy.service.internal.ContinuationTokenInvalidException
import org.wfanet.measurement.duchy.service.internal.ContinuationTokenMalformedException
import org.wfanet.measurement.internal.duchy.ContinuationTokensGrpcKt.ContinuationTokensCoroutineImplBase
import org.wfanet.measurement.internal.duchy.GetContinuationTokenRequest
import org.wfanet.measurement.internal.duchy.GetContinuationTokenResponse
import org.wfanet.measurement.internal.duchy.SetContinuationTokenRequest
import org.wfanet.measurement.internal.duchy.SetContinuationTokenResponse
import org.wfanet.measurement.internal.duchy.getContinuationTokenResponse

class PostgresContinuationTokensService(
  private val client: DatabaseClient,
  private val idGenerator: IdGenerator,
) : ContinuationTokensCoroutineImplBase() {

  override suspend fun getContinuationToken(
    request: GetContinuationTokenRequest
  ): GetContinuationTokenResponse {
    val result: ContinuationTokenReader.Result =
      ContinuationTokenReader().getContinuationToken(client.singleUse())
        ?: return GetContinuationTokenResponse.getDefaultInstance()
    return getContinuationTokenResponse { token = result.continuationToken }
  }

  override suspend fun setContinuationToken(
    request: SetContinuationTokenRequest
  ): SetContinuationTokenResponse {
    try {
      SetContinuationToken(request.token).execute(client, idGenerator)
    } catch (e: ContinuationTokenInvalidException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: ContinuationTokenMalformedException) {
      throw e.asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    return SetContinuationTokenResponse.getDefaultInstance()
  }
}
