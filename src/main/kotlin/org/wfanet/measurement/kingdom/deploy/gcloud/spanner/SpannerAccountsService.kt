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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner

import io.grpc.Status
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ActivateAccountRequest
import org.wfanet.measurement.internal.kingdom.AuthenticateAccountRequest
import org.wfanet.measurement.internal.kingdom.GenerateOpenIdRequestParamsRequest
import org.wfanet.measurement.internal.kingdom.OpenIdRequestParams
import org.wfanet.measurement.internal.kingdom.ReplaceAccountIdentityRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateAccount

class SpannerAccountsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : AccountsCoroutineImplBase() {

  override suspend fun createAccount(request: Account): Account {
    try {
      return CreateAccount(
          request.externalCreatorAccountId,
          request.externalOwnedMeasurementConsumerId
        )
        .execute(client, idGenerator)
    } catch (e: KingdomInternalException) {
      when (e.code) {
        KingdomInternalException.Code.ACCOUNT_NOT_FOUND ->
          failGrpc(Status.NOT_FOUND) { "Creator account not found" }
        KingdomInternalException.Code.ACCOUNT_NOT_OWNER ->
          failGrpc(Status.PERMISSION_DENIED) {
            "Caller does not own the owned measurement consumer"
          }
        KingdomInternalException.Code.REQUISITION_NOT_FOUND,
        KingdomInternalException.Code.REQUISITION_STATE_ILLEGAL,
        KingdomInternalException.Code.MEASUREMENT_STATE_ILLEGAL,
        KingdomInternalException.Code.DUCHY_NOT_FOUND,
        KingdomInternalException.Code.MEASUREMENT_CONSUMER_NOT_FOUND,
        KingdomInternalException.Code.MODEL_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.MEASUREMENT_NOT_FOUND,
        KingdomInternalException.Code.DATA_PROVIDER_NOT_FOUND,
        KingdomInternalException.Code.CERT_SUBJECT_KEY_ID_ALREADY_EXISTS,
        KingdomInternalException.Code.CERTIFICATE_NOT_FOUND,
        KingdomInternalException.Code.CERTIFICATE_IS_INVALID,
        KingdomInternalException.Code.CERTIFICATE_REVOCATION_STATE_ILLEGAL,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_STATE_ILLEGAL,
        KingdomInternalException.Code.COMPUTATION_PARTICIPANT_NOT_FOUND -> throw e
      }
    }
  }

  override suspend fun activateAccount(request: ActivateAccountRequest): Account {
    TODO("Not yet implemented")
  }

  override suspend fun replaceAccountIdentity(request: ReplaceAccountIdentityRequest): Account {
    TODO("Not yet implemented")
  }

  override suspend fun authenticateAccount(request: AuthenticateAccountRequest): Account {
    TODO("Not yet implemented")
  }

  override suspend fun generateOpenIdRequestParams(
    request: GenerateOpenIdRequestParamsRequest
  ): OpenIdRequestParams {
    TODO("Not yet implemented")
  }
}
