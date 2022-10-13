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
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.Account
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ActivateAccountRequest
import org.wfanet.measurement.internal.kingdom.AuthenticateAccountRequest
import org.wfanet.measurement.internal.kingdom.CreateMeasurementConsumerCreationTokenRequest
import org.wfanet.measurement.internal.kingdom.CreateMeasurementConsumerCreationTokenResponse
import org.wfanet.measurement.internal.kingdom.GenerateOpenIdRequestParamsRequest
import org.wfanet.measurement.internal.kingdom.GetOpenIdRequestParamsRequest
import org.wfanet.measurement.internal.kingdom.OpenIdRequestParams
import org.wfanet.measurement.internal.kingdom.ReplaceAccountIdentityRequest
import org.wfanet.measurement.internal.kingdom.createMeasurementConsumerCreationTokenResponse
import org.wfanet.measurement.internal.kingdom.openIdRequestParams
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.AccountActivationStateIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.AccountNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.DuplicateAccountIdentityException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.KingdomInternalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.PermissionDeniedException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.AccountReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.OpenIdConnectIdentityReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.OpenIdRequestParamsReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ActivateAccount
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateAccount
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.CreateMeasurementConsumerCreationToken
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.GenerateOpenIdRequestParams
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.writers.ReplaceAccountIdentityWithNewOpenIdConnectIdentity

private const val VALID_SECONDS = 3600L

class SpannerAccountsService(
  private val idGenerator: IdGenerator,
  private val client: AsyncDatabaseClient
) : AccountsCoroutineImplBase() {
  override suspend fun createAccount(request: Account): Account {
    try {
      val externalCreatorAccountId: ExternalId? =
        if (request.externalCreatorAccountId != 0L) {
          ExternalId(request.externalCreatorAccountId)
        } else {
          null
        }

      val externalOwnedMeasurementConsumerId: ExternalId? =
        if (request.externalOwnedMeasurementConsumerId != 0L) {
          ExternalId(request.externalOwnedMeasurementConsumerId)
        } else {
          null
        }

      return CreateAccount(
          externalCreatorAccountId = externalCreatorAccountId,
          externalOwnedMeasurementConsumerId = externalOwnedMeasurementConsumerId
        )
        .execute(client, idGenerator)
    } catch (e: AccountNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "Creator's Account not found." }
    } catch (e: PermissionDeniedException) {
      e.throwStatusRuntimeException(Status.PERMISSION_DENIED) {
        "Caller does not own the owned measurement consumer."
      }
    } catch (e: KingdomInternalException) {
      e.throwStatusRuntimeException(Status.INTERNAL) { "Unexpected internal error." }
    }
  }

  override suspend fun createMeasurementConsumerCreationToken(
    request: CreateMeasurementConsumerCreationTokenRequest
  ): CreateMeasurementConsumerCreationTokenResponse {
    return createMeasurementConsumerCreationTokenResponse {
      measurementConsumerCreationToken =
        CreateMeasurementConsumerCreationToken().execute(client, idGenerator)
    }
  }

  override suspend fun activateAccount(request: ActivateAccountRequest): Account {
    try {
      return ActivateAccount(
          externalAccountId = ExternalId(request.externalAccountId),
          activationToken = ExternalId(request.activationToken),
          issuer = request.identity.issuer,
          subject = request.identity.subject
        )
        .execute(client, idGenerator)
    } catch (e: PermissionDeniedException) {
      e.throwStatusRuntimeException(Status.PERMISSION_DENIED) {
        "Activation token is not valid for this account."
      }
    } catch (e: DuplicateAccountIdentityException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
        "Issuer and subject pair already exists."
      }
    } catch (e: AccountActivationStateIllegalException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
        "Cannot activate an account again. "
      }
    } catch (e: AccountNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "Account to activate has not been found." }
    } catch (e: KingdomInternalException) {
      e.throwStatusRuntimeException(Status.INTERNAL) { "Unexpected internal error" }
    }
  }

  override suspend fun replaceAccountIdentity(request: ReplaceAccountIdentityRequest): Account {
    try {
      return ReplaceAccountIdentityWithNewOpenIdConnectIdentity(
          externalAccountId = ExternalId(request.externalAccountId),
          issuer = request.identity.issuer,
          subject = request.identity.subject
        )
        .execute(client, idGenerator)
    } catch (e: DuplicateAccountIdentityException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
        "Issuer and subject pair already exists."
      }
    } catch (e: AccountNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "Account was not found." }
    } catch (e: AccountActivationStateIllegalException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) {
        "Account has not been activated yet."
      }
    } catch (e: KingdomInternalException) {
      e.throwStatusRuntimeException(Status.INTERNAL) { "Unexpected internal error." }
    }
  }

  override suspend fun authenticateAccount(request: AuthenticateAccountRequest): Account {
    val identityResult =
      OpenIdConnectIdentityReader()
        .readByIssuerAndSubject(
          client.singleUse(),
          request.identity.issuer,
          request.identity.subject
        )
        ?: failGrpc(Status.NOT_FOUND) { "Identity not found" }

    return AccountReader()
      .readByInternalAccountId(client.singleUse(), identityResult.accountId)
      ?.account
      ?: failGrpc(Status.NOT_FOUND) { "Account not found" }
  }

  override suspend fun generateOpenIdRequestParams(
    request: GenerateOpenIdRequestParamsRequest
  ): OpenIdRequestParams {
    return GenerateOpenIdRequestParams(VALID_SECONDS).execute(client, idGenerator)
  }

  override suspend fun getOpenIdRequestParams(
    request: GetOpenIdRequestParamsRequest
  ): OpenIdRequestParams {
    val result =
      OpenIdRequestParamsReader().readByState(client.singleUse(), ExternalId(request.state))

    return if (result == null) {
      OpenIdRequestParams.getDefaultInstance()
    } else {
      openIdRequestParams {
        state = result.state.value
        nonce = result.nonce.value
        isExpired = result.isExpired
      }
    }
  }
}
