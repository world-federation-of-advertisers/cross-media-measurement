/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.access.deploy.gcloud.spanner

import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.SpannerException
import com.google.protobuf.ByteString
import com.google.protobuf.Empty
import com.google.protobuf.Timestamp
import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.access.common.TlsClientPrincipalMapping
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.deletePrincipal
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.getPrincipalByResourceId
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.getPrincipalByUserKey
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.getPrincipalIdByResourceId
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.insertPrincipal
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.insertUserPrincipal
import org.wfanet.measurement.access.deploy.gcloud.spanner.db.principalExists
import org.wfanet.measurement.access.service.internal.PrincipalAlreadyExistsException
import org.wfanet.measurement.access.service.internal.PrincipalNotFoundException
import org.wfanet.measurement.access.service.internal.PrincipalNotFoundForTlsClientException
import org.wfanet.measurement.access.service.internal.PrincipalNotFoundForUserException
import org.wfanet.measurement.access.service.internal.PrincipalTypeNotSupportedException
import org.wfanet.measurement.access.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.access.CreateUserPrincipalRequest
import org.wfanet.measurement.internal.access.DeletePrincipalRequest
import org.wfanet.measurement.internal.access.GetPrincipalRequest
import org.wfanet.measurement.internal.access.LookupPrincipalRequest
import org.wfanet.measurement.internal.access.Principal
import org.wfanet.measurement.internal.access.PrincipalKt.tlsClient
import org.wfanet.measurement.internal.access.PrincipalsGrpcKt
import org.wfanet.measurement.internal.access.principal

class SpannerPrincipalsService(
  private val databaseClient: AsyncDatabaseClient,
  private val tlsClientMapping: TlsClientPrincipalMapping,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
  private val idGenerator: IdGenerator = IdGenerator.Default,
) : PrincipalsGrpcKt.PrincipalsCoroutineImplBase(coroutineContext) {
  override suspend fun getPrincipal(request: GetPrincipalRequest): Principal {
    val tlsClient = tlsClientMapping.getByPrincipalResourceId(request.principalResourceId)
    if (tlsClient != null) {
      return tlsClient.toPrincipal()
    }

    try {
      databaseClient.singleUse().use { txn ->
        return txn.getPrincipalByResourceId(request.principalResourceId).principal
      }
    } catch (e: PrincipalNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }
  }

  override suspend fun createUserPrincipal(request: CreateUserPrincipalRequest): Principal {
    val runner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=createUserPrincipal"))
    try {
      runner.run { txn ->
        val principalId: Long = idGenerator.generateNewId { id -> txn.principalExists(id) }
        txn.insertPrincipal(principalId, request.principalResourceId)
        txn.insertUserPrincipal(principalId, request.user.issuer, request.user.subject)
      }
      val commitTimestamp: Timestamp = runner.getCommitTimestamp().toProto()
      return principal {
        principalResourceId = request.principalResourceId
        user = request.user
        createTime = commitTimestamp
        updateTime = commitTimestamp
      }
    } catch (e: SpannerException) {
      if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
        throw PrincipalAlreadyExistsException(e)
          .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
      } else {
        throw e
      }
    }
  }

  override suspend fun deletePrincipal(request: DeletePrincipalRequest): Empty {
    if (request.principalResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("principal_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (tlsClientMapping.getByPrincipalResourceId(request.principalResourceId) != null) {
      throw PrincipalTypeNotSupportedException(
          request.principalResourceId,
          Principal.IdentityCase.TLS_CLIENT,
        )
        .asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }

    try {
      databaseClient.readWriteTransaction(Options.tag("action=deletePrincipal")).run { txn ->
        val principalId: Long = txn.getPrincipalIdByResourceId(request.principalResourceId)
        txn.deletePrincipal(principalId)
      }
    } catch (e: PrincipalNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
    }

    return Empty.getDefaultInstance()
  }

  override suspend fun lookupPrincipal(request: LookupPrincipalRequest): Principal {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf case enums cannot be null.
    return when (request.lookupKeyCase) {
      LookupPrincipalRequest.LookupKeyCase.TLS_CLIENT -> {
        val authorityKeyIdentifier: ByteString = request.tlsClient.authorityKeyIdentifier
        try {
          lookupTlsClientPrincipal(authorityKeyIdentifier)
        } catch (e: PrincipalNotFoundForTlsClientException) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        }
      }
      LookupPrincipalRequest.LookupKeyCase.USER -> {
        try {
          return databaseClient.singleUse().use { txn ->
            txn.getPrincipalByUserKey(request.user.issuer, request.user.subject).principal
          }
        } catch (e: PrincipalNotFoundForUserException) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        }
      }
      LookupPrincipalRequest.LookupKeyCase.LOOKUPKEY_NOT_SET ->
        throw RequiredFieldNotSetException("lookup_key")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
  }

  /**
   * Looks up a TLS client [Principal] by [authorityKeyIdentifier].
   *
   * @throws PrincipalNotFoundForTlsClientException
   */
  private fun lookupTlsClientPrincipal(authorityKeyIdentifier: ByteString): Principal {
    val tlsClient: TlsClientPrincipalMapping.TlsClient =
      tlsClientMapping.getByAuthorityKeyIdentifier(authorityKeyIdentifier)
        ?: throw PrincipalNotFoundForTlsClientException(authorityKeyIdentifier)
    return tlsClient.toPrincipal()
  }
}

private fun TlsClientPrincipalMapping.TlsClient.toPrincipal(): Principal {
  val source = this
  return principal {
    principalResourceId = source.principalResourceId
    tlsClient = tlsClient { authorityKeyIdentifier = source.authorityKeyIdentifier }
  }
}
