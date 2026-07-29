/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import io.grpc.Status
import io.grpc.StatusException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.edpaggregator.service.InvalidFieldValueException
import org.wfanet.measurement.edpaggregator.service.RequiredFieldNotSetException
import org.wfanet.measurement.edpaggregator.service.UnlinkedClientAccountKey
import org.wfanet.measurement.edpaggregator.service.internal.Errors as InternalErrors
import org.wfanet.measurement.edpaggregator.v1alpha.ReplaceUnlinkedClientAccountsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ReplaceUnlinkedClientAccountsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.UnlinkedClientAccount
import org.wfanet.measurement.edpaggregator.v1alpha.UnlinkedClientAccountsServiceGrpcKt.UnlinkedClientAccountsServiceCoroutineImplBase
import org.wfanet.measurement.edpaggregator.v1alpha.replaceUnlinkedClientAccountsResponse
import org.wfanet.measurement.edpaggregator.v1alpha.unlinkedClientAccount
import org.wfanet.measurement.internal.edpaggregator.ReplaceUnlinkedClientAccountsResponse as InternalReplaceUnlinkedClientAccountsResponse
import org.wfanet.measurement.internal.edpaggregator.UnlinkedClientAccount as InternalUnlinkedClientAccount
import org.wfanet.measurement.internal.edpaggregator.UnlinkedClientAccountsServiceGrpcKt.UnlinkedClientAccountsServiceCoroutineStub as InternalUnlinkedClientAccountsServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.replaceUnlinkedClientAccountsRequest as internalReplaceUnlinkedClientAccountsRequest
import org.wfanet.measurement.internal.edpaggregator.unlinkedClientAccount as internalUnlinkedClientAccount

/**
 * Public v1alpha implementation of `UnlinkedClientAccountsService`.
 *
 * Fronts the internal `UnlinkedClientAccountsService`, translating between public resource names
 * and internal resource IDs. `ReplaceUnlinkedClientAccounts` is a full-set reconcile scoped to a
 * single DataProvider.
 */
class UnlinkedClientAccountsService(
  private val internalUnlinkedClientAccountsStub:
    InternalUnlinkedClientAccountsServiceCoroutineStub,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : UnlinkedClientAccountsServiceCoroutineImplBase(coroutineContext) {

  override suspend fun replaceUnlinkedClientAccounts(
    request: ReplaceUnlinkedClientAccountsRequest
  ): ReplaceUnlinkedClientAccountsResponse {
    if (request.parent.isEmpty()) {
      throw RequiredFieldNotSetException("parent")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val dataProviderKey: DataProviderKey =
      DataProviderKey.fromName(request.parent)
        ?: throw InvalidFieldValueException("parent")
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)

    val referenceIds = mutableSetOf<String>()
    request.unlinkedClientAccountsList.forEachIndexed { index, account ->
      if (account.clientAccountReferenceId.isEmpty()) {
        throw RequiredFieldNotSetException(
            "unlinked_client_accounts.$index.client_account_reference_id"
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
      if (!referenceIds.add(account.clientAccountReferenceId)) {
        throw InvalidFieldValueException(
            "unlinked_client_accounts.$index.client_account_reference_id"
          )
          .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
      }
    }

    val internalResponse: InternalReplaceUnlinkedClientAccountsResponse =
      try {
        internalUnlinkedClientAccountsStub.replaceUnlinkedClientAccounts(
          internalReplaceUnlinkedClientAccountsRequest {
            dataProviderResourceId = dataProviderKey.dataProviderId
            unlinkedClientAccounts += request.unlinkedClientAccountsList.map { it.toInternal() }
          }
        )
      } catch (e: StatusException) {
        throw when (InternalErrors.getReason(e)) {
          // The public service validates all inputs above, so any error from the internal
          // service is a programming error and maps to INTERNAL (Lesson #37). Listed
          // exhaustively (not `else`) so a new reason forces a compile error (Lesson #38).
          InternalErrors.Reason.DATA_PROVIDER_MISMATCH,
          InternalErrors.Reason.IMPRESSION_METADATA_NOT_FOUND,
          InternalErrors.Reason.IMPRESSION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.IMPRESSION_METADATA_STATE_INVALID,
          InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_NOT_FOUND,
          InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_STATE_INVALID,
          InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_MODEL_LINE_NOT_FOUND,
          InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_INVALID,
          InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_MODEL_LINE_CONCURRENT,
          InternalErrors.Reason.POOL_ASSIGNMENT_JOB_NOT_FOUND,
          InternalErrors.Reason.POOL_ASSIGNMENT_JOB_STATE_INVALID,
          InternalErrors.Reason.POOL_ASSIGNMENT_JOB_ALREADY_EXISTS,
          InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_ALREADY_EXISTS,
          InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_FILE_NOT_FOUND,
          InternalErrors.Reason.RAW_IMPRESSION_UPLOAD_FILE_ALREADY_EXISTS,
          InternalErrors.Reason.VID_LABELING_JOB_NOT_FOUND,
          InternalErrors.Reason.VID_LABELING_JOB_STATE_INVALID,
          InternalErrors.Reason.VID_LABELING_JOB_ALREADY_EXISTS,
          InternalErrors.Reason.RANKER_JOB_NOT_FOUND,
          InternalErrors.Reason.RANKER_JOB_ALREADY_EXISTS,
          InternalErrors.Reason.RANKER_JOB_STATE_INVALID,
          InternalErrors.Reason.RANK_INDEX_BLOB_NOT_FOUND,
          InternalErrors.Reason.RANK_INDEX_BLOB_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND,
          InternalErrors.Reason.REQUISITION_METADATA_NOT_FOUND_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_BLOB_URI,
          InternalErrors.Reason.REQUISITION_METADATA_ALREADY_EXISTS_BY_CMMS_REQUISITION,
          InternalErrors.Reason.REQUISITION_METADATA_STATE_INVALID,
          InternalErrors.Reason.ETAG_MISMATCH,
          InternalErrors.Reason.REQUIRED_FIELD_NOT_SET,
          InternalErrors.Reason.INVALID_FIELD_VALUE,
          null -> Status.INTERNAL.withCause(e).asRuntimeException()
        }
      }

    return replaceUnlinkedClientAccountsResponse {
      unlinkedClientAccounts +=
        internalResponse.unlinkedClientAccountsList.map { it.toUnlinkedClientAccount() }
    }
  }
}

/** Converts a public [UnlinkedClientAccount] to an internal [InternalUnlinkedClientAccount]. */
private fun UnlinkedClientAccount.toInternal(): InternalUnlinkedClientAccount {
  val source = this
  return internalUnlinkedClientAccount {
    clientAccountReferenceId = source.clientAccountReferenceId
    brands += source.brandsList
    eventGroupReferenceId = source.eventGroupReferenceId
  }
}

/** Converts an internal [InternalUnlinkedClientAccount] to a public [UnlinkedClientAccount]. */
private fun InternalUnlinkedClientAccount.toUnlinkedClientAccount(): UnlinkedClientAccount {
  val source = this
  return unlinkedClientAccount {
    name =
      UnlinkedClientAccountKey(source.dataProviderResourceId, source.clientAccountReferenceId)
        .toName()
    clientAccountReferenceId = source.clientAccountReferenceId
    brands += source.brandsList
    eventGroupReferenceId = source.eventGroupReferenceId
    firstObservedTime = source.firstObservedTime
  }
}
