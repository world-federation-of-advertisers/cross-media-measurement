// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.crypto.tink.Aead
import com.google.protobuf.ByteString
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.util.Timestamps
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusException
import java.io.IOException
import java.security.GeneralSecurityException
import java.time.Clock
import java.time.Duration
import org.wfanet.measurement.api.v2alpha.EventGroupSyncToken
import org.wfanet.measurement.api.v2alpha.EventGroupSyncTokenKt.after
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.SyncEventGroupsRequest
import org.wfanet.measurement.api.v2alpha.SyncEventGroupsResponse
import org.wfanet.measurement.api.v2alpha.eventGroupSyncToken
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.api.v2alpha.syncEventGroupsResponse
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.asRuntimeException
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ReadEventGroupsRequestKt
import org.wfanet.measurement.internal.kingdom.readEventGroupsRequest

/** Stateless operator synchronization. Tokens bind the caller and snapshot. */
class EventGroupSync(private val stub: EventGroupsCoroutineStub, private val options: Options) {
  data class Options(
    val operatorNames: Set<String>,
    val tokenAead: Aead,
    val pageTokenTtl: Duration,
    val syncTokenTtl: Duration,
    val clock: Clock,
  ) {
    init {
      require(
        operatorNames.isNotEmpty() &&
          operatorNames.all { MeasurementPrincipal.fromName(it) != null }
      )
      require(
        !pageTokenTtl.isNegative && !pageTokenTtl.isZero && pageTokenTtl <= Duration.ofHours(1)
      )
      require(!syncTokenTtl.isNegative && !syncTokenTtl.isZero)
    }
  }

  suspend fun sync(request: SyncEventGroupsRequest): SyncEventGroupsResponse {
    val principalName = principalFromCurrentContext.resourceKey.toName()
    if (principalName !in options.operatorNames) {
      throw Status.PERMISSION_DENIED.withDescription("Operator permission required")
        .asRuntimeException()
    }
    grpcRequire(request.parent == "dataProviders/-") { "parent must be dataProviders/-" }
    grpcRequire(request.pageSize >= 0) { "page_size cannot be negative" }
    val associatedData =
      "EventGroups.SyncEventGroups/v1\n$principalName\n${request.parent}"
        .toByteArray(Charsets.UTF_8)
    val syncToken =
      if (request.syncToken.isEmpty()) null
      else decode(request.syncToken, EventGroupSyncToken.Kind.SYNC, associatedData)
    val pageToken =
      if (request.pageToken.isEmpty()) null
      else decode(request.pageToken, EventGroupSyncToken.Kind.PAGE, associatedData)
    grpcRequire(pageToken == null || pageToken.syncToken == request.syncToken) {
      "sync_token must remain unchanged while paging"
    }
    val pageSize = if (request.pageSize == 0) 1000 else request.pageSize.coerceAtMost(2000)
    val internalRequest = readEventGroupsRequest {
      limit = pageSize + 1
      if (syncToken != null) updatedAfter = syncToken.readTime
      if (pageToken != null) {
        readTime = pageToken.readTime
        after =
          ReadEventGroupsRequestKt.after {
            updateTime = pageToken.after.updateTime
            externalDataProviderId = pageToken.after.externalDataProviderId
            externalEventGroupId = pageToken.after.externalEventGroupId
          }
      }
    }
    val response =
      try {
        stub.readEventGroups(internalRequest)
      } catch (e: StatusException) {
        if (
          pageToken != null &&
            e.status.code == Status.Code.FAILED_PRECONDITION &&
            e.errorInfo?.domain == "internal.kingdom.wfanet.org" &&
            e.errorInfo?.reason == "READ_TIME_EXPIRED"
        ) {
          throw expired(EventGroupSyncToken.Kind.PAGE)
        }
        throw when (e.status.code) {
            Status.Code.DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
            Status.Code.UNAVAILABLE -> Status.UNAVAILABLE
            Status.Code.CANCELLED -> Status.CANCELLED
            Status.Code.ABORTED -> Status.ABORTED
            Status.Code.RESOURCE_EXHAUSTED -> Status.RESOURCE_EXHAUSTED
            else -> Status.INTERNAL
          }
          .withDescription("Unable to read event groups")
          .asRuntimeException()
      }
    check(response.hasReadTime() && Timestamps.isValid(response.readTime))
    check(pageToken == null || response.readTime == pageToken.readTime)
    check(syncToken == null || Timestamps.compare(response.readTime, syncToken.readTime) >= 0)
    check(response.eventGroupsCount <= pageSize + 1)
    val groups = response.eventGroupsList.take(pageSize)
    return syncEventGroupsResponse {
      readTime = response.readTime
      eventGroups += groups.map { it.toEventGroup() }
      if (response.eventGroupsCount > pageSize) {
        val last = groups.last()
        nextPageToken =
          encode(
            eventGroupSyncToken {
              version = 1
              kind = EventGroupSyncToken.Kind.PAGE
              expiresAt =
                pageToken?.expiresAt
                  ?: response.readTime.toInstant().plus(options.pageTokenTtl).toProtoTime()
              this.readTime = response.readTime
              this.syncToken = request.syncToken
              after = after {
                updateTime = last.updateTime
                externalDataProviderId = last.externalDataProviderId
                externalEventGroupId = last.externalEventGroupId
              }
            },
            associatedData,
          )
      } else {
        nextSyncToken =
          encode(
            eventGroupSyncToken {
              version = 1
              kind = EventGroupSyncToken.Kind.SYNC
              expiresAt = response.readTime.toInstant().plus(options.syncTokenTtl).toProtoTime()
              this.readTime = response.readTime
            },
            associatedData,
          )
      }
    }
  }

  private fun decode(
    value: String,
    kind: EventGroupSyncToken.Kind,
    associatedData: ByteArray,
  ): EventGroupSyncToken {
    grpcRequire(value.length <= 4096) { "Invalid token" }
    val token =
      try {
        EventGroupSyncToken.parseFrom(
          options.tokenAead.decrypt(value.base64UrlDecode().toByteArray(), associatedData)
        )
      } catch (e: GeneralSecurityException) {
        throw Status.INVALID_ARGUMENT.withDescription("Invalid token").asRuntimeException()
      } catch (e: InvalidProtocolBufferException) {
        throw Status.INVALID_ARGUMENT.withDescription("Invalid token").asRuntimeException()
      } catch (e: IOException) {
        throw Status.INVALID_ARGUMENT.withDescription("Invalid token").asRuntimeException()
      } catch (e: IllegalArgumentException) {
        throw Status.INVALID_ARGUMENT.withDescription("Invalid token").asRuntimeException()
      }
    grpcRequire(
      token.version == 1 &&
        token.kind == kind &&
        token.hasExpiresAt() &&
        Timestamps.isValid(token.expiresAt) &&
        token.hasReadTime() &&
        Timestamps.isValid(token.readTime) &&
        (kind != EventGroupSyncToken.Kind.PAGE ||
          (token.hasAfter() &&
            token.after.hasUpdateTime() &&
            Timestamps.isValid(token.after.updateTime) &&
            token.after.externalDataProviderId != 0L &&
            token.after.externalEventGroupId != 0L))
    ) {
      "Invalid token"
    }
    if (!options.clock.instant().isBefore(token.expiresAt.toInstant())) throw expired(kind)
    return token
  }

  private fun encode(token: EventGroupSyncToken, associatedData: ByteArray): String =
    ByteString.copyFrom(options.tokenAead.encrypt(token.toByteArray(), associatedData))
      .base64UrlEncode()

  private fun expired(kind: EventGroupSyncToken.Kind) =
    Status.FAILED_PRECONDITION.withDescription(
        if (kind == EventGroupSyncToken.Kind.PAGE)
          "Snapshot expired; restart from the last completed sync token"
        else "Sync token expired; restart with a full inventory"
      )
      .asRuntimeException(
        errorInfo {
          domain = Errors.DOMAIN
          reason =
            if (kind == EventGroupSyncToken.Kind.PAGE) "SYNC_PAGE_TOKEN_EXPIRED"
            else "SYNC_TOKEN_EXPIRED"
        }
      )
}
