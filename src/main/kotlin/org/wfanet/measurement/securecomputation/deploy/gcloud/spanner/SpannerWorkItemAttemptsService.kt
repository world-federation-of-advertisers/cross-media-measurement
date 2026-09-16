/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.deploy.gcloud.spanner

import com.google.cloud.spanner.ErrorCode
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.SpannerException
import io.grpc.Status
import java.time.Clock
import java.time.Duration
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.generateNewId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.securecomputation.controlplane.CompleteWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.CreateWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.FailWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.GetWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemAttemptsPageTokenKt
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemAttemptsRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.ListWorkItemAttemptsResponse
import org.wfanet.measurement.internal.securecomputation.controlplane.RenewWorkItemAttemptRequest
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.internal.securecomputation.controlplane.copy
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemAttemptsPageToken
import org.wfanet.measurement.internal.securecomputation.controlplane.listWorkItemAttemptsResponse
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.WorkItemAttemptResult
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.completeWorkItemAttempt
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.failWorkItemAttempt
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.failWorkItemAttemptAndScheduleRecovery
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.getActiveWorkItemAttempt
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.getWorkItemAttemptByResourceId
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.getWorkItemByResourceId
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.insertWorkItemAttempt
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.readWorkItemAttempts
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.renewWorkItemAttemptLease
import org.wfanet.measurement.securecomputation.deploy.gcloud.spanner.db.workItemAttemptExists
import org.wfanet.measurement.securecomputation.service.internal.InvalidFieldValueException
import org.wfanet.measurement.securecomputation.service.internal.QueueMapping
import org.wfanet.measurement.securecomputation.service.internal.QueueNotFoundForWorkItem
import org.wfanet.measurement.securecomputation.service.internal.RequiredFieldNotSetException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemAttemptAlreadyExistsException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemAttemptInvalidStateException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemAttemptNotFoundException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemGenerationMismatchException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemInvalidStateException
import org.wfanet.measurement.securecomputation.service.internal.WorkItemNotFoundException

class SpannerWorkItemAttemptsService(
  private val databaseClient: AsyncDatabaseClient,
  private val queueMapping: QueueMapping,
  private val idGenerator: IdGenerator,
  coroutineContext: CoroutineContext,
  private val clock: Clock = Clock.systemUTC(),
  private val attemptLeaseDuration: Duration = DEFAULT_ATTEMPT_LEASE_DURATION,
  private val initialAttemptRetryDelay: Duration = DEFAULT_INITIAL_ATTEMPT_RETRY_DELAY,
  private val maxAttemptRetryDelay: Duration = DEFAULT_MAX_ATTEMPT_RETRY_DELAY,
) : WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineImplBase(coroutineContext) {

  init {
    require(attemptLeaseDuration > Duration.ZERO) { "attemptLeaseDuration must be positive" }
    require(initialAttemptRetryDelay > Duration.ZERO) {
      "initialAttemptRetryDelay must be positive"
    }
    require(maxAttemptRetryDelay >= initialAttemptRetryDelay) {
      "maxAttemptRetryDelay must not be less than initialAttemptRetryDelay"
    }
  }

  override suspend fun createWorkItemAttempt(
    request: CreateWorkItemAttemptRequest
  ): WorkItemAttempt {

    if (request.workItemAttempt.workItemResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("work_item_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.workItemAttempt.workItemAttemptResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("work_item_attempt_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    if (request.hasExpectedWorkItemGeneration() && request.expectedWorkItemGeneration < 1L) {
      throw InvalidFieldValueException("expected_work_item_generation") { fieldName ->
          "$fieldName must be at least 1"
        }
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val expectedGeneration =
      if (request.hasExpectedWorkItemGeneration()) {
        request.expectedWorkItemGeneration
      } else {
        INITIAL_GENERATION
      }
    val leaseExpirationTime =
      if (request.supportsAttemptLease) clock.instant().plus(attemptLeaseDuration) else null

    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=createWorkItemAttempt"))

    val workItemAttempt =
      try {
        transactionRunner.run { txn ->
          val result =
            txn.getWorkItemByResourceId(queueMapping, request.workItemAttempt.workItemResourceId)
          if (result.workItem.generation != expectedGeneration) {
            throw WorkItemGenerationMismatchException(
              result.workItem.workItemResourceId,
              expectedGeneration,
              result.workItem.generation,
            )
          }
          val workItemState = result.workItem.state
          @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum accessors cannot return null.
          when (workItemState) {
            WorkItem.State.FAILED,
            WorkItem.State.SUCCEEDED,
            WorkItem.State.STATE_UNSPECIFIED,
            WorkItem.State.UNRECOGNIZED -> {
              throw WorkItemInvalidStateException(result.workItem.workItemResourceId, workItemState)
            }
            WorkItem.State.QUEUED,
            WorkItem.State.RUNNING -> {
              val activeAttempt = txn.getActiveWorkItemAttempt(result.workItemId)
              if (activeAttempt != null) {
                if (
                  request.supportsAttemptLease &&
                    !activeAttempt.workItemAttempt.hasLeaseExpirationTime()
                ) {
                  txn.failWorkItemAttempt(
                    activeAttempt.workItemId,
                    activeAttempt.workItemAttemptId,
                    "Replaced by a lease-capable worker",
                  )
                } else {
                  throw WorkItemInvalidStateException(
                    result.workItem.workItemResourceId,
                    WorkItem.State.RUNNING,
                  )
                }
              }
              val workItemAttemptId: Long =
                idGenerator.generateNewId { id -> txn.workItemAttemptExists(result.workItemId, id) }
              val (attemptNumber, state) =
                txn.insertWorkItemAttempt(
                  result.workItemId,
                  workItemAttemptId,
                  request.workItemAttempt.workItemAttemptResourceId,
                  leaseExpirationTime,
                )
              request.workItemAttempt.copy {
                this.state = state
                this.attemptNumber = attemptNumber
                if (leaseExpirationTime != null) {
                  this.leaseExpirationTime = leaseExpirationTime.toProtoTime()
                }
              }
            }
          }
        }
      } catch (e: SpannerException) {
        if (e.errorCode == ErrorCode.ALREADY_EXISTS) {
          throw WorkItemAttemptAlreadyExistsException(e)
            .asStatusRuntimeException(Status.Code.ALREADY_EXISTS)
        } else {
          throw e
        }
      } catch (e: WorkItemInvalidStateException) {
        throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
      } catch (e: WorkItemGenerationMismatchException) {
        throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
      } catch (e: WorkItemNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      }

    val commitTimestamp = transactionRunner.getCommitTimestamp().toProto()
    return workItemAttempt.copy {
      this.createTime = commitTimestamp
      this.updateTime = commitTimestamp
    }
  }

  override suspend fun getWorkItemAttempt(request: GetWorkItemAttemptRequest): WorkItemAttempt {
    if (request.workItemResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("work_item_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.workItemAttemptResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("work_item_attempt_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }

    val workItemAttemptResult: WorkItemAttemptResult =
      try {
        databaseClient.singleUse().use { txn ->
          txn.getWorkItemAttemptByResourceId(
            request.workItemResourceId,
            request.workItemAttemptResourceId,
          )
        }
      } catch (e: WorkItemAttemptNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
      } catch (e: QueueNotFoundForWorkItem) {
        throw e.asStatusRuntimeException(Status.Code.INTERNAL)
      }

    return workItemAttemptResult.workItemAttempt
  }

  override suspend fun failWorkItemAttempt(request: FailWorkItemAttemptRequest): WorkItemAttempt {
    if (request.workItemResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("work_item_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.workItemAttemptResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("work_item_attempt_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=failWorkItemAttempt"))

    val (workItemAttempt, wasUpdated) =
      transactionRunner.run { txn ->
        try {
          val workItemAttemptResult =
            txn.getWorkItemAttemptByResourceId(
              request.workItemResourceId,
              request.workItemAttemptResourceId,
            )
          val workItemAttemptState = workItemAttemptResult.workItemAttempt.state
          @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum accessors cannot return null.
          when (workItemAttemptState) {
            WorkItemAttempt.State.FAILED -> workItemAttemptResult.workItemAttempt to false
            WorkItemAttempt.State.SUCCEEDED,
            WorkItemAttempt.State.STATE_UNSPECIFIED,
            WorkItemAttempt.State.UNRECOGNIZED -> {
              throw WorkItemAttemptInvalidStateException(
                workItemAttemptResult.workItemAttempt.workItemResourceId,
                workItemAttemptResult.workItemAttempt.workItemAttemptResourceId,
                workItemAttemptState,
              )
            }
            WorkItemAttempt.State.ACTIVE -> {
              val state =
                if (workItemAttemptResult.workItemAttempt.hasLeaseExpirationTime()) {
                  val queue =
                    queueMapping.getQueueById(workItemAttemptResult.queueId)
                      ?: throw QueueNotFoundForWorkItem(
                        workItemAttemptResult.workItemAttempt.workItemResourceId
                      )
                  txn.failWorkItemAttemptAndScheduleRecovery(
                    workItemAttemptResult,
                    queue,
                    request.errorMessage.take(MAX_ERROR_MESSAGE_LENGTH),
                    clock.instant().plus(attemptRetryDelay(workItemAttemptResult.workItemAttempt)),
                  )
                  WorkItemAttempt.State.FAILED
                } else {
                  txn.failWorkItemAttempt(
                    workItemAttemptResult.workItemId,
                    workItemAttemptResult.workItemAttemptId,
                    request.errorMessage.take(MAX_ERROR_MESSAGE_LENGTH),
                  )
                }
              workItemAttemptResult.workItemAttempt.copy {
                this.state = state
                errorMessage = request.errorMessage.take(MAX_ERROR_MESSAGE_LENGTH)
              } to true
            }
          }
        } catch (e: WorkItemAttemptInvalidStateException) {
          throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        } catch (e: WorkItemAttemptNotFoundException) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        } catch (e: QueueNotFoundForWorkItem) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        }
      }
    return if (wasUpdated) {
      workItemAttempt.copy { this.updateTime = transactionRunner.getCommitTimestamp().toProto() }
    } else {
      workItemAttempt
    }
  }

  override suspend fun completeWorkItemAttempt(
    request: CompleteWorkItemAttemptRequest
  ): WorkItemAttempt {
    if (request.workItemResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("work_item_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.workItemAttemptResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("work_item_attempt_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val transactionRunner: AsyncDatabaseClient.TransactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=completeWorkItemAttempt"))

    val workItemAttempt =
      transactionRunner.run { txn ->
        try {
          val workItemAttemptResult =
            txn.getWorkItemAttemptByResourceId(
              request.workItemResourceId,
              request.workItemAttemptResourceId,
            )
          val workItemAttemptState = workItemAttemptResult.workItemAttempt.state
          @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Protobuf enum accessors cannot return null.
          when (workItemAttemptState) {
            WorkItemAttempt.State.FAILED,
            WorkItemAttempt.State.SUCCEEDED,
            WorkItemAttempt.State.STATE_UNSPECIFIED,
            WorkItemAttempt.State.UNRECOGNIZED -> {
              throw WorkItemAttemptInvalidStateException(
                workItemAttemptResult.workItemAttempt.workItemResourceId,
                workItemAttemptResult.workItemAttempt.workItemAttemptResourceId,
                workItemAttemptState,
              )
            }
            WorkItemAttempt.State.ACTIVE -> {
              if (
                workItemAttemptResult.workItemAttempt.hasLeaseExpirationTime() &&
                  !workItemAttemptResult.workItemAttempt.leaseExpirationTime
                    .toInstant()
                    .isAfter(clock.instant())
              ) {
                throw expiredLeaseException(workItemAttemptResult)
              }
              val state =
                txn.completeWorkItemAttempt(
                  workItemAttemptResult.workItemId,
                  workItemAttemptResult.workItemAttemptId,
                )
              workItemAttemptResult.workItemAttempt.copy { this.state = state }
            }
          }
        } catch (e: WorkItemAttemptInvalidStateException) {
          throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        } catch (e: WorkItemAttemptNotFoundException) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        } catch (e: QueueNotFoundForWorkItem) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        }
      }
    return workItemAttempt.copy {
      this.updateTime = transactionRunner.getCommitTimestamp().toProto()
    }
  }

  override suspend fun renewWorkItemAttempt(request: RenewWorkItemAttemptRequest): WorkItemAttempt {
    if (request.workItemResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("work_item_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    if (request.workItemAttemptResourceId.isEmpty()) {
      throw RequiredFieldNotSetException("work_item_attempt_resource_id")
        .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    val leaseExpirationTime = clock.instant().plus(attemptLeaseDuration)
    val transactionRunner =
      databaseClient.readWriteTransaction(Options.tag("action=renewWorkItemAttempt"))
    val workItemAttempt =
      transactionRunner.run { txn ->
        try {
          val result =
            txn.getWorkItemAttemptByResourceId(
              request.workItemResourceId,
              request.workItemAttemptResourceId,
            )
          if (result.workItemAttempt.state != WorkItemAttempt.State.ACTIVE) {
            throw WorkItemAttemptInvalidStateException(
              result.workItemAttempt.workItemResourceId,
              result.workItemAttempt.workItemAttemptResourceId,
              result.workItemAttempt.state,
            )
          }
          if (
            !result.workItemAttempt.hasLeaseExpirationTime() ||
              !result.workItemAttempt.leaseExpirationTime.toInstant().isAfter(clock.instant())
          ) {
            throw expiredLeaseException(result)
          }
          txn.renewWorkItemAttemptLease(
            result.workItemId,
            result.workItemAttemptId,
            leaseExpirationTime,
          )
          result.workItemAttempt.copy {
            this.leaseExpirationTime = leaseExpirationTime.toProtoTime()
          }
        } catch (e: WorkItemAttemptInvalidStateException) {
          throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
        } catch (e: WorkItemAttemptNotFoundException) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        } catch (e: QueueNotFoundForWorkItem) {
          throw e.asStatusRuntimeException(Status.Code.NOT_FOUND)
        }
      }
    return workItemAttempt.copy { updateTime = transactionRunner.getCommitTimestamp().toProto() }
  }

  override suspend fun listWorkItemAttempts(
    request: ListWorkItemAttemptsRequest
  ): ListWorkItemAttemptsResponse {
    if (request.pageSize < 0) {
      throw InvalidFieldValueException("max_page_size") { fieldName ->
        "$fieldName must be non-negative"
      }
    }
    val pageSize =
      if (request.pageSize == 0) {
        DEFAULT_PAGE_SIZE
      } else {
        request.pageSize.coerceAtMost(MAX_PAGE_SIZE)
      }
    val after = if (request.hasPageToken()) request.pageToken.after else null
    return databaseClient.singleUse().use { txn ->
      val workItemAttempts: Flow<WorkItemAttempt> =
        txn.readWorkItemAttempts(pageSize + 1, request.workItemResourceId, after).map {
          it.workItemAttempt
        }
      listWorkItemAttemptsResponse {
        workItemAttempts.collectIndexed { index, workItemAttempt ->
          if (index == pageSize) {
            nextPageToken = listWorkItemAttemptsPageToken {
              this.after =
                ListWorkItemAttemptsPageTokenKt.after {
                  createdAfter =
                    this@listWorkItemAttemptsResponse.workItemAttempts.last().createTime
                  workItemResourceId =
                    this@listWorkItemAttemptsResponse.workItemAttempts.last().workItemResourceId
                  workItemAttemptResourceId =
                    this@listWorkItemAttemptsResponse.workItemAttempts
                      .last()
                      .workItemAttemptResourceId
                }
            }
          } else {
            this.workItemAttempts += workItemAttempt
          }
        }
      }
    }
  }

  private fun attemptRetryDelay(workItemAttempt: WorkItemAttempt): Duration {
    val exponent = (workItemAttempt.attemptNumber - 1).coerceIn(0, MAX_ATTEMPT_RETRY_EXPONENT)
    return minOf(initialAttemptRetryDelay.multipliedBy(1L shl exponent), maxAttemptRetryDelay)
  }

  private fun expiredLeaseException(result: WorkItemAttemptResult) =
    WorkItemAttemptInvalidStateException(
      result.workItemAttempt.workItemResourceId,
      result.workItemAttempt.workItemAttemptResourceId,
      result.workItemAttempt.state,
      IllegalStateException("WorkItemAttempt lease is absent or expired"),
    )

  companion object {
    private const val MAX_PAGE_SIZE = 100
    private const val DEFAULT_PAGE_SIZE = 50
    private const val INITIAL_GENERATION = 1L
    private const val MAX_ERROR_MESSAGE_LENGTH = 1024
    private const val MAX_ATTEMPT_RETRY_EXPONENT = 16
    val DEFAULT_ATTEMPT_LEASE_DURATION: Duration = Duration.ofMinutes(5)
    val DEFAULT_INITIAL_ATTEMPT_RETRY_DELAY: Duration = Duration.ofSeconds(1)
    val DEFAULT_MAX_ATTEMPT_RETRY_DELAY: Duration = Duration.ofMinutes(1)
  }
}
