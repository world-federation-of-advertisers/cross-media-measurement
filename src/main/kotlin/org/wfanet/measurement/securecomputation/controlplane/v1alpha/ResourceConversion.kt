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

package org.wfanet.measurement.securecomputation.controlplane.v1alpha

import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItem as InternalWorkItem
import org.wfanet.measurement.internal.securecomputation.controlplane.WorkItemAttempt as InternalWorkItemAttempt
import org.wfanet.measurement.securecomputation.service.WorkItemAttemptKey
import org.wfanet.measurement.securecomputation.service.WorkItemKey

/** Converts an internal [InternalWorkItem.State] to a public [WorkItem.State]. */
fun InternalWorkItem.State.toWorkItemState(): WorkItem.State =
  when (this) {
    InternalWorkItem.State.QUEUED -> WorkItem.State.QUEUED
    InternalWorkItem.State.RUNNING -> WorkItem.State.RUNNING
    InternalWorkItem.State.SUCCEEDED -> WorkItem.State.SUCCEEDED
    InternalWorkItem.State.FAILED -> WorkItem.State.FAILED
    InternalWorkItem.State.UNRECOGNIZED,
    InternalWorkItem.State.STATE_UNSPECIFIED -> WorkItem.State.STATE_UNSPECIFIED
  }

/** Converts an internal [InternalWorkItemAttempt.State] to a public [WorkItemAttempt.State]. */
fun InternalWorkItemAttempt.State.toWorkItemAttemptState(): WorkItemAttempt.State =
  when (this) {
    InternalWorkItemAttempt.State.ACTIVE -> WorkItemAttempt.State.ACTIVE
    InternalWorkItemAttempt.State.SUCCEEDED -> WorkItemAttempt.State.SUCCEEDED
    InternalWorkItemAttempt.State.FAILED -> WorkItemAttempt.State.FAILED
    InternalWorkItemAttempt.State.UNRECOGNIZED,
    InternalWorkItemAttempt.State.STATE_UNSPECIFIED -> WorkItemAttempt.State.STATE_UNSPECIFIED
  }

fun InternalWorkItem.toWorkItem(): WorkItem {
  val source = this
  return workItem {
    name = WorkItemKey(source.workItemResourceId).toName()
    queue = source.queueResourceId
    state = source.state.toWorkItemState()
    createTime = source.createTime
    updateTime = source.updateTime
  }
}

fun InternalWorkItemAttempt.toWorkItemAttempt(): WorkItemAttempt {
  val source = this
  return workItemAttempt {
    name = WorkItemAttemptKey(source.workItemResourceId, source.workItemAttemptResourceId).toName()
    state = source.state.toWorkItemAttemptState()
    attemptNumber = source.attemptNumber
    errorMessage = source.errorMessage
    createTime = source.createTime
    updateTime = source.updateTime
  }
}
