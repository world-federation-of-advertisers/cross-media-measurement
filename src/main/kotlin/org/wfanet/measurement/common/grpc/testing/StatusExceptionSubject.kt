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

package org.wfanet.measurement.common.grpc.testing

import com.google.common.truth.ComparableSubject
import com.google.common.truth.FailureMetadata
import com.google.common.truth.Subject
import com.google.common.truth.ThrowableSubject
import com.google.common.truth.Truth
import com.google.common.truth.extensions.proto.ProtoSubject
import com.google.common.truth.extensions.proto.ProtoTruth
import com.google.rpc.ErrorInfo
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import org.wfanet.measurement.common.grpc.errorInfo

/**
 * Truth [Subject] for [StatusException] or [StatusRuntimeException].
 *
 * This includes more context in failure messages than less-specific subjects.
 */
class StatusExceptionSubject
private constructor(metadata: FailureMetadata, private val actual: StatusExceptionAdapter?) :
  ThrowableSubject(metadata, actual?.delegate) {
  constructor(
    metadata: FailureMetadata,
    actual: StatusException?,
  ) : this(metadata, actual?.let { StatusExceptionAdapter(actual) })

  constructor(
    metadata: FailureMetadata,
    actual: StatusRuntimeException?,
  ) : this(metadata, actual?.let { StatusExceptionAdapter(actual) })

  fun status(): StatusSubject {
    val status = checkNotNull(actual).status
    return check("status")
      .withMessage("errorInfo: ${actual.errorInfo}")
      .about(StatusSubject.Factory)
      .that(status)
  }

  fun errorInfo(): ProtoSubject {
    return check("errorInfo").about(ProtoTruth.protos()).that(checkNotNull(actual).errorInfo)
  }

  private class StatusExceptionAdapter
  private constructor(val delegate: Exception, val status: Status, val errorInfo: ErrorInfo?) {
    constructor(e: StatusException) : this(e, e.status, e.errorInfo)

    constructor(e: StatusRuntimeException) : this(e, e.status, e.errorInfo)
  }

  object Factory : Subject.Factory<StatusExceptionSubject, StatusException> {
    override fun createSubject(
      metadata: FailureMetadata,
      actual: StatusException?,
    ): StatusExceptionSubject = StatusExceptionSubject(metadata, actual)
  }

  object RuntimeFactory : Subject.Factory<StatusExceptionSubject, StatusRuntimeException> {
    override fun createSubject(
      metadata: FailureMetadata,
      actual: StatusRuntimeException?,
    ): StatusExceptionSubject = StatusExceptionSubject(metadata, actual)
  }

  companion object {
    fun assertThat(actual: StatusException?) = Truth.assertAbout(Factory).that(actual)

    fun assertThat(actual: StatusRuntimeException?) = Truth.assertAbout(RuntimeFactory).that(actual)
  }
}

class StatusSubject(metadata: FailureMetadata, private val actual: Status?) :
  Subject(metadata, actual) {
  fun code(): ComparableSubject<Status.Code> {
    checkNotNull(actual)
    return check("code").withMessage("status: $actual").that(actual.code)
  }

  companion object Factory : Subject.Factory<StatusSubject, Status> {
    override fun createSubject(metadata: FailureMetadata, actual: Status?): StatusSubject =
      StatusSubject(metadata, actual)

    fun assertThat(actual: Status?) = Truth.assertAbout(this)
  }
}
