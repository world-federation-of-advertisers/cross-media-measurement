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

package org.wfanet.measurement.common.grpc

import com.google.rpc.ErrorInfo
import io.grpc.Metadata
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto

/** [ErrorInfo] from status details. */
val StatusException.errorInfo: ErrorInfo?
  get() {
    return getErrorInfo(this.status, this.trailers)
  }
val StatusRuntimeException.errorInfo: ErrorInfo?
  get() {
    return getErrorInfo(this.status, this.trailers)
  }

private fun getErrorInfo(status: Status, trailers: Metadata?): ErrorInfo? {
  val errorInfoFullName = ErrorInfo.getDescriptor().fullName
  val errorInfoPacked =
    StatusProto.fromStatusAndTrailers(status, trailers).detailsList.find {
      it.typeUrl.endsWith("/$errorInfoFullName")
    }
  return errorInfoPacked?.unpack(ErrorInfo::class.java)
}
