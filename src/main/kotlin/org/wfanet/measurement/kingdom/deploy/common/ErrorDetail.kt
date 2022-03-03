// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.deploy.common

import com.google.rpc.ErrorInfo
import io.grpc.Metadata
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.ProtoUtils
import org.wfanet.measurement.internal.kingdom.ErrorDetail

fun failGrpcWithDetail(
  status: Status = Status.INVALID_ARGUMENT,
  code: ErrorDetail.ErrorCode,
  domain: String,
  extraInfo: Map<String, String> = emptyMap(),
  provideDescription: () -> String,
): Nothing {

  val info =
    ErrorInfo.newBuilder()
      .also {
        it.reason = code.toString()
        it.domain = domain
        it.putAllMetadata(extraInfo)
      }
      .build()
  val detail =
    ErrorDetail.newBuilder()
      .also {
        it.code = code
        it.info = info
      }
      .build()

  val metadata = Metadata()
  metadata.put(ProtoUtils.keyForProto(detail), detail)

  throw status.withDescription(provideDescription()).asRuntimeException(metadata)
}

fun getErrorDetail(error: StatusRuntimeException): ErrorDetail? {
  val key = ProtoUtils.keyForProto(ErrorDetail.getDefaultInstance())
  return error.trailers?.get(key)
}
