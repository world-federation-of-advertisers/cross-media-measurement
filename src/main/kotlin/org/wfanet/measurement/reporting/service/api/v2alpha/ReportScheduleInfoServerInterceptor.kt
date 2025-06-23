/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.gson.GsonBuilder
import com.google.protobuf.Timestamp
import io.grpc.Context
import io.grpc.Contexts
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.ServerInterceptors
import io.grpc.ServerServiceDefinition
import io.grpc.stub.AbstractStub
import io.grpc.stub.MetadataUtils
import org.wfanet.measurement.common.grpc.withContext

/**
 * Extracts a name from the gRPC [Metadata] and adds it to the gRPC [Context].
 *
 * To install, wrap a service with:
 * ```
 *   yourService.withReportScheduleNameInterceptor()
 * ```
 *
 * This expects the Metadata to have a key "report-schedule-info-bin" associated with a value equal
 * to the binary serialization of a [ReportScheduleInfo] for a ReportSchedule. The recommended way
 * to set this is to use [withReportScheduleInfo] on a stub.
 */
class ReportScheduleInfoServerInterceptor : ServerInterceptor {
  data class ReportScheduleInfo(val name: String, val nextReportCreationTime: Timestamp)

  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>,
  ): ServerCall.Listener<ReqT> {
    val gsonBuilder = GsonBuilder()
    val gson = gsonBuilder.create()

    val reportScheduleInfo =
      gson.fromJson(
        (headers[REPORT_SCHEDULE_INFO_METADATA_KEY]
            ?: return Contexts.interceptCall(Context.current(), call, headers, next))
          .decodeToString(),
        ReportScheduleInfo::class.java,
      )

    val context = Context.current().withReportScheduleInfo(reportScheduleInfo)
    return Contexts.interceptCall(context, call, headers, next)
  }

  companion object {
    private const val KEY_NAME = "report-schedule-info-bin"
    private val REPORT_SCHEDULE_INFO_CONTEXT_KEY: Context.Key<ReportScheduleInfo> =
      Context.key(KEY_NAME)
    private val REPORT_SCHEDULE_INFO_METADATA_KEY: Metadata.Key<ByteArray> =
      Metadata.Key.of(KEY_NAME, Metadata.BINARY_BYTE_MARSHALLER)

    /**
     * Sets metadata key "report-schedule-info-bin" on all outgoing requests. On the server side,
     * use [ReportScheduleInfoServerInterceptor].
     */
    fun <T : AbstractStub<T>> T.withReportScheduleInfo(info: ReportScheduleInfo): T {
      val extraHeaders = Metadata()
      val gsonBuilder = GsonBuilder()
      val gson = gsonBuilder.create()
      extraHeaders.put(REPORT_SCHEDULE_INFO_METADATA_KEY, gson.toJson(info).toByteArray())
      return withInterceptors(MetadataUtils.newAttachHeadersInterceptor(extraHeaders))
    }

    /** Installs [ReportScheduleInfoServerInterceptor] on the service. */
    fun ServerServiceDefinition.withReportScheduleInfoInterceptor(): ServerServiceDefinition =
      ServerInterceptors.interceptForward(this, ReportScheduleInfoServerInterceptor())

    /**
     * Returns a [ReportScheduleInfo] in the current gRPC context. Requires
     * [ReportScheduleInfoServerInterceptor] to be installed.
     */
    val reportScheduleInfoFromCurrentContext: ReportScheduleInfo?
      get() = REPORT_SCHEDULE_INFO_CONTEXT_KEY.get()

    /** Executes [block] with the resource name installed in a new [Context]. */
    inline fun <T> withReportScheduleInfo(info: ReportScheduleInfo, block: () -> T): T {
      return withContext(Context.current().withReportScheduleInfo(info)) { block() }
    }

    /** Adds the [ReportScheduleInfo] to the receiver and returns the new [Context]. */
    fun Context.withReportScheduleInfo(info: ReportScheduleInfo): Context {
      return withValue(REPORT_SCHEDULE_INFO_CONTEXT_KEY, info)
    }
  }
}
