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

import io.grpc.BindableService
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
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig

/**
 * Extracts a name from the gRPC [Metadata] and adds it to the gRPC [Context].
 *
 * To install, wrap a service with:
 * ```
 *   yourService.withReportScheduleNameInterceptor()
 * ```
 *
 * The principal can be accessed within gRPC services via [principalFromCurrentContext].
 *
 * This expects the Metadata to have a key "report_schedule" associated with a value equal to the
 * resource name of the ReportSchedule. The recommended way to set this is to use
 * [withReportScheduleName] on a stub.
 */
class ReportScheduleNameServerInterceptor() : ServerInterceptor {
  override fun <ReqT, RespT> interceptCall(
    call: ServerCall<ReqT, RespT>,
    headers: Metadata,
    next: ServerCallHandler<ReqT, RespT>
  ): ServerCall.Listener<ReqT> {
    val reportScheduleName =
      headers[REPORT_SCHEDULE_NAME_METADATA_KEY]
        ?: return Contexts.interceptCall(Context.current(), call, headers, next)

    val context = Context.current().withReportSchedule(reportScheduleName)
    return Contexts.interceptCall(context, call, headers, next)
  }

  companion object {
    private const val KEY_NAME = "report_schedule"
    private val REPORT_SCHEDULE_NAME_METADATA_KEY: Metadata.Key<String> =
      Metadata.Key.of(KEY_NAME, Metadata.ASCII_STRING_MARSHALLER)

    /**
     * Sets metadata key "report_schedule" on all outgoing requests. On the server side, use
     * [ReportScheduleNameServerInterceptor].
     */
    fun <T : AbstractStub<T>> T.withReportScheduleName(name: String): T {
      val extraHeaders = Metadata()
      extraHeaders.put(REPORT_SCHEDULE_NAME_METADATA_KEY, name)
      return withInterceptors(MetadataUtils.newAttachHeadersInterceptor(extraHeaders))
    }

    /** Installs [ReportScheduleNameServerInterceptor] on the service. */
    fun BindableService.withReportScheduleNameInterceptor(): ServerServiceDefinition =
      ServerInterceptors.interceptForward(this, ReportScheduleNameServerInterceptor())

    /**
     * Returns a resource name in the current gRPC context. Requires
     * [ReportScheduleNameServerInterceptor] to be installed.
     */
    val reportScheduleNameFromCurrentContext: String?
      get() = ContextKeys.REPORT_SCHEDULE_NAME_CONTEXT_KEY.get()

    /** Executes [block] with the resource name installed in a new [Context]. */
    fun <T> withReportSchedule(name: String, block: () -> T): T {
      return Context.current().withReportSchedule(name).call(block)
    }

    /**
     * Executes [block] with the resource name and [MeasurementConsumerPrincipal] installed in a new
     * [Context].
     */
    fun <T> withReportScheduleAndMeasurementConsumerPrincipal(
      name: String,
      measurementConsumerName: String,
      config: MeasurementConsumerConfig,
      block: () -> T
    ): T {
      val measurementConsumerPrincipal =
        MeasurementConsumerPrincipal(
          MeasurementConsumerKey.fromName(measurementConsumerName)!!,
          config
        )

      return Context.current()
        .withPrincipal(measurementConsumerPrincipal)
        .withReportSchedule(name)
        .call(block)
    }

    /** Adds the resource name to the receiver and returns the new [Context]. */
    private fun Context.withReportSchedule(name: String): Context {
      return withValue(ContextKeys.REPORT_SCHEDULE_NAME_CONTEXT_KEY, name)
    }
  }
}
