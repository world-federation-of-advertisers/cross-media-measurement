package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner

import com.google.protobuf.Empty
import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.internal.reporting.v2.AddNoisyResultValuesRequest
import org.wfanet.measurement.internal.reporting.v2.ReportResultsGrpcKt
import org.wfanet.measurement.reporting.service.internal.RequiredFieldNotSetException

/**
 * Spanner implementation of ReportResults gRPC service.
 */
class SpannerReportResultsService(coroutineContext: CoroutineContext = EmptyCoroutineContext) : ReportResultsGrpcKt.ReportResultsCoroutineImplBase(coroutineContext) {
  override suspend fun addNoisyResultValues(request: AddNoisyResultValuesRequest): Empty {
    if (!request.hasReportResult()) {
      throw RequiredFieldNotSetException("report_result").asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
    }
    return super.addNoisyResultValues(request)
  }
}
