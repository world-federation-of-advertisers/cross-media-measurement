package org.wfanet.measurement.reporting.service.api.v1alpha

import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.principalFromCurrentContext
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v1alpha.CreateReportingSetRequest
import org.wfanet.measurement.reporting.v1alpha.ReportingSet
import org.wfanet.measurement.reporting.v1alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase

private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 100

class ReportingSetsService(private val internalReportingSetsStub: ReportingSetsCoroutineStub):
  ReportingSetsCoroutineImplBase() {
  override suspend fun createReportingSet(request: CreateReportingSetRequest): ReportingSet {
    val parentKey = grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
      "Parent is either unspecified or invalid"
    }

    val principal = principalFromCurrentContext

    grpcRequire(request.parent == principal.resourceKey.toName()) {
      "Cannot create ReportingSet for another MeasurementConsumer."
    }

    grpcRequire(request.hasReportingSet()) {
      ""
    }



    return super.createReportingSet(request)
  }
}
