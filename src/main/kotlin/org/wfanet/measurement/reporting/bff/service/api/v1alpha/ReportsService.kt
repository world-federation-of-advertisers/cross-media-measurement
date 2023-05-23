package org.wfanet.measurement.reporting.bff.service.api.v1alpha

import org.wfanet.measurement.reporting.bff.v1alpha.Report
import org.wfanet.measurement.reporting.bff.v1alpha.GetReportRequest
import org.wfanet.measurement.reporting.bff.v1alpha.ListReportsRequest
import org.wfanet.measurement.reporting.bff.v1alpha.ListReportsResponse
import org.wfanet.measurement.reporting.bff.v1alpha.ReportsGrpcKt

class ReportsService(
    private val reportsStub: ReportsGrpcKt.ReportsCoroutineStub
) : ReportsGrpcKt.ReportsCoroutineImplBase() { 
    override suspend fun listReports(request: ListReportsRequest): ListReportsResponse {
        // Map UI request to internal request
        // val resp = reportsStub.listReports("measurementConsumers/VCTqwV_vFXw")
        val resp = reportsStub.listReports(request)

        // Map internal response to UI response
        val results = ListReportsResponse.newBuilder()

        resp.reportsList.forEach{
            val r = Report.newBuilder().setName(it.name).build()
            results.addReports(r)
        }

        val f = results.build()
        return f
    }

    override suspend fun getReport(request: GetReportRequest): Report {
        val resp = reportsStub.getReport(request)

        val result = Report.newBuilder()
            .setName(resp.name)
            .build()
        return result
    }
}