package org.wfanet.measurement.reporting.bff.service.api.v1alpha

import org.wfanet.measurement.reporting.bff.v1alpha.Report
import org.wfanet.measurement.reporting.bff.v1alpha.report
import org.wfanet.measurement.reporting.bff.v1alpha.GetReportRequest
import org.wfanet.measurement.reporting.bff.v1alpha.ListReportsRequest
import org.wfanet.measurement.reporting.bff.v1alpha.ListReportsResponse
import org.wfanet.measurement.reporting.bff.v1alpha.listReportsResponse
import org.wfanet.measurement.reporting.bff.v1alpha.ReportsGrpcKt
import org.wfanet.measurement.reporting.v1alpha.report as haloReport
import org.wfanet.measurement.reporting.v1alpha.getReportRequest
import org.wfanet.measurement.reporting.v1alpha.ReportsGrpcKt as HaloReportsGrpcKt
import org.wfanet.measurement.reporting.v1alpha.listReportsRequest

class ReportsService(
    private val haloReportsStub: HaloReportsGrpcKt.ReportsCoroutineStub
) : ReportsGrpcKt.ReportsCoroutineImplBase() { 
    override suspend fun listReports(request: ListReportsRequest): ListReportsResponse {
        // Map UI request to internal request
        val haloRequest = listReportsRequest { parent = request.parent }
        val resp = haloReportsStub.listReports(haloRequest)

        // Map internal response to UI response
        val results = listReportsResponse {
            nextPageToken = resp.nextPageToken

            resp.reportsList.forEach{
                val r = report { name = it.name }
                reports += r
            }
        }

        return results
    }

    override suspend fun getReport(request: GetReportRequest): Report {
        val haloRequest = getReportRequest { name = request.name }
        val resp = haloReportsStub.getReport(haloRequest)

        val result = report {
            name = resp.name
        }
        return result
    }
}