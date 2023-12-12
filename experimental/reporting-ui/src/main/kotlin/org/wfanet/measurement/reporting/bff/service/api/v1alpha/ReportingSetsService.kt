class ReportingSetsService(private val backendReportingSetsStub: BackendReportingSetsGrpcKt.ReportsCoroutineStub) :
  ReportsGrpcKt.ReportsCoroutineImplBase() {
  suspend fun createReportingSet(request: CreateReportingSetRequest): CreateReportingSetResponse {
    return CreateReportingSetResponse{}
  }
}

class CreateReportingSetRequest {

}

class CreateReportingSetResponse {

}