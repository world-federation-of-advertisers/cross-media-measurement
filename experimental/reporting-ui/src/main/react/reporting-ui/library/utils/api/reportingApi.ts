import { memoizePromiseFn } from "./memoize";
import { InitApiProps, ListReportsResponse, GetReportRequest, GetReportResponse, Report } from "./models";
import { ReportApi } from "./reportApi";

export class ReportingApi{
    reports: Report[] = [];

    constructor(private api: ReportApi) {}

    init(props: InitApiProps): void {
        this.api.init(props);
    }

    listReports(): Promise<ListReportsResponse> {
        return this.api.listReports();
    }

    getReport(req: GetReportRequest): Promise<GetReportResponse> {
        const getCached = memoizePromiseFn(this.api.getReport(req));

        return getCached(req, this.reports);    }

    createReport(): Promise<void> {
        throw new Error("Method not implemented.");
    }
}
