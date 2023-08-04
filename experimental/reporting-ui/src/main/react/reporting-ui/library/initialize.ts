import { ReportingClient } from "./reporting_client/reporting_client"
import { ReportingClientImpl } from "./reporting_client/reporting_client_impl";

type ConfigProps = {
  reportingApi: ReportingClient,
}

class AppConfig {
  reportingApi?: ReportingClient;

  initialize(props:ConfigProps) {
    this.reportingApi = new ReportingClientImpl(props.reportingApi);
  }
}

const appConfig = new AppConfig();
export default appConfig;
