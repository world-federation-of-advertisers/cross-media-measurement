import { GetReport } from "./views/getReport/GetReport";
import { ListReports } from "./views/listReports/ListReports";
import ErrorPage from "./views/error";

type route = {
    path: string,
    element: React.JSX.Element,
    errorElement?: React.JSX.Element,
}

export const routes: route[] = [
    {
        path: "/",
        element: <ListReports />,
        errorElement: <ErrorPage />,
    },
    {
        path: "/reports/:reportId",
        element: <GetReport />
    }
];
