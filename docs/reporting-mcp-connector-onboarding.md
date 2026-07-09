# Reporting MCP Connector Onboarding

This guide is for people who want to **use** the Reporting MCP connector from an
MCP-capable client (for example, a Claude client) to explore cross-media
measurement data and create reports in plain language.

If you are setting up the server for an environment, see
[Reporting MCP Server Deployment on GKE](gke/reporting-mcp-server-deployment.md)
instead.

Environment-specific values are shown as placeholders (`<LIKE_THIS>`) — get the
real ones for your environment from your administrator.

## What it is

The connector exposes the Reporting v2alpha public API as a set of MCP tools. A
client that supports MCP connectors can call those tools on your behalf, using
your own credentials — no SQL, API calls, or dashboards required. The server
forwards your OAuth token to the Reporting API, so you only ever see the data
you are authorized to see.

## Prerequisites

-   An MCP-capable client that supports remote connectors with OAuth (for
    example, a recent Claude client).
-   The connector endpoint URL for your environment: `https://<MCP_HOST>/mcp`.
-   Access to a `MeasurementConsumer`. Your administrator must have completed the
    server deployment and granted your account access (see the deployment guide,
    Step 5). Without that grant you can connect but tool calls return a
    permission error.

## Add the connector

Add the connector using your client's connector settings, or from the command
line:

```shell
claude mcp add halo-reporting https://<MCP_HOST>/mcp
```

Use a name that identifies the environment (for example, one per environment if
you connect to more than one).

## Sign in

The first time you use the connector, the client starts an OAuth sign-in in your
browser. Approve the request. The client discovers where to authenticate from
the server's protected-resource metadata and requests the `reporting.*` scope
automatically — you do not need to configure scopes by hand.

Once sign-in succeeds, the client loads the available tools.

## What you can do

The connector provides read and create tools over the core reporting resources:

| Area                          | Tools                                                                 |
| ----------------------------- | --------------------------------------------------------------------- |
| Event groups (campaigns)      | `get_event_group`, `list_event_groups`                                |
| Reporting sets (campaign groups) | `get_reporting_set`, `list_reporting_sets`, `create_reporting_set` |
| Basic reports                 | `get_basic_report`, `list_basic_reports`, `create_basic_report`       |
| Impression qualification filters | `get_impression_qualification_filter`, `list_impression_qualification_filters` |

## Example prompts

Replace `<MEASUREMENT_CONSUMER>` with your resource name (it looks like
`measurementConsumers/<id>`).

-   "For `<MEASUREMENT_CONSUMER>`, list the event groups and show a table of
    brand, campaign, publisher, media types, and data-availability window."
-   "How many distinct publishers and brands is that, and which brand runs
    across the most publishers?"
-   "List the reporting sets (campaign groups) for `<MEASUREMENT_CONSUMER>`."
-   "Create a basic report for campaign group `<REPORTING_SET>` from
    `<START_DATE>` to `<END_DATE>` with reach, impressions, and average
    frequency."
-   "Get the results for basic report `<BASIC_REPORT>`."

Reports compute asynchronously — create one, then poll it with the get tool
until its state is `SUCCEEDED`.

## Troubleshooting

-   **"Authorized, but the server returned an error when connecting."** The
    OAuth sign-in worked but the endpoint is not reachable. The managed
    certificate may still be provisioning after a fresh deploy, or the endpoint
    may not be exposed yet — check with your administrator.
-   **Tool calls return `PERMISSION_DENIED`.** Your account has not been granted
    access to that `MeasurementConsumer` yet, or the requested scope is missing.
    Ask your administrator to complete the access grant (deployment guide,
    Step 3d and Step 5).
-   **Sign-in loops or a tool says the token is missing/expired.** Sign out and
    reconnect the connector so the client mints a fresh token.

## See also

-   [Reporting MCP Server Deployment on GKE](gke/reporting-mcp-server-deployment.md)
