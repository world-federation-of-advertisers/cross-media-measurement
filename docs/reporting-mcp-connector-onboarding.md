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

Point your MCP client at the connector endpoint (`https://<MCP_HOST>/mcp`) and
complete the OAuth sign-in. Use a name that identifies the environment if you
connect to more than one.

### Claude Code (CLI)

```shell
claude mcp add --transport http halo-reporting https://<MCP_HOST>/mcp
```

The `--transport http` flag is required for a remote server.

### Claude Desktop / Claude.ai (web)

Settings &rarr; Connectors &rarr; Add custom connector &rarr; paste
`https://<MCP_HOST>/mcp` &rarr; Add, then complete the OAuth sign-in in the
browser.

### Other MCP clients

Any client that supports the Streamable HTTP transport can connect — point it at
`https://<MCP_HOST>/mcp`. The server advertises OAuth Protected Resource
Metadata at `/.well-known/oauth-protected-resource`, so clients discover where to
authenticate automatically.

### Server-to-server (machine-to-machine)

A backend service that runs without a browser uses the OAuth Client Credentials
grant instead of the interactive sign-in: request a token from the provider's
token endpoint with `grant_type=client_credentials`, the reporting API audience,
and the `reporting.*` scope, then send it as `Authorization: Bearer <token>` on
requests to `https://<MCP_HOST>/mcp`. The service's own identity still needs an
access grant — see the deployment guide's server-to-server section.

## Sign in

The first time you use the connector, the client starts an OAuth sign-in in your
browser. Approve the request. The client discovers where to authenticate from
the server's protected-resource metadata and requests the `reporting.*` scope
automatically — you do not need to configure scopes by hand.

Once sign-in succeeds, the client loads the available tools.

## Verify your connection

Confirm the client shows the connector loaded with its tools and prompts (10
tools and 4 prompts). Then try a read-only call:

-   "List the event groups for `<MEASUREMENT_CONSUMER>`."

If it returns data, authentication and access are working. If it returns
`PERMISSION_DENIED`, your account still needs an access grant — ask your
administrator (deployment guide, Step 5).

## What you can do

The connector provides read and create tools over the core reporting resources:

| Area                          | Tools                                                                 |
| ----------------------------- | --------------------------------------------------------------------- |
| Event groups (campaigns)      | `get_event_group`, `list_event_groups`                                |
| Reporting sets (campaign groups) | `get_reporting_set`, `list_reporting_sets`, `create_reporting_set` |
| Basic reports                 | `get_basic_report`, `list_basic_reports`, `create_basic_report`       |
| Impression qualification filters | `get_impression_qualification_filter`, `list_impression_qualification_filters` |

## Guided prompts

The connector also provides **prompts** — guided workflows that call the tools in
the right order. In clients that support them, they appear as slash commands.

| Prompt                   | What it does                                                          |
| ------------------------ | -------------------------------------------------------------------- |
| Explore Reporting Data   | Discover the event groups, reporting sets, and impression qualification filters available before building a report |
| Create Basic Report      | End to end: pick a campaign group, choose a date range, create the report, and poll until it is done |
| Interpret Basic Report   | Read a completed report for a media-planning audience, with headline metrics, tables, and charts |
| Compare Campaigns        | Compare reach and frequency across recent campaigns                  |

Using a prompt is often easier than calling the tools one by one.

## Example prompts

Replace `<MEASUREMENT_CONSUMER>` with your resource name (it looks like
`measurementConsumers/<id>`). If you connect to more than one environment, use
the `MeasurementConsumer` that belongs to the environment this connector points
at — each environment has its own, and using another environment's ID returns a
permission or not-found error.

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
    reconnect the connector so the client mints a fresh token. If your
    administrator recently changed the OIDC or audience configuration, remove and
    re-add the connector — a cached token keeps the old audience and will keep
    failing until it is reissued.

## See also

-   [Reporting MCP Server Deployment on GKE](gke/reporting-mcp-server-deployment.md)
