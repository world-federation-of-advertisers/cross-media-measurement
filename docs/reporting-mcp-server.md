# Reporting MCP Server

The Reporting MCP server exposes a subset of the Reporting `v2alpha` public API
to AI agents via the [Model Context Protocol](https://modelcontextprotocol.io)
over Streamable HTTP. It is a thin, **stateless** front end: it forwards each
request's bearer token to the Reporting API and returns the API's Proto-JSON
responses. It provides MCP **tools** (read and create operations on event
groups, reporting sets, basic reports, and impression qualification filters) and
**prompts** (guided reporting workflows).

## Authentication

The server authenticates every request with an OAuth 2.0 bearer token and
forwards it **unchanged** to the Reporting API (bearer passthrough). The MCP
server does not mint, store, or validate tokens itself — the Reporting API
validates them — so any token the Reporting API accepts is accepted here.

### Obtaining a token (OAuth 2.0 client credentials)

Service-to-service callers authenticate with the OAuth 2.0
[client-credentials grant](https://datatracker.ietf.org/doc/html/rfc6749#section-4.4).
Exchange your client credentials for an access token at your OAuth provider's
token endpoint:

```
POST <token-endpoint>
Content-Type: application/json

{
  "client_id": "<client-id>",
  "client_secret": "<client-secret>",
  "audience": "<reporting-api-audience>",
  "grant_type": "client_credentials"
}
```

The response contains an `access_token`. The `audience` you request, and the
token's issuer, must match what the Reporting API deployment is configured to
accept; otherwise requests fail with `UNAUTHENTICATED`. Obtain the token
endpoint, audience, and credentials from your deployment's configuration — they
are environment-specific and are not listed here.

### Presenting the token

Send the token in the `Authorization` header on every request:

```
Authorization: Bearer <access_token>
```

An MCP client that speaks Streamable HTTP registers the server with that header.
For example, with a coding agent:

```shell
mcp add --transport http reporting <mcp-server-url> \
  --header "Authorization: Bearer <access_token>"
```

### Token lifetime and refresh

Access tokens are short-lived. Because the server is stateless and reads the
bearer token from **each** request (it holds no per-session token), a
long-running session must present a fresh token as the old one expires:
re-acquire a token and re-send it (re-register the header). An expired, missing,
or otherwise invalid token surfaces as a clear authentication error from the
tool call rather than a transport failure.

## Security

*   Never commit access tokens or client secrets to source control.
*   Scope client credentials to the minimum the workflow requires.
*   Transmit tokens only over TLS, and do not log them.
