# Reporting MCP Server Deployment on GKE

This guide covers standing up the Reporting MCP server as an externally
reachable, OAuth-protected endpoint in an environment, end to end. It is meant
to be repeatable by any operator setting up a new environment.

The MCP server is a thin HTTP-to-gRPC front end for the Reporting v2alpha public
API: it exposes the API as a set of Model Context Protocol (MCP) tools and
forwards the caller's OAuth bearer token, unchanged, to the Reporting API. It
runs alongside the other reporting components (see
[Reporting V2 Server Deployment](reporting-v2-server-deployment.md)).

Values that are specific to your environment are shown as placeholders
(`<LIKE_THIS>`). Configuration that the deploy already reads from a variable is
referred to by the variable name (for example `MCP_HOST`).

## How authorization works

A client (for example an MCP-capable Claude client) signs in through the OpenID
Connect (OIDC) provider and receives an access token. The MCP server forwards
that token to the Reporting API, which applies **two independent checks, in
order**:

1.  **Scope check** — the token's `scope` claim must contain the permission
    being exercised, or a matching wildcard such as `reporting.*`. This is
    checked first.
2.  **Access check** — the caller's identity (issuer + subject) must be a
    registered principal in the Access service, bound to a role that grants the
    permission on the target `MeasurementConsumer`.

Both must pass. Most setup problems are one of these two checks failing quietly;
see [Troubleshooting](#troubleshooting).

## Prerequisites

-   The reporting stack (Kingdom, Reporting API, Access service) is already
    deployed in the target environment.
-   You have admin access to:
    -   the OIDC provider (for example, an Auth0 tenant),
    -   the repository's GitHub Actions **environment variables**,
    -   DNS for the MCP hostname,
    -   the Google Cloud project (to reserve a static IP),
    -   the Access service (a client mTLS certificate trusted by its
        client-auth CA).

## Step 1 — Reserve a static IP and DNS record

The MCP server is exposed through a GKE Ingress, which needs a stable global
address.

1.  Reserve a **global** external IP for the Ingress. In Terraform this is a
    `google_compute_global_address` (a regional address will not work for a
    global Ingress). If the address is pre-created out of band, adopt it into
    Terraform state with an `import` block.
2.  Create a DNS `A` record for your MCP hostname pointing at that IP:
    `<MCP_HOST>` &rarr; the reserved IP.
3.  Do this **before** the first deploy. The Google-managed certificate can only
    provision once the hostname resolves, so DNS must exist first.

## Step 2 — Set the deploy configuration

Set the following GitHub Actions **environment variables** for the target
environment. The deploy (CUE tags plus the `configure-reporting-v2` workflow)
reads them and renders the Ingress, managed certificate, service NEG annotation,
and the MCP server's OAuth flags. External exposure is gated on **both**
`MCP_HOST` and the OIDC issuer being set, so a partial configuration will not
create a public endpoint without OAuth.

| Variable                   | Scope                         | Example value                         |
| -------------------------- | ----------------------------- | ------------------------------------- |
| `MCP_HOST`                 | per-environment               | `<MCP_HOST>`                          |
| `REPORTING_TOKEN_AUDIENCE` | per-environment               | `<REPORTING_API_AUDIENCE>`            |
| `OPEN_ID_PROVIDER_ISSUER`  | shared, or per-environment    | `<OIDC_ISSUER>`                       |

If one OIDC tenant serves multiple environments, `OPEN_ID_PROVIDER_ISSUER` can
be a single shared (repository-level) variable while each environment keeps its
own `REPORTING_TOKEN_AUDIENCE`.

## Step 3 — Configure the OIDC provider

Do this once per environment, on the API (audience) that represents the
Reporting API for that environment. The steps below use Auth0 terminology; adapt
as needed for another provider.

### 3a. The API (audience)

Ensure an API exists whose identifier equals `<REPORTING_API_AUDIENCE>` (this is
the value clients request as the audience and that the Reporting API validates).

### 3b. Access-token format

-   Set the **JWT Profile** to **RFC 9068**. The Reporting API's token validator
    requires the `at+jwt` token type header; the RFC 9068 profile emits it. A
    provider's default profile typically emits `typ: JWT`, which is rejected.
-   Set the **signing algorithm** to **RS256**.

### 3c. Define the scope

Add a permission (scope) named **`reporting.*`** on the API. The Reporting API's
scope check treats `reporting.*` as covering every `reporting.` permission, so
this single entry is sufficient for all tools.

### 3d. Authorize the client application for the scope

This step is easy to miss. If the API uses per-application authorization, grant
the client application the **`reporting.*`** permission for **user-delegated
access**. Without this grant the provider silently drops the requested scope and
issues a token with an empty `scope` claim — the tools then fail with
`PERMISSION_DENIED` even though everything else is correct.

### 3e. RBAC

Leave role-based access control **off** for this API. With the per-application
grant (3d) and the client requesting the scope (Step 4), the scope flows into
the token's `scope` claim, which is what the Reporting API reads. If your tenant
policy requires RBAC, additionally assign `reporting.*` to each user; note that
provider RBAC may populate a separate `permissions` claim, which the Reporting
API does not read — the value must end up in `scope`.

### 3f. Audience delivery across environments

A single OIDC tenant has one default audience, but each environment needs a
different audience (`<REPORTING_API_AUDIENCE>`). Either have the client request
the per-environment audience explicitly (via the resource indicator / the MCP
server's advertised protected resource), or use a separate tenant per
environment. Decide this per deployment.

### 3g. The client application

The connector uses a public (PKCE, no client secret) application. It may be
created through Dynamic Client Registration or manually; either way it must be
authorized for the API as in 3d.

## Step 4 — MCP server OAuth flags

No manual action is needed beyond Step 2. When `MCP_HOST` and the issuer are
set, the deploy renders these arguments on the MCP server. They drive OAuth
discovery and tell the client which scope to request:

```
--oauth-protected-resource=https://<MCP_HOST>
--oauth-authorization-server=<OIDC_ISSUER>
--oauth-scope=reporting.*
```

## Step 5 — Register the principal and bind a role

This satisfies the second authorization check. Each user's OIDC identity must be
a registered principal in the Access service, bound to a reporting role on the
target `MeasurementConsumer`. Use the Access CLI with a client mTLS certificate
trusted by the Access server's client-auth CA.

```shell
# Create the principal for the user's OIDC identity.
Access principals create \
  --issuer=<OIDC_ISSUER> \
  --subject=<USER_SUBJECT> \
  --principal-id=<PRINCIPAL_ID>

# Add the principal to a reporting role on the MeasurementConsumer's policy.
Access policies add-members \
  --name=<POLICY_NAME> \
  --binding-role=roles/<REPORTING_ROLE> \
  --binding-member=principals/<PRINCIPAL_ID> \
  --etag=<CURRENT_ETAG>
```

This is per-user: the Access model has no group- or tenant-wide grant, so every
user who needs access gets their own principal and binding.

## Step 6 — Verify

1.  Sign in through the connector.
2.  Decode the issued access token and confirm `iss`, `sub`, and `aud` are
    correct **and** that `scope` contains `reporting.*`.
3.  Call a tool (for example, list event groups) and confirm it returns data.

## Troubleshooting

| Symptom (error)                                        | Likely cause                                             | Fix     |
| ------------------------------------------------------ | ------------------------------------------------------- | ------- |
| `UNAUTHENTICATED: Principal not found`                 | User not registered in the Access service               | Step 5  |
| `UNAUTHENTICATED: expected type header at+jwt`         | JWT profile is not RFC 9068                              | Step 3b |
| `UNAUTHENTICATED: Token is not a valid signed JWT`     | Wrong or missing audience requested                     | Step 3f |
| `PERMISSION_DENIED`, token has **no** `scope` claim    | Client app not granted the scope (per-app authorization) | Step 3d |
| `PERMISSION_DENIED`, token **has** the `reporting.*` scope | Reporting role not bound to the principal            | Step 5  |
| Connector fails to connect, endpoint unreachable       | Managed certificate still provisioning, or exposure not rendered (both `MCP_HOST` and the issuer must be set) | Steps 1–2 |
| Public endpoint stands up without OAuth                | Exposure gated on both `MCP_HOST` and the issuer; one is missing | Step 2  |

## See also

-   [Reporting V2 Server Deployment](reporting-v2-server-deployment.md)
-   [Reporting MCP Connector Onboarding](../reporting-mcp-connector-onboarding.md)
