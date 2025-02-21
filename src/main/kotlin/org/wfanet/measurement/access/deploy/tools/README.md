# Public API Tools

Command-line interface (CLI) tools for Cross-Media Access public API.

The examples assume that you have built the relevant target, which outputs to
`bazel-bin` by default. For brevity, the examples do not include the full path
to the executable.

Run the `help` subcommand for usage information.

### Authenticating to the Access API Server

  ```shell
  Access \
  --tls-cert-file=secretfiles/reporting_tls.pem \
  --tls-key-file=secretfiles/reporting_tls.key \
  --cert-collection-file=secretfiles/reporting_root.pem \
  --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
  sub-command
  ```

Arguments:

`--access-public-api-target`: specify the public API target.

`--access-public-api-cert-host`: In the event that the host you specify to the `--access-public-api-target`
option doesn't match what is in the Subject Alternative Name (SAN) extension of
the server's certificate, this option specifies a host that does match using
the `--access-public-api-cert-host` option.

`--tls-cert-file`: TLS client certificate. The issuer of this certificate must be
trusted by the Access server, i.e. the issuer certificate must be in that
server's trusted certificate collection file.

`--tls-key-file`: TLS client key.

### Commands

#### `principals`

* `get`

  Get a Principal

  Pre-conditions: Principal exists.

  Usage:
  ```shell
  Access principals get <principalName>
  ```

  Arguments:

  `<principalName>`: API resource name of the Principal

* `create`

  Create a Principal that is either an OAuth user or Tls client.

  Pre-conditions: Principal does not exist.

  Usage:
  ```shell
  Access principals create
  --principal-id=<id>
  ((--issuer=<issuer> --subject=<subject>) | [--principal-tls-client-cert-file=<tlsCertFile>])
  ```

  Arguments:

  `--principal-id`: Resource ID of the Principal

  `--issuer`: OAuth issuer identifier

  `--subject`: OAuth subject identifier

  `--principal-tls-client-cert-file`: Path to TLS client certificate belonging to Principal

* `delete`

  Delete a Principal using a Principal resource name (This also remove the Principal from all Policy bindings).

  Pre-conditions: Principal exists, and Principal type is OAuth user.

  Usage:
  ```shell
  Access principals delete <principalName>
  ```

  Arguments:

  `<principalName>`: API resource name of the Principal

* `lookup`

  Lookup a Principal using an OAuth user or Tls client.

  Pre-conditions: Principal exists.

  Usage:
  ```shell
  Access principals lookup
  ((--issuer=<issuer> --subject=<subject>) | [--principal-tls-client-cert-file=<tlsCertFile>])
  ```

  Arguments:

  `--issuer`: OAuth issuer identifier

  `--subject`: OAuth subject identifier

  `--principal-tls-client-cert-file`: Path to TLS client certificate belonging to Principal

#### `roles`

* `get`

  Get a Role using a Role resource name.

  Pre-conditions: Role exists.

  Usage:
  ```shell
  Access principals get <principalName>
  ```

  Arguments:

  `<roleName>`: API resource name of the Role

* `list`

  List Roles. To navigate to next page, use `--page-token`.

  Usage:
  ```shell
  Access roles list [--page-size=<listPageSize>] [--page-token=<listPageToken>]
  ```

  Arguments:

  `--page-size`: The maximum number of Roles to return

  `--page-token`: A page token, received from a previous `ListRoles` call. Provide this
  to retrieve the subsequent page.


* `create`

  Create a Role.

  Pre-conditions: Role does not exist, Permissions exist, and Resource Type exists in Permission.

  Usage:
  ```shell
  Access roles create --role-id=<id> --permission=<permissionList> [--permission=<permissionList>]...
  --resource-type=<resourceTypeList> [--resource-type=<resourceTypeList>]
  ```

  Arguments:

  `-permission`: Resource name of permission granted by this Role. Can be
  specified multiple times.

  `--resource-type`: Resource type that this Role can be granted on. Can be
  specified multiple times.

  `--role-id`: Resource ID of the Role

* `update`

  Update a Role.

  Pre-conditions: Role exists, Permissions exist, Permissions found in Role, Etag matches,
  and Resource Type exists in Permission.

  Usage:
  ```shell
  Access roles update --etag=<roleEtag> --name=<roleName>
  --permission=<permissionList> [--permission=<permissionList>]...
  --resource-type=<resourceTypeList> [--resource-type=<resourceTypeList>]...
  ```

  Arguments:

  `--etag`: Entity tag of the Role

  `--name`: API resource name of the Role

  `--permission`: Resource name of permission granted by this Role. Can
  be specified multiple times.

  `--resource-type`: Resource type that this Role can be granted on.
  Can be specified multiple times.

* `delete`

  Delete a Role using Role resource name (This will also remove all Policy bindings which reference the Role).

  Pre-conditions: Role exists

  Usage:
  ```shell
  Access roles delete <roleName>
  ```

  Arguments:

  `<roleName>`: API resource name of the Role

#### `permissions`

* `get`

  Get a Permission using a Permission resource name.

  Pre-conditions: Permission does not exist.

  Usage:
  ```shell
  Access permissions get <permissionName>
  ```

  Arguments:

  `<permissionName>`: API resource name of the Permission

* `list`

  List Permissions. To navigate to next page, use `--page-token`.

  Usage:
  ```shell
  Access permissions list [--page-size=<listPageSize>] [--page-token=<listPageToken>]
  ```

  Arguments:

  `--page-size`: The maximum number of Permissions to return

  `--page-token`: A page token, received from a previous `ListPermissions` call. Provide
  this to retrieve the subsequent page.

* `check`

  Check what Permissions a Principal has on a given resource.

  Pre-conditions: Principal and Permissions exist.

  Usage:
  ```shell
  Access permissions check --principal=<principalName> [--protected-resource=<protectedResourceName>]
  --permission=<permissionList> [--permission=<permissionList>]...
  ```

  Arguments:

  `--permission`: Resource name of permission to check. Can be specified multiple times.

  `--principal`: Resource name of the Principal

  `--protected-resource`: Name of resource on which to check permissions. If not specified, this
  means the root of the protected API.

#### `policies`

* `get`

  Get a Policy using a Policy resource name

  Pre-conditions: Policy exists.

  Usage:
  ```shell
  Access policies get <policyName>
  ```

  Arguments:

  `<policyName>`: Resource name of the Policy

* `create`

  Create a Policy

  Pre-conditions: Principal exists, Principal type is oAuth user, Role exists, and Policy does not exist.

  Usage:
  ```shell
  Access policies create [--policy-id=<id>] [--protected-resource=<resource>]
  [--binding-role=<role> --binding-member=<members> [--binding-member=<members>]...]...
  ```

  Arguments:

  `--policy-id`: Resource ID of the Policy

  `--protected-resource`: Name of the resource protected by this Policy. If not
  specified, this means the root of the protected API.

  `--binding-member`: Resource name of the Principal which is a member of
  the Role on `resource`

  `--binding-role`: Resource name of the Role

* `lookup`

  Lookup a Policy

  Pre-conditions: Policy exists for Protected Resource.

  Usage:
  ```shell
  Access policies lookup --protected-resource=<resource>
  ```

  Arguments:

  `-protected-resource`: Name of the resource to which the policy applies

* `add-members`

  Add members to a Policy Binding

  Pre-conditions: Policy exists, etag matches, Role exists, Principal exists, Principal type is OAuth user,
  and Policy Binding does not exist

  Usage:
  ```shell
  Access policies add-members (--name=<policyName> [--etag=<currentEtag>]
  (--binding-role=<role> --binding-member=<members> [--binding-member=<members>]...))
  ```

  Arguments:

  `--binding-member`: Resource name of the principal, which is a member
  of the Role on resource. Can be specified multiple times.

  `--binding-role`: Resource name of the Role

  `--etag`: Current etag of the resource

  `--name`: Resource name of the Policy

* `remove-members`

  Remove members from a Policy Binding

  Pre-conditions: Policy exists, etag matches, Role exists, Principal exists, Principal type is OAuth user,
  and Policy Binding exists

  Usage:
  ```shell
  Access policies remove-members (--name=<policyName> [--etag=<currentEtag>]
  (--binding-role=<role> --binding-member=<members> [--binding-member=<members>]...))
  ```

  Arguments:

  `--binding-member`: Resource name of the principal, which is a member
  of the Role on resource. Can be specified multiple times.

  `--binding-role`: Resource name of the Role

  `--etag`: Current etag of the resource

  `--name`: Resource name of the Policy