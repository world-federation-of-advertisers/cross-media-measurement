# Public API Tools

Command-line interface (CLI) tools for Cross-Media Access public
API.

The examples assume that you have built the relevant target, which outputs to
`bazel-bin` by default. For brevity, the examples to not include the full path
to the executable.

Run the `help` subcommand for
usage information.

### TLS options

To specify the public API target, use the `--access-public-api-target` option.

In the event that the host you specify to the `--access-public-api-target`
option doesn't match what is in the Subject Alternative Name (SAN) extension of
the server's certificate, you'll need to specify a host that does match using
the `--access-public-api-cert-host` option.

To specify a TLS client certificate and key, use the `--tls-cert-file` and
`--tls-key-file` options, respectively. The issuer of this certificate must be
trusted by the Access server, i.e. the issuer certificate must be in that
server's trusted certificate collection file.

### Commands

#### `principals`

* `get`

  Get a Principal using a Principal resource name.

  Pre-conditions: Principal exists.

* `create`

  Create a Principal that is either an OAuth user or Tls client.

  Pre-conditions: Principal does not exist.

  ```shell
  Access \
  --tls-cert-file=secretfiles/reporting_tls.pem \
  --tls-key-file=secretfiles/reporting_tls.key \
  --cert-collection-file=secretfiles/reporting_root.pem \
  --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
  principals \
  create \
  --issuer=example.com \
  --subject=user1@example.com ``
  --principal-id=user-1
  ```

* `delete`

  Delete a Principal using a Principal resource name(This also remove the Principal from all Policy bindings).

  Pre-conditions: Principal exists, and Principal type is OAuth user.

* `lookup`

  Lookup a Principal using an OAuth user or Tls client.

  Pre-conditions: Principal exists.

#### `roles`

* `get`

  Get a Role using a Role resource name.

  Pre-conditions: Role exists.

* `list`

  List Roles. To navigate to next page, use `--page-token`.

* `create`

  Create a Role.

  Pre-conditions: Role does not exist, Permissions exist, and Resource Type exists in Permission.

* `update`

  Update a Role.

  Pre-conditions: Role exists, Permissions exist, Permissions found in Role, Etag matches,
  and Resource Type exists in Permission.

  ```shell
  Access \
  --tls-cert-file=secretfiles/reporting_tls.pem \
  --tls-key-file=secretfiles/reporting_tls.key \
  --cert-collection-file=secretfiles/reporting_root.pem \
  --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
  roles \
  update \
  --name=roles/bookReader \
  --resource-type=library.googleapis.com/Shelf \
  --resource-type=library.googleapis.com/Desk \
  --permission=permissions/books.get\
  --permission=permissions/books.read\
  --etag=request-etag
  ```

* `delete`

  Delete a Role using Role resource name (This will also remove all Policy bindings which reference the Role).

  Pre-conditions: Role exists

#### `permissions`

* `get`

  Get a Permission using a Permission resource name.

  Pre-conditions: Permission does not exist.

* `list`

  List Permissions. To navigate to next page, use `--page-token`.

* `check`

  Check what Permissions a Principal has on a given resource.

  Pre-conditions: Principal and Permissions exist.

  ```shell
  Access \
  --tls-cert-file=secretfiles/reporting_tls.pem \
  --tls-key-file=secretfiles/reporting_tls.key \
  --cert-collection-file=secretfiles/reporting_root.pem \
  --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
  permissions \
  check \
  --principal=principals/owner \
  --protected-resource=library.googleapis.com/Shelf \
  --permission=permissions/books.get\
  --permission=permissions/books.read\
  ```

#### `policies`

* `get`

  Get a Policy using a Policy resource name

  Pre-conditions: Policy exists.

* `create`

  Create a Policy

  Pre-conditions: Principal exists, Principal type is oAuth user, Role exists, and Policy does not exist.

* `lookup`

  Lookup a Policy

  Pre-conditions: Policy exists for Protected Resource.

* `add-members`

  Add members to a Policy Binding

  Pre-conditions: Policy exists, etag matches, Role exists, Principal exists, Principal type is OAuth user,
  and Policy Binding does not exist

* `remove-members`

  Remove members from a Policy Binding

  Pre-conditions: Policy exists, etag matches, Role exists, Principal exists, Principal type is OAuth user,
  and Policy Binding exists

  ```shell
  Access \
  --tls-cert-file=secretfiles/reporting_tls.pem \
  --tls-key-file=secretfiles/reporting_tls.key \
  --cert-collection-file=secretfiles/reporting_root.pem \
  --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
  policies \
  remove-members \
  --name=policies/policy-1 \
  --binding-role=roles/bookReader \
  --binding-member=principals/member \
  --etag=request-etag
  ```
