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

`--access-public-api-cert-host`: In the event that the host you specify to the
`--access-public-api-target` option doesn't match what is in the Subject
Alternative Name (SAN) extension of the server's certificate, this option
specifies a host that does match using the `--access-public-api-cert-host`
option.

`--tls-cert-file`: TLS client certificate. The issuer of this certificate must
be trusted by the Access server, i.e. the issuer certificate must be in that
server's trusted certificate collection file.

`--tls-key-file`: TLS client key.

### Commands

#### `principals`

*   Get a Principal

    ```shell
    Access \
      --tls-cert-file=secretfiles/reporting_tls.pem \
      --tls-key-file=secretfiles/reporting_tls.key \
      --cert-collection-file=secretfiles/reporting_root.pem \
      --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
      principals get principals/owner
    ```

*   Create a Principal

    ```shell
    Access \
      --tls-cert-file=secretfiles/reporting_tls.pem \
      --tls-key-file=secretfiles/reporting_tls.key \
      --cert-collection-file=secretfiles/reporting_root.pem \
      --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
      principals create --principal-id=user-1 --issuer=example.com --subject=user1@example.com
    ```

*   Delete a Principal

    ```shell
    Access \
      --tls-cert-file=secretfiles/reporting_tls.pem \
      --tls-key-file=secretfiles/reporting_tls.key \
      --cert-collection-file=secretfiles/reporting_root.pem \
      --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
      principals delete principals/owner
    ```

*   Lookup a Principal

    ```shell
    Access \
      --tls-cert-file=secretfiles/reporting_tls.pem \
      --tls-key-file=secretfiles/reporting_tls.key \
      --cert-collection-file=secretfiles/reporting_root.pem \
      --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
      principals lookup --principal-tls-client-cert-file=src/main/k8s/testing/secretfiles/mc_tls.pem
    ```

#### `roles`

*   Get a Role

    ```shell
    Access \
      --tls-cert-file=secretfiles/reporting_tls.pem \
      --tls-key-file=secretfiles/reporting_tls.key \
      --cert-collection-file=secretfiles/reporting_root.pem \
      --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
      roles get roles/bookReader
    ```

*   List Roles

    ```shell
    Access \
      --tls-cert-file=secretfiles/reporting_tls.pem \
      --tls-key-file=secretfiles/reporting_tls.key \
      --cert-collection-file=secretfiles/reporting_root.pem \
      --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
      roles list --page-size=50 --page-token=pageTokenFromPreviousResponse
    ```

*   Create a Role.

    ```shell
    Access \
      --tls-cert-file=secretfiles/reporting_tls.pem \
      --tls-key-file=secretfiles/reporting_tls.key \
      --cert-collection-file=secretfiles/reporting_root.pem \
      --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
      roles create --role-id=bookReader \
      --permission=permissions/books.get --permission=permissions/books.put \
      --resource-type=library.googleapis.com/Desk --resource-type=library.googleapis.com/Shelf
    ```

*   Update a Role.

    ```shell
    Access \
      --tls-cert-file=secretfiles/reporting_tls.pem \
      --tls-key-file=secretfiles/reporting_tls.key \
      --cert-collection-file=secretfiles/reporting_root.pem \
      --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
      roles update --etag=etag --name=roles/bookReader \
      --permission=permissions/books.get --permission=permissions/books.write \
      --resource-type=library.googleapis.com/Desk --resource-type=library.googleapis.com/Shelf
    ```

*   Delete a Role

    This will also remove all Policy bindings which reference the Role.

    ```shell
    Access \
      --tls-cert-file=secretfiles/reporting_tls.pem \
      --tls-key-file=secretfiles/reporting_tls.key \
      --cert-collection-file=secretfiles/reporting_root.pem \
      --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
      roles delete roles/bookReader
    ```

#### `permissions`

*   Get a Permission

    ```shell
    Access \
      --tls-cert-file=secretfiles/reporting_tls.pem \
      --tls-key-file=secretfiles/reporting_tls.key \
      --cert-collection-file=secretfiles/reporting_root.pem \
      --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
      permissions get permissions/books.get
    ```

*   List Permissions.

    ```shell
    Access \
      --tls-cert-file=secretfiles/reporting_tls.pem \
      --tls-key-file=secretfiles/reporting_tls.key \
      --cert-collection-file=secretfiles/reporting_root.pem \
      --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
      permissions list --page-size=50 --page-token=pageTokenFromPreviousResponse
    ```

*   Check what Permissions a Principal has on a given resource.

    ```shell
    Access \
      --tls-cert-file=secretfiles/reporting_tls.pem \
      --tls-key-file=secretfiles/reporting_tls.key \
      --cert-collection-file=secretfiles/reporting_root.pem \
      --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
      permissions check --principal=principals/owner
      --protected-resource=library.googleapis.com/Shelf
      --permission=permissions/books.get --permission=permissions/books.write
    ```

#### `policies`

*   Get a Policy

    ```shell
    Access \
      --tls-cert-file=secretfiles/reporting_tls.pem \
      --tls-key-file=secretfiles/reporting_tls.key \
      --cert-collection-file=secretfiles/reporting_root.pem \
      --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
      policies get policies/policy-1
    ```

*   Create a Policy

    ```shell
    Access \
      --tls-cert-file=secretfiles/reporting_tls.pem \
      --tls-key-file=secretfiles/reporting_tls.key \
      --cert-collection-file=secretfiles/reporting_root.pem \
      --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
      policies create --protected-resource=library.googleapis.com/Shelf \
      --binding-role=roles/bookReader --binding-member=principals/member --binding-member=principals/editor \
      --binding-role=roles/bookWriter --binding-member=principals/owner --binding-member=principals/owner \
      --policy-id=policy-1
    ```

*   Lookup a Policy

    ```shell
    Access \
      --tls-cert-file=secretfiles/reporting_tls.pem \
      --tls-key-file=secretfiles/reporting_tls.key \
      --cert-collection-file=secretfiles/reporting_root.pem \
      --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
      policies lookup --protected-resource=library.googleapis.com/Shelf
    ```

*   `add-members`

    ```shell
    Access \
      --tls-cert-file=secretfiles/reporting_tls.pem \
      --tls-key-file=secretfiles/reporting_tls.key \
      --cert-collection-file=secretfiles/reporting_root.pem \
      --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
      policies add-members --name=policies/policy-1 --etag=etag \
      --binding-role=roles/bookReader --binding-member=principals/member
    ```

*   `remove-members`

    ```shell
    Access \
      --tls-cert-file=secretfiles/reporting_tls.pem \
      --tls-key-file=secretfiles/reporting_tls.key \
      --cert-collection-file=secretfiles/reporting_root.pem \
      --access-public-api-target=public.access.dev.halo-cmm.org:8443 \
      policies remove-members --name=policies/policy-1 --etag=etag \
      --binding-role=roles/bookReader --binding-member=principals/member
    ```
