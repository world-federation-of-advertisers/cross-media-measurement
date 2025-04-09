# Common Tools

## OpenIdProvider

CLI tool to act as a fake OpenID provider for testing. Specify the path to the
private key in Tink binary keyset format using the `--keyset` option, and the
issuer using the `--issuer` option.

### Commands

*   `get-jwks`

    Print the JSON Web Keyset (JWKS) of the provider. This can be used in the
    [`OpenIdProvidersConfig`](../../../../../../proto/wfa/measurement/config/access/open_id_providers_config.proto)
    for an API protected by the Access system.

*   `generate-access-token`

    Generates an OAuth 2.0 access token as a signed JWT.

    Example:

    ```shell
    OpenIdProvider \
    --keyset src/main/k8s/testing/secretfiles/open_id_provider.tink \
    --issuer https://auth.halo-cmm.local \
    --audience reporting.local \
    --scope 'reporting.reportingSets.*' \
    --scope 'reporting.basicReports.get' \
    --subject alice123 \
    --ttl 10m
    ```
