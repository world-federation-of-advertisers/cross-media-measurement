# EDPA cloud test authentication

The EDPA cloud test uses Application Default Credentials to authenticate its
RequisitionFetcher requests. Credentials must support Google ID tokens:
service-account, Compute Engine, impersonated service-account, or identity-pool
Workload Identity Federation credentials with service-account impersonation.

GitHub Actions configures Workload Identity Federation automatically. For a
manual run using a user login, configure impersonated Application Default
Credentials for a service account allowed to invoke RequisitionFetcher:

```sh
gcloud auth application-default login --impersonate-service-account=SERVICE_ACCOUNT_EMAIL
```

The ID token audience is the configured RequisitionFetcher endpoint. Google's
credential library refreshes the token as needed throughout the test.
