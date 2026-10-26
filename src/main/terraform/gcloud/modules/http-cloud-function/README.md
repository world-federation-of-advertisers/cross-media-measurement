# Google Cloud Function

Terraform script to provision the Service Account required by the HTTP Cloud Function.

## Deployment retries

The deploy provisioner retries a Cloud Build `EXPIRED` queue failure up to three
total attempts, waiting 30 and 60 seconds before retrying the same deployment
arguments. Compilation, configuration, and permission failures return immediately.
Retries occur inside the function provisioner rather than reapplying a saved
Terraform plan after other resources may have changed.

If retries are exhausted, Terraform still fails. During Update CMMS quiescence,
this can leave consumers or RequisitionFetcher paused. The workflow reports the
incomplete rollout in its job summary. After correcting the failure, re-run failed
jobs on the original Update CMMS run so all consumer-stop and API compatibility
barriers complete before activation. If its commit exceeds the deployment age
limit, start a fresh full rollout from main. A standalone apply that enables
consumers is not a recovery procedure for a partially upgraded control plane.
