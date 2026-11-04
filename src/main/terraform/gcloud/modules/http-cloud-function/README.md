# Google Cloud Function

Terraform script to provision the Service Account required by the HTTP Cloud Function.

## Deployment retries

The deploy provisioner retries a Cloud Build `EXPIRED` queue failure up to three
total attempts, waiting 30 and 60 seconds before retrying the same deployment
arguments. Deployment output streams throughout each attempt. The provisioner
uses the build link in that attempt's output to look up its API status and retries
only a confirmed `EXPIRED` build. Compilation, configuration, and permission
failures remain fatal.

The deployment identity needs `cloudbuild.builds.get` on the build's project
(included in `roles/cloudbuild.builds.viewer`). Missing or ambiguous build links,
unreadable status, and all other statuses stop retries and preserve the original
deployment exit code.

The limit counts attempts, not elapsed time. Two additional six-minute queue
expirations add about 13.5 minutes, including backoff, for an affected function.
Processing can remain paused during this interval. Repeated expiration still
requires investigating Cloud Build scheduling or quota. Retries occur inside the
function provisioner; recovery runs a fresh plan after a partially completed apply.

If retries are exhausted, Terraform still fails. During Update CMMS quiescence,
this can leave consumers or RequisitionFetcher paused. The workflow reports the
incomplete rollout in its job summary. After correcting the failure, re-run failed
jobs on the original Update CMMS run so all consumer-stop and API compatibility
barriers complete before activation. If its commit exceeds the deployment age
limit, start a fresh full rollout from main. A standalone apply that enables
consumers is not a recovery procedure for a partially upgraded control plane.
