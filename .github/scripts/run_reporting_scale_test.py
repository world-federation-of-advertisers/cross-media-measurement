# Copyright 2026 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run each scale case with fetching paused until creation has been verified."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import zipfile

TARGET = "//src/test/kotlin/org/wfanet/measurement/integration/k8s:ReportingScaleTest"
OUTPUT = Path("reporting-scale-results")
LOGS = Path("bazel-testlogs/src/test/kotlin/org/wfanet/measurement/integration/k8s/ReportingScaleTest")
TEST_ENVIRONMENT = (
    "MC_NAME", "MC_API_KEY", "PDP_NAME", "KINGDOM_PUBLIC_API_TARGET",
    "REPORTING_PUBLIC_API_TARGET", "ACCESS_PUBLIC_API_TARGET", "REPORTING_SERVICE_ENDPOINT",
    "REPORTING_TOKEN_AUDIENCE", "HIGH_OVERLAP_MODEL_LINE", "LD_LIBRARY_PATH",
    "REPORTING_SCALE_REPORT_DAYS", "REPORTING_SCALE_REPORT_START",
    "REPORTING_SCALE_EVENT_GROUP_REFERENCE_IDS", "REPORTING_SCALE_MODEL_LINE",
    "REPORTING_SCALE_DEMOGRAPHIC_FIELDS", "REPORTING_SCALE_STATE_COUNTS",
    "REPORTING_SCALE_FILTER_IDS", "REPORTING_SCALE_REPETITIONS",
    "REPORTING_SCALE_FULFILLMENT_TIMEOUT_SECONDS", "REPORTING_SCALE_DEADLINE_SECONDS",
    "REPORTING_SCALE_SEED", "REPORTING_SCALE_PHASE", "REPORTING_SCALE_CANCEL_FAILED_CREATION",
    "REPORTING_SCALE_EXISTING_BASIC_REPORT", "REPORTING_SCALE_EDP_TLS_NAMES",
)


def scheduler(action, job=None):
    command = ["gcloud", "scheduler", "jobs", action]
    if job:
        command.append(job)
    command += ["--project=" + os.environ["GCLOUD_PROJECT"],
                "--location=" + os.environ["GCLOUD_REGION"], "--format=json", "--quiet"]
    response = subprocess.run(command, check=True, text=True, capture_output=True)
    return json.loads(response.stdout) if response.stdout.strip() else None


def collect(destination):
    destination.mkdir(parents=True, exist_ok=True)
    undeclared = LOGS / "test.outputs"
    archive = undeclared / "outputs.zip"
    if archive.exists():
        with zipfile.ZipFile(archive) as source:
            source.extractall(destination)
    elif undeclared.exists():
        shutil.copytree(undeclared, destination, dirs_exist_ok=True)
    if (LOGS / "test.log").exists():
        shutil.copyfile(LOGS / "test.log", destination / "test.log")
    summary = destination / "summary.json"
    return json.loads(summary.read_text()) if summary.exists() else []


def run_phase(environment, destination):
    result = subprocess.run(
        ["bazel", "test", "--config=ci", "--test_output=streamed",
         "--test_timeout=21000", "--cache_test_results=no"]
        + ["--test_env=" + key for key in TEST_ENVIRONMENT] + [TARGET],
        env=environment,
    )
    return result.returncode, collect(destination)


def main():
    OUTPUT.mkdir(exist_ok=True)
    # Complete compilation before pausing a shared scheduler.
    subprocess.run(["bazel", "build", "--config=ci", TARGET], check=True)
    output_base = subprocess.check_output(["bazel", "info", "output_base"], text=True).strip()
    environment = os.environ.copy()
    environment["LD_LIBRARY_PATH"] = str(Path(output_base) / "external/cloud-spanner-emulator-bin+")
    environment.setdefault("REPORTING_SCALE_EDP_TLS_NAMES", "edp7,edpa_meta,edpa_video_pub")
    environment["REPORTING_SCALE_REPETITIONS"] = "1"
    jobs = scheduler("list")
    matching = [job for job in jobs if job.get("httpTarget", {}).get("uri") == os.environ["REQUISITION_FETCHER_ENDPOINT"]]
    if len(matching) != 1:
        raise RuntimeError("Expected one scheduler targeting REQUISITION_FETCHER_ENDPOINT")
    job = matching[0]["name"].rsplit("/", 1)[-1]
    reuse_pause = os.environ.get("REPORTING_SCALE_REUSE_PAUSED_FETCHER", "false").lower() == "true"
    if matching[0]["state"] != "ENABLED" and not (matching[0]["state"] == "PAUSED" and reuse_pause):
        raise RuntimeError("Fetcher scheduler is already paused; explicitly reuse your previous scale-test pause to retry")
    records = []
    state_counts = os.environ.get("REPORTING_SCALE_STATE_COUNTS", "50").split(",")
    repetitions = int(os.environ.get("REPORTING_SCALE_REPETITIONS", "1"))
    try:
        for state_count in state_counts:
            for repetition in range(repetitions):
                case = OUTPUT / f"states-{state_count.strip()}-repeat-{repetition + 1}"
                environment["REPORTING_SCALE_STATE_COUNTS"] = state_count.strip()
                environment["REPORTING_SCALE_PHASE"] = "creation"
                environment["REPORTING_SCALE_CANCEL_FAILED_CREATION"] = "true"
                environment["REPORTING_SCALE_EXISTING_BASIC_REPORT"] = ""
                scheduler("pause", job)
                if scheduler("describe", job)["state"] != "PAUSED":
                    raise RuntimeError("Fetcher scheduler did not pause")
                creation_complete = False
                cleanup_complete = False
                try:
                    code, creation = run_phase(environment, case / "creation")
                    records.extend(creation)
                    creation_complete = (
                        code == 0 and len(creation) == 1
                        and creation[0].get("creation_verified") is True
                        and creation[0].get("outcome") == "creation_succeeded"
                    )
                    cleanup_complete = bool(creation) and all(row.get("cleanup_complete") is True for row in creation)
                    if not creation_complete:
                        raise RuntimeError("Creation failed; see cancellation results in the case summary")
                finally:
                    if creation_complete:
                        scheduler("resume", job)
                    elif cleanup_complete:
                        print("Failed-report cleanup completed; fetcher remains PAUSED for the next creation attempt", flush=True)
                    else:
                        print("Fetcher remains PAUSED because failed-report cleanup was not verified", flush=True)
                # Submission and fulfillment are separate test invocations. This second invocation
                # reuses the exact report and cannot create or seed additional work.
                scheduler("run", job)
                environment["REPORTING_SCALE_PHASE"] = "full"
                environment["REPORTING_SCALE_CANCEL_FAILED_CREATION"] = "false"
                environment["REPORTING_SCALE_EXISTING_BASIC_REPORT"] = creation[0]["basic_report"]
                code, fulfillment = run_phase(environment, case / "fulfillment")
                records.extend(fulfillment)
                if code != 0:
                    raise RuntimeError("Fulfillment verification failed after successful creation")
    finally:
        (OUTPUT / "summary.json").write_text(json.dumps(records, indent=2))
        current = scheduler("describe", job)
        (OUTPUT / "fetcher-state.json").write_text(json.dumps({"job": current["name"], "state": current["state"]}, indent=2))


if __name__ == "__main__":
    main()
