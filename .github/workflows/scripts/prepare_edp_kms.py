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

"""Prepare shared KMS Terraform inputs and verify published workload signatures."""

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path


def command(*args):
    return subprocess.check_output(args, stderr=subprocess.PIPE)


def prepare(repository, output_directory):
    config = json.loads((repository / ".github/env-configs/edp-kms.json").read_text())
    environments = {
        env: json.loads((repository / f".github/env-configs/{env}/cmms.json").read_text())
        for env in ("dev", "head", "qa")
    }
    fingerprints = {
        key: hashlib.sha256(command("openssl", "pkey", "-pubin", "-in",
                                    str(repository / f"cosign-{key}-pub-key.pem"),
                                    "-outform", "DER")).hexdigest()
        for key in ("test", "release")
    }
    isolation = json.loads((repository / ".github/env-configs/tee-workloads.json").read_text())
    if isolation["retire_legacy_federation"] and not all(isolation["activate_environments"].values()):
        raise ValueError("Roll out every environment before retiring legacy federation")
    google = dict(config["google"])
    google["legacy_federation_enabled"] = not isolation["retire_legacy_federation"]
    google["input_kek_uri"] = environments["dev"]["EDP7_KEK_URI"]
    google["workloads"] = {
        env: {
            "project_id": environments[env]["GCLOUD_PROJECT"],
            "provider_id": workload["provider_id"],
            "image_signature_fingerprints": [
                f"ECDSA_P256_SHA256:{fingerprints[key]}" for key in workload["signing_keys"]
            ],
        }
        for env, workload in config["google"]["workloads"].items()
    }
    aws = {key: value for key, value in config["aws"].items()
           if key not in ("deployment_role_arn", "additional_export_google_subjects")}
    aws["legacy_federation_enabled"] = not isolation["retire_legacy_federation"]
    aws["input_kek_uri"] = environments["dev"]["EDPA_META_KEK_URI"]
    aws["workload_project_ids"] = [value["GCLOUD_PROJECT"] for value in environments.values()]
    aws["image_signature_key_ids"] = list(fingerprints.values())
    aws["export_google_subjects"] = config["aws"]["additional_export_google_subjects"] + [
        value["EDPA_META_AWS_AUDIENCE"] for value in environments.values()
    ]
    for env, values in environments.items():
        if values["EDP7_KEK_URI"] != google["input_kek_uri"] or values["EDPA_META_KEK_URI"] != aws["input_kek_uri"]:
            raise ValueError("The shared KMS state requires identical input keys across environments")
        proto = (repository / f"src/main/terraform/gcloud/cmms/envs/{env}/event-data-provider-configs.textproto").read_text()
        audiences = set(re.findall(r'kms_audience:\s*"([^"]+)"', proto))
        expected = f'//iam.googleapis.com/projects/{google["project_number"]}/locations/global/workloadIdentityPools/{google["workload_identity_pool_id"]}/providers/{google["workloads"][env]["provider_id"]}'
        project = google["input_kek_uri"].split("/")[3]
        accounts = set(re.findall(r'\bservice_account:\s*"([^\"]+)"', proto))
        if accounts != {f'{google["service_account_name"]}@{project}.iam.gserviceaccount.com'}:
            raise ValueError(f"Federated account differs from runtime configuration for {env}")
        if set(re.findall(r'\bkek_uri:\s*"([^\"]+)"', proto)) != {google["input_kek_uri"]}:
            raise ValueError(f"Input key differs from runtime configuration for {env}")
        if audiences != {expected}:
            raise ValueError(f"KMS provider configuration differs from the runtime configuration for {env}")
        if set(re.findall(r'aws_role_arn:\s*"([^"]+)"', proto)) != {f'arn:aws:iam::{aws["input_kek_uri"].split(":")[5]}:role/{aws["role_name"]}'}:
            raise ValueError(f"AWS KMS role differs from the runtime configuration for {env}")
    output_directory.mkdir(parents=True, exist_ok=True)
    for name, values in (("google", google), ("aws", aws)):
        (output_directory / f"{name}.tfvars.json").write_text(json.dumps(values, indent=2) + "\n")
    return config, environments


def verify_images(repository, config, environments, environment, candidate_tag, candidate_environment):
    values = environments[environment]
    project = values["GCLOUD_PROJECT"]
    for account in config["google"]["workload_service_account_names"]:
        mig = {"results-fulfiller-sa": "results-fulfiller-mig-v2", "trustee-mill-mig-sa": "trustee-mill-mig"}.get(account, account.removesuffix("-sa") + "-mig")
        group = json.loads(command("gcloud", "compute", "instance-groups", "managed", "describe",
                                   mig, "--region", values["GCLOUD_REGION"], "--project", project, "--format=json"))
        template = group["instanceTemplate"].rsplit("/", 1)[-1]
        instance = json.loads(command("gcloud", "compute", "instance-templates", "describe",
                                      template, "--project", project, "--format=json"))
        metadata = {item["key"]: item["value"] for item in instance["properties"]["metadata"]["items"]}
        image = metadata["tee-image-reference"]
        signature_repository = metadata.get("tee-signed-image-repos")
        image_repository = image.split("@", 1)[0] if "@" in image else image.rsplit(":", 1)[0]
        if signature_repository != image_repository:
            raise ValueError(f"{environment}/{mig} does not expose its signatures to Confidential Space")
        images = [image]
        if candidate_tag and environment == candidate_environment:
            images.append(f"{signature_repository}:{candidate_tag}")
        for image in images:
            verified = False
            for key in config["google"]["workloads"][environment]["signing_keys"]:
                result = subprocess.run(["cosign", "verify", "--insecure-ignore-tlog=true", "--key",
                                         str(repository / f"cosign-{key}-pub-key.pem"), image],
                                        capture_output=True, text=True)
                if result.returncode == 0:
                    verified = True
                    break
            if not verified:
                raise ValueError(f"{environment}/{mig}: no trusted signature for {image}")
        print(f"Verified trusted signatures and launcher configuration: {environment}/{mig}")


def check_plan(path, repository):
    plan = json.loads(command("terraform", "-chdir=" + str(repository / ("src/main/terraform/gcloud/edp-kms" if path.stem == "google" else "src/main/terraform/aws/edp-kms")), "show", "-json", str(path)))
    for resource in plan.get("resource_changes", []):
        change = resource["change"]
        if "delete" in change["actions"]:
            raise ValueError(f'Resource removal or replacement is prohibited: {resource["address"]}')
        if resource["type"] in ("google_kms_crypto_key", "aws_kms_key"):
            if "create" in change["actions"]:
                raise ValueError(f'Existing KMS keys must be imported: {resource["address"]}')
            for field in ("rotation_period", "enable_key_rotation", "version_template"):
                if (change.get("before") or {}).get(field) != (change.get("after") or {}).get(field):
                    raise ValueError(f'KMS adoption must preserve {field}: {resource["address"]}')
    print("Verified that the plan preserves existing resources and key rotation settings")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-directory", required=True, type=Path)
    parser.add_argument("--environment", choices=("dev", "head", "qa"))
    parser.add_argument("--candidate-image-tag")
    parser.add_argument("--candidate-environment", choices=("dev", "head", "qa"))
    parser.add_argument("--github-output", type=Path)
    parser.add_argument("--check-plan", type=Path)
    args = parser.parse_args()
    repository = Path(__file__).resolve().parents[3]
    config, environments = prepare(repository, args.output_directory)
    if args.environment:
        verify_images(repository, config, environments, args.environment,
                      args.candidate_image_tag, args.candidate_environment)
    if args.check_plan:
        check_plan(args.check_plan, repository)
    if args.github_output:
        project = environments["dev"]["EDP7_KEK_URI"].split("/")[3]
        with args.github_output.open("a") as output:
            output.write(f'backend_bucket={config["backend_bucket"]}\n')
            output.write(f'aws_deployment_role={config["aws"]["deployment_role_arn"]}\n')
            output.write(f'aws_region={config["aws"]["region"]}\n')
            output.write(f'google_service_account=terraform@{project}.iam.gserviceaccount.com\n')
            output.write(f'google_workload_identity_provider=projects/{config["google"]["project_number"]}/locations/global/workloadIdentityPools/edp-kms-deployment/providers/edp-kms\n')


if __name__ == "__main__":
    main()
