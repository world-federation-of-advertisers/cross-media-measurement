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

"""Prepare per-job KMS policies, sign approved images, and render runtime configs."""

import argparse
import base64
import hashlib
import json
import os
import re
import subprocess
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[3]
ENVIRONMENTS = ("dev", "head", "qa")


def command(*args):
    return subprocess.check_output(args, stderr=subprocess.PIPE)


def configuration():
    config = json.loads((REPOSITORY / ".github/env-configs/tee-workloads.json").read_text())
    envs = {env: json.loads((REPOSITORY / f".github/env-configs/{env}/cmms.json").read_text()) for env in ENVIRONMENTS}
    if config["retire_legacy_federation"] and not all(config["activate_environments"].values()):
        raise ValueError("Roll out every environment before retiring legacy federation")
    return config, envs


def signing_keys(directory, environment):
    values = json.loads((directory / f"{environment}.json").read_text())
    return values["signing_keys"]["value"] if "signing_keys" in values else values


def prepare(directory, signing_directory):
    config, envs = configuration()
    shared = json.loads((REPOSITORY / ".github/env-configs/edp-kms.json").read_text())
    gcp_key = envs["dev"]["EDP7_KEK_URI"].removeprefix("gcp-kms://")
    aws_key = envs["dev"]["EDPA_META_KEK_URI"].removeprefix("aws-kms://")
    values = {
        "google_project_id": gcp_key.split("/")[1],
        "google_key_ring_id": gcp_key.rsplit("/cryptoKeys/", 1)[0],
        "google_input_key_id": gcp_key,
        "aws_account_id": aws_key.split(":")[4],
        "aws_region": shared["aws"]["region"],
        "aws_input_key_arn": aws_key,
        "environments": {},
    }
    for env in ENVIRONMENTS:
        keys = signing_keys(signing_directory, config["signing"][env]["environment"])
        jobs = {}
        for job, workload in config["jobs"].items():
            pem = keys[job]["public_key"].encode()
            der = subprocess.check_output(["openssl", "pkey", "-pubin", "-outform", "DER"], input=pem)
            jobs[job] = {
                name: workload[name] for name in ("launch_service_account_name", "image_repositories", "key_permissions")
            }
            jobs[job]["image_signature_fingerprints"] = ["ECDSA_P256_SHA256:" + hashlib.sha256(der).hexdigest()]
        values["environments"][env] = {"project_id": envs[env]["GCLOUD_PROJECT"], "jobs": jobs}
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "workloads.tfvars.json").write_text(json.dumps(values, indent=2) + "\n")


def deployed_images(env, values, workload):
    def compute(*args):
        credentials = os.environ.get("TEE_COMPUTE_CREDENTIALS")
        env = dict(os.environ)
        if credentials:
            env["GOOGLE_APPLICATION_CREDENTIALS"] = credentials
            env["CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"] = credentials
        return subprocess.check_output(args, stderr=subprocess.PIPE, env=env)
    name = workload["launch_service_account_name"]
    mig = {"results-fulfiller-sa": "results-fulfiller-mig-v2", "trustee-mill-mig-sa": "trustee-mill-mig"}.get(name, name.removesuffix("-sa") + "-mig")
    group = json.loads(compute("gcloud", "compute", "instance-groups", "managed", "describe", mig, "--region", values["GCLOUD_REGION"], "--project", values["GCLOUD_PROJECT"], "--format=json"))
    template = group["instanceTemplate"].rsplit("/", 1)[-1]
    instance = json.loads(compute("gcloud", "compute", "instance-templates", "describe", template, "--project", values["GCLOUD_PROJECT"], "--format=json"))
    metadata = {item["key"]: item["value"] for item in instance["properties"]["metadata"]["items"]}
    image = metadata["tee-image-reference"]
    repo = image.split("@", 1)[0] if "@" in image else image.rsplit(":", 1)[0]
    if repo not in workload["image_repositories"] or metadata.get("tee-signed-image-repos") != repo:
        raise ValueError(f"{env}/{mig} must use an approved repository and expose its signatures")
    return image, repo


def sign(environment, signing_directory, candidate_tag, candidate_environment):
    config, envs = configuration()
    keys = signing_keys(signing_directory, config["signing"][environment]["environment"])
    public_key = REPOSITORY / ("cosign-release-pub-key.pem" if environment == "qa" else "cosign-test-pub-key.pem")
    for job, workload in config["jobs"].items():
        image, repo = deployed_images(environment, envs[environment], workload)
        images = {image}
        if candidate_tag and candidate_environment == environment:
            images.add(repo + ":" + candidate_tag)
        key = keys[job]
        encoded = base64.b64encode(key["public_key"].encode()).decode().rstrip("=")
        for image in sorted(images):
            signatures = json.loads(command("cosign", "verify", "--insecure-ignore-tlog=true", "--key", str(public_key), image))
            # A global signature must have been produced for this job's repository.
            # Copying another globally signed job into this repository is insufficient.
            matching = [s for s in signatures if s["critical"]["identity"]["docker-reference"] in workload["image_repositories"]]
            if not matching:
                raise ValueError(f"No Halo signature identifying {job}'s approved repository")
            digests = {s["critical"]["image"]["docker-manifest-digest"] for s in matching}
            if len(digests) != 1:
                raise ValueError("Ambiguous image digest")
            # Sign the verified digest to avoid a tag change between verify and sign.
            pinned = repo + "@" + digests.pop()
            command("cosign", "sign", "--yes", "--tlog-upload=false", "--new-bundle-format=false", "--key", "gcpkms://" + key["name"], "-a", "dev.cosignproject.cosign/sigalg=ECDSA_P256_SHA256", "-a", "dev.cosignproject.cosign/pub=" + encoded, pinned)
            print(f"Signed approved job image: {environment}/{job} {pinned}")


def validate_version(environment, image_tag):
    config, _ = configuration()
    if not config["activate_environments"][environment]:
        return
    if not image_tag or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", image_tag):
        raise ValueError("Activation requires a published image tag or commit")
    for ref in (image_tag, "v" + image_tag):
        try:
            resource = "repos/world-federation-of-advertisers/cross-media-measurement/contents/src/main/proto/wfa/measurement/config/edpaggregator/event_data_provider_configs.proto?ref=" + ref
            result = json.loads(command("gh", "api", resource))
        except subprocess.CalledProcessError:
            continue
        proto = base64.b64decode(result["content"]).decode()
        if "repeated WorkloadKmsConfig workload_kms_configs = 7;" in proto:
            return
    raise ValueError("The selected release does not support per-job KMS config")


def provider_blocks(text):
    """Return EDP block boundaries, ignoring braces inside strings and comments."""
    tokens = list(re.finditer(r'#[^\n]*|//[^\n]*|"(?:\\.|[^"\\])*"|[A-Za-z_][A-Za-z_0-9]*|[{}:]', text))
    stack = []
    start = None
    previous = None
    for token in tokens:
        value = token.group()
        if value.startswith("#") or value.startswith("//"):
            continue
        if value == "{":
            if not stack:
                if previous != "event_data_provider_config":
                    raise ValueError("Expected an event_data_provider_config block")
                start = token.start()
            stack.append(token.start())
        elif value == "}":
            if not stack:
                raise ValueError("Unbalanced configuration")
            stack.pop()
            if not stack:
                yield start, token.start()
        previous = value
    if stack:
        raise ValueError("Unbalanced configuration")


def render(directory, terraform_output):
    config, envs = configuration()
    outputs = json.loads(terraform_output.read_text())["environments"]["value"]
    directory.mkdir(parents=True, exist_ok=True)
    for env in ENVIRONMENTS:
        text = (REPOSITORY / f"src/main/terraform/gcloud/cmms/envs/{env}/event-data-provider-configs.textproto").read_text()
        additions = []
        for start, end in provider_blocks(text):
            block = text[start:end]
            if "workload_kms_configs" in block:
                raise ValueError("Source runtime config already contains job profiles")
            aws = re.search(r'kms_type:\s*(AWS|AWS_CONFIDENTIAL_SPACE)\b', block) is not None
            entries = []
            for job, workload in config["jobs"].items():
                identity = outputs[env]["workloads"][job]
                fields = {
                    "kms_audience": identity["kms_audience"],
                    "service_account": identity["service_account"],
                    "kek_uri": outputs[env]["aws_kek_uris" if aws else "google_kek_uris"][workload["output_purpose"]],
                }
                if aws:
                    fields.update(aws_role_arn=identity["aws_role_arn"], aws_audience=identity["aws_audience"], aws_role_session_name=job, aws_region=envs[env]["EDPA_META_AWS_REGION"])
                lines = ["  workload_kms_configs {", "    workload: " + workload["workload"], "    kms_config {", "      kms_type: " + ("AWS" if aws else "GCP")]
                lines += ["      " + name + ": " + json.dumps(value) for name, value in fields.items()]
                lines += ["    }", "  }"]
                entries.append("\n".join(lines))
            additions.append((end, "\n" + "\n".join(entries) + "\n"))
        for end, addition in reversed(additions):
            text = text[:end] + addition + text[end:]
        if not additions:
            raise ValueError("No EDP configuration blocks")
        (directory / f"{env}.textproto").write_text(text)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("prepare", "sign", "render"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--signing-directory", type=Path)
    parser.add_argument("--environment", choices=ENVIRONMENTS)
    parser.add_argument("--candidate-tag")
    parser.add_argument("--candidate-environment", choices=ENVIRONMENTS)
    parser.add_argument("--terraform-output", type=Path)
    args = parser.parse_args()
    if args.action == "prepare":
        prepare(args.directory, args.signing_directory)
    elif args.action == "sign":
        sign(args.environment, args.signing_directory, args.candidate_tag, args.candidate_environment)
    else:
        if args.candidate_environment:
            validate_version(args.candidate_environment, args.candidate_tag)
        render(args.directory, args.terraform_output)


if __name__ == "__main__":
    main()
