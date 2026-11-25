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

"""Sign published image digests with bounded concurrency."""

import argparse
import concurrent.futures
import json
import re
import subprocess
import sys
import time

TRANSPORT_ERRORS = re.compile(
    r"invalid response when retrieving subject token:.*"
    r"((dial|read|write) (tcp|udp)|lookup [^:]+:|timeout|context deadline exceeded|EOF"
    r"|connection (reset by peer|refused|aborted)|network is unreachable|no route to host)",
    re.IGNORECASE,
)
REPOSITORY = re.compile(r"[a-z0-9.-]+(?::[0-9]+)?/[a-z0-9]+(?:[._-][a-z0-9]+)*(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


def read_images(path, commit, image_tag):
    with open(path) as manifest_file:
        manifest = json.load(manifest_file)
    if manifest["commit"] != commit or manifest["image_tag"] != image_tag:
        raise ValueError("Image manifest does not match this commit and image tag")
    images = manifest["images"]
    if not isinstance(images, list) or not images:
        raise ValueError("Image manifest is empty")
    repositories = set()
    for image in images:
        repository = image["repository"]
        if not REPOSITORY.fullmatch(repository) or not DIGEST.fullmatch(image["digest"]):
            raise ValueError("Invalid image repository or digest")
        if type(image["sign"]) is not bool:
            raise ValueError("Image signing selection must be boolean")
        if repository in repositories:
            raise ValueError("Duplicate image repository: " + repository)
        repositories.add(repository)
    selected = [image for image in images if image["sign"]]
    if not selected:
        raise ValueError("Image manifest has no images to sign")
    return selected


def sign_image(image, cosign, key, public_key):
    reference = image["repository"] + "@" + image["digest"]
    command = [
        cosign, "sign", reference, "--tlog-upload=false", "--key=" + key,
        "-a", "dev.cosignproject.cosign/sigalg=ECDSA_P256_SHA256",
        "-a", "dev.cosignproject.cosign/pub=" + public_key,
    ]
    for attempt in range(1, 4):
        result = subprocess.run(command, capture_output=True, text=True)
        output = result.stdout + result.stderr
        print(reference + "\n" + output, flush=True)
        if result.returncode == 0:
            return
        if attempt == 3 or not TRANSPORT_ERRORS.search(output):
            raise RuntimeError("Image signing failed: " + reference)
        delay = 15 * attempt
        print(f"::warning::Identity-token transport failure signing {reference}; retrying in {delay}s", flush=True)
        time.sleep(delay)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--image-tag", required=True)
    parser.add_argument("--cosign", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--public-key", required=True)
    args = parser.parse_args()
    try:
        images = read_images(args.manifest, args.commit, args.image_tag)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print("::error::" + str(error), file=sys.stderr)
        return 1
    success = True
    # Exiting the executor waits for every worker before the job can be retried.
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(sign_image, image, args.cosign, args.key, args.public_key) for image in images]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except (OSError, RuntimeError) as error:
                print("::error::" + str(error), file=sys.stderr)
                success = False
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
