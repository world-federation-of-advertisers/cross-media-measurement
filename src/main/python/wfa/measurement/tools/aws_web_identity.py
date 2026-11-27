# Copyright 2026 The Cross-Media Measurement Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
from collections.abc import Sequence
from contextlib import ExitStack, suppress
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
from urllib.parse import unquote, urlparse


TOKEN_REFRESH_INTERVAL_SECONDS = 30 * 60


def _stop_command(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    with suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        with suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()


def _source_credentials(credentials_file: Path, directory: Path) -> tuple[Path, str]:
    config = json.loads(credentials_file.read_text())
    if config.get("type") != "external_account":
        raise ValueError("Google Workload Identity Federation credentials are required")
    target = urlparse(config.pop("service_account_impersonation_url", "")).path
    prefix = "/v1/projects/-/serviceAccounts/"
    suffix = ":generateAccessToken"
    if not target.startswith(prefix) or not target.endswith(suffix):
        raise ValueError("Google credentials must select a service account")
    service_account = unquote(target[len(prefix) : -len(suffix)])
    if not service_account:
        raise ValueError("Google credentials must select a service account")
    # Explicit gcloud impersonation must start with the WIF principal, rather than
    # impersonating an already impersonated service account a second time.
    source_file = directory / "source-credentials.json"
    source_file.write_text(json.dumps(config))
    source_file.chmod(0o600)
    return source_file, service_account


def _refresh_token(
    source_file: Path, service_account: str, audience: str, token_file: Path
) -> None:
    environment = os.environ.copy()
    environment["CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"] = str(source_file)
    result = subprocess.run(
        [
            "gcloud",
            "auth",
            "print-identity-token",
            f"--impersonate-service-account={service_account}",
            f"--audiences={audience}",
            "--format=value(id_token)",
        ],
        env=environment,
        stdout=subprocess.PIPE,
        text=True,
        check=True,
        timeout=120,
    )
    token = result.stdout.strip()
    if not token:
        raise ValueError("gcloud returned no identity token")
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print(f"::add-mask::{token}", flush=True)
    # Replace within the same filesystem, so the AWS SDK reads a complete token.
    with tempfile.TemporaryDirectory(
        dir=token_file.parent, prefix="aws-identity-"
    ) as directory:
        replacement = Path(directory) / "token"
        replacement.write_text(token)
        replacement.chmod(0o600)
        replacement.replace(token_file)
    print("Refreshed AWS web identity token", flush=True)


def run_command(
    command: Sequence[str],
    credentials_file: Path,
    audience: str,
    token_file: Path,
    refresh_interval_seconds: float = TOKEN_REFRESH_INTERVAL_SECONDS,
) -> int:
    """Runs a command with a private token file renewed until the command finishes.

    Owns token_file for the command's lifetime. A renewal failure stops the command;
    its process group is also stopped on cancellation. Google authentication and
    request retries are provided by gcloud, and AWS session renewal by the SDK.
    """
    if refresh_interval_seconds <= 0:
        raise ValueError("The token refresh interval must be positive")
    with ExitStack() as resources:
        directory = resources.enter_context(
            tempfile.TemporaryDirectory(prefix="aws-source-")
        )
        source_file, service_account = _source_credentials(
            credentials_file, Path(directory)
        )
        resources.callback(token_file.unlink, missing_ok=True)
        _refresh_token(source_file, service_account, audience, token_file)
        process = subprocess.Popen(command, start_new_session=True)
        resources.callback(_stop_command, process)
        while True:
            try:
                return process.wait(timeout=refresh_interval_seconds)
            except subprocess.TimeoutExpired:
                if process.poll() is None:
                    _refresh_token(source_file, service_account, audience, token_file)


def _exit_on_signal(signum: int, frame: object) -> None:
    raise SystemExit(128 + signum)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a command while renewing AWS web identity"
    )
    parser.add_argument("--audience", required=True)
    parser.add_argument("--token-file", required=True, type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("A command is required")
    credentials_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_file:
        parser.error(
            "GOOGLE_APPLICATION_CREDENTIALS must select the workflow's Google credentials"
        )
    signal.signal(signal.SIGINT, _exit_on_signal)
    signal.signal(signal.SIGTERM, _exit_on_signal)
    try:
        result = run_command(
            command, Path(credentials_file), args.audience, args.token_file
        )
        return result if result >= 0 else 128 - result
    except (OSError, subprocess.SubprocessError, ValueError) as error:
        print(f"AWS web identity command failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
