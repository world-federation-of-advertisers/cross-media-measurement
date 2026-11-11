#!/usr/bin/env python3
# Copyright 2026 The Cross-Media Measurement Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied.

"""Render Terraform's machine authorization output for the system API deployment."""

import argparse
import json
import pathlib
import re
import urllib.parse


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-json", default="")
    parser.add_argument("--audience", required=True)
    parser.add_argument("--require-subjects", action="store_true")
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    role_fields = (
        "dispatcher_service_account_subjects",
        "healing_controller_service_account_subjects",
    )
    config = json.loads(args.config_json) if args.config_json else {
        "audience": args.audience,
        **{field: [] for field in role_fields},
    }
    if not isinstance(config, dict) or set(config) != {"audience", *role_fields}:
        raise ValueError("Machine API configuration has missing or unknown fields")
    if config["audience"] != args.audience:
        raise ValueError("Terraform and Kubernetes machine API audiences disagree")
    uri = urllib.parse.urlparse(config["audience"])
    if uri.scheme != "https" or not uri.hostname or uri.username or uri.fragment:
        raise ValueError("Machine API audience must be an absolute HTTPS URI")
    for field in role_fields:
        subjects = config[field]
        if not isinstance(subjects, list) or not all(
            isinstance(value, str) and re.fullmatch(r"[0-9]+", value)
            for value in subjects
        ):
            raise ValueError(f"{field} must contain numeric IAM service-account unique IDs")
        if args.require_subjects and not subjects:
            raise ValueError(f"{field} requires Terraform's applied service-account identities")
    if set(config[role_fields[0]]) & set(config[role_fields[1]]):
        raise ValueError("Dispatcher and controller identities must be distinct")
    lines = [f"audience: {json.dumps(config['audience'])}"]
    for field in role_fields:
        lines.extend(f"{field}: {json.dumps(value)}" for value in sorted(set(config[field])))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
