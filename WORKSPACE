workspace(name = "wfa_measurement_system")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_file")

http_file(
    name = "cosign_bin",
    url = "https://github.com/sigstore/cosign/releases/download/v2.5.0/cosign-linux-amd64",
    sha256 = "1f6c194dd0891eb345b436bb71ff9f996768355f5e0ce02dde88567029ac2188",
    executable = True,
)