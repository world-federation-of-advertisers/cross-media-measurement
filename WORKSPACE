workspace(name = "wfa_measurement_proto")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

http_archive(
    name = "rules_proto",
    sha256 = "d8992e6eeec276d49f1d4e63cfa05bbed6d4a26cfe6ca63c972827a0d141ea3b",
    strip_prefix = "rules_proto-cfdc2fa31879c0aebe31ce7702b1a9c8a4be02d2",
    urls = [
        "https://github.com/bazelbuild/rules_proto/archive/cfdc2fa31879c0aebe31ce7702b1a9c8a4be02d2.tar.gz",
    ],
)

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")

rules_proto_dependencies()

rules_proto_toolchains()

http_file(
    name = "plantuml",
    downloaded_file_path = "plantuml.jar",
    sha256 = "112b9c44ea069a9b24f237dfb6cb7a6cfb9cd918e507e9bee2ebb9c3797f6051",
    urls = ["https://downloads.sourceforge.net/project/plantuml/1.2020.19/plantuml.1.2020.19.jar"],
)
