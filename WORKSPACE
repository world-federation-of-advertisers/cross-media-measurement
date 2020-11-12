workspace(name = "wfa_measurement_proto")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

http_archive(
    name = "rules_proto",
    sha256 = "602e7161d9195e50246177e7c55b2f39950a9cf7366f74ed5f22fd45750cd208",
    strip_prefix = "rules_proto-97d8af4dc474595af3900dd85cb3a29ad28cc313",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_proto/archive/97d8af4dc474595af3900dd85cb3a29ad28cc313.tar.gz",
        "https://github.com/bazelbuild/rules_proto/archive/97d8af4dc474595af3900dd85cb3a29ad28cc313.tar.gz",
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
