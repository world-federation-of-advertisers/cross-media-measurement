load("//tools/build_defs/kotlin:rules.bzl", "kt_jvm_library")

kt_jvm_library(
    name = "event_filter_validator",
    srcs = ["EventFilterValidator.kt"],
    deps = [
        "//google/api/expr/v1alpha1:expr_java_proto",
        "//java/com/google/protobuf",
        "//third_party/world_federation_of_advertisers/cross_media_measurement/src/main/kotlin/org/wfanet/measurement/eventdataprovider/eventfiltration/validation",
    ],
)
