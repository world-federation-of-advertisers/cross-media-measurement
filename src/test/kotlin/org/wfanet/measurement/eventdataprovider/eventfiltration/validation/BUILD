load("//tools/build_defs/kotlin:rules.bzl", "kt_jvm_library")

kt_jvm_library(
    name = "event_filter_validator_test",
    srcs = ["EventFilterValidatorTest.kt"],
    deps = [
        "//google/api/expr/v1alpha1:expr_java_proto",
        "//third_party/java/rdf4j:v4_0_0",
        "//third_party/java/truth/extensions:proto_not-testonly",
        "//third_party/kotlin/kotlin:kotlin_test",
        "//third_party/world_federation_of_advertisers/cross_media_measurement/src/main/kotlin/org/wfanet/measurement/eventdataprovider/eventfiltration/validation",
    ],
)

java_test(
    name = "EventFilterValidatorTest",
    runtime_deps = [":event_filter_validator_test"],
)
