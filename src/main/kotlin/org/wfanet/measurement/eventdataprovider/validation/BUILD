load("//tools/build_defs/kotlin:rules.bzl", "kt_jvm_library")

kt_jvm_library(
    name = "event_filter_validator",
    srcs = ["EventFilterValidator.kt"],
    deps = [
        "//java/com/google/protobuf",
        "//third_party/world_federation_of_advertisers/cross_media_measurement/src/main/kotlin/org/wfanet/measurement/eventdataprovider/eventfiltration:event_filters",
        "//third_party/world_federation_of_advertisers/cross_media_measurement/src/main/kotlin/org/wfanet/measurement/eventdataprovider/eventfiltration/validation",
        "//third_party/world_federation_of_advertisers/cross_media_measurement/src/main/proto/wfa/measurement/api/v2alpha:requisition_kt_jvm_proto",
        "//third_party/world_federation_of_advertisers/cross_media_measurement_api/src/main/proto/wfa/measurement/api/v2alpha:requisition_kt_proto",
    ],
)
