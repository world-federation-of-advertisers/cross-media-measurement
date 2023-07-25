load("//tools/build_defs/kotlin:rules.bzl", "kt_jvm_library")

kt_jvm_library(
    name = "big_query_event_query",
    srcs = ["BigQueryEventQuery.kt"],
    deps = [
        "//third_party/java/google_cloud_bigquery",
        "//third_party/world_federation_of_advertisers/cross_media_measurement/src/main/kotlin/org/wfanet/measurement/eventdataprovider/eventfiltration:event_filters",
    ],
)
