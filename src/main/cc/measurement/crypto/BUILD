cc_library(
    name = "protocol_encryption_utility",
    srcs = [
        "protocol_encryption_utility.cc",
    ],
    hdrs = [
        "protocol_encryption_utility.h",
    ],
    deps = [
        "//src/main/proto/wfa/measurement/internal/duchy:protocol_encryption_methods_cc_proto",
        "@com_google_private_join_and_compute//crypto:commutative_elgamal",
        "@com_google_private_join_and_compute//crypto:ec_commutative_cipher",
        "@com_google_private_join_and_compute//util:status",
    ],
)
