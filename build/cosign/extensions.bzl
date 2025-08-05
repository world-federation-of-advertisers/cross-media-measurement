"extensions for bzlmod"

load("@rules_oci//cosign:repositories.bzl", "cosign_register_toolchains")

def _cosign_extension(module_ctx):
    cosign_register_toolchains(name = "cosign", register = False)
    return module_ctx.extension_metadata()

cosign = module_extension(
    implementation = _cosign_extension,
    tag_classes = {
        "toolchains": tag_class(),
    },
)
