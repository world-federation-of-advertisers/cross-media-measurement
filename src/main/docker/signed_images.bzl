load("//build:variables.bzl", "IMAGE_REPOSITORY_SETTINGS")
load(":images.bzl", "TEST_SIGNED_BUILD_IMAGES")

def _dump_images_impl(ctx):
    registry = ctx.expand_make_variables("registry", IMAGE_REPOSITORY_SETTINGS.container_registry, {})
    tag = ctx.expand_make_variables("tag", IMAGE_REPOSITORY_SETTINGS.image_tag, {})
    lines = []
    for image_spec in TEST_SIGNED_BUILD_IMAGES:
        repository = ctx.expand_make_variables("repository", image_spec.repository, {})
        lines.append("%s/%s:%s" % (registry, repository, tag))
    ctx.actions.write(
        output = ctx.outputs.out,
        content = "\n".join(lines),
    )

dump_images_to_sign = rule(
    implementation = _dump_images_impl,
    outputs = {"out": "%{name}.txt"},
)


script_template = """\
#!/bin/bash
set -euo pipefail
if [ "$#" -ne 1 ]; then
  echo "Error: missing argument" >&2
  echo "Usage: $0 <GCP_KMS_KEY_FOR_SIGNING> (e.g. projects/MYPROJECT/locations/global/keyRings/MYKEYRING/cryptoKeys/MYKEY/cryptoKeyVersions/MYVERSION" >&2
  exit 1
fi

kms_key=$1

# install cosign
COSIGN_VERSION="2.5.0"
wget -nv -O cosign "https://github.com/sigstore/cosign/releases/download/v${COSIGN_VERSION}/cosign-linux-amd64"
chmod +x cosign  

# sign images
for image_ref in $(cat {images_file}); do
  echo "Signing image: $image_ref"
  cosign sign --tlog-upload=false --key gcpkms://$kms_key $image_ref
done
"""

def _compute_image_list(ctx):
    registry = ctx.expand_make_variables("registry", IMAGE_REPOSITORY_SETTINGS.container_registry, {})
    tag = ctx.expand_make_variables("tag", IMAGE_REPOSITORY_SETTINGS.image_tag, {})
    lines = []
    for image_spec in TEST_SIGNED_BUILD_IMAGES:
        repository = ctx.expand_make_variables("repository", image_spec.repository, {})
        lines.append("%s/%s:%s" % (registry, repository, tag))
    return lines

def _sign_images_impl(ctx):
    images = _compute_image_list(ctx)
    images_file = ctx.actions.declare_file(ctx.label.name + "-images.txt")
    ctx.actions.write(
        output = images_file,
        content = "\n".join(images),
    )

    script = ctx.actions.declare_file("%s-sign-script" % ctx.label.name)
    ctx.actions.expand_template(
        template = ctx.file._template,
        output = script,
        substitutions = {
            "{{images_file}}": images_file.short_path
        },
        is_executable = True,
    )
    #script_content = script_template.format(
    #    images_file = images_file.short_path,
    #)
    #ctx.actions.write(script, script_content, is_executable = True)

    runfiles = ctx.runfiles(files = [images_file])
    return [DefaultInfo(executable = script, runfiles = runfiles)]    

sign_images = rule(
    implementation = _sign_images_impl,
    attrs = {
        "_template": attr.label(
            default = ":sign_images.sh.template",
            allow_single_file = True,
        ),
    },
    executable = True,
)