load("//build:variables.bzl", "IMAGE_REPOSITORY_SETTINGS")
load(":images.bzl", "IMAGES_TO_SIGN")

def _compute_image_list(ctx):
    registry = ctx.expand_make_variables("registry", IMAGE_REPOSITORY_SETTINGS.container_registry, {})
    tag = ctx.expand_make_variables("tag", IMAGE_REPOSITORY_SETTINGS.image_tag, {})
    image_refs = []
    for image_spec in IMAGES_TO_SIGN:
        repository = ctx.expand_make_variables("repository", image_spec.repository, {})
        image_refs.append("%s/%s:%s" % (registry, repository, tag))
    return image_refs

def _sign_images_impl(ctx):
    image_refs = _compute_image_list(ctx)
    images_file = ctx.actions.declare_file(ctx.label.name + "-images.txt")
    ctx.actions.write(
        output = images_file,
        content = "\n".join(image_refs),
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