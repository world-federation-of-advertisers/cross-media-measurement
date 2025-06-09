load("//build:variables.bzl", "IMAGE_REPOSITORY_SETTINGS")
load(":images.bzl", "TEST_SIGNED_BUILD_IMAGES")

def _dump_images_impl(ctx):
    registry = ctx.expand_make_variables("registry", IMAGE_REPOSITORY_SETTINGS.container_registry, {})
    tag = ctx.expand_make_variables("tag", IMAGE_REPOSITORY_SETTINGS.image_tag, {})
    lines = []
    lines.append(ctx.var)
    lines.append(IMAGE_REPOSITORY_SETTINGS.container_registry)
    lines.append(IMAGE_REPOSITORY_SETTINGS.image_tag)
    for image_spec in TEST_SIGNED_BUILD_IMAGES:
        lines.append(image_spec.repository)
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