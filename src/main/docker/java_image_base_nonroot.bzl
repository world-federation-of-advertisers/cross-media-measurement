load("@io_bazel_rules_docker//container:container.bzl", "container_image")

container_image(
    name = "java_image_base_nonroot",
    base = "@java_image_base//image",
    user = "nonroot",
)